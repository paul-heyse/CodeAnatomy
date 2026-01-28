"""Schema metadata helpers for ordering and provenance."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

import pyarrow as pa
import pyarrow.types as patypes

from arrow_utils.core.ordering import Ordering, OrderingKey, OrderingLevel
from arrow_utils.core.schema_constants import (
    KEY_FIELDS_META,
    OPTIONAL_FUNCTIONS_META,
    PROVENANCE_COLS,
    REQUIRED_FUNCTION_SIGNATURE_TYPES_META,
    REQUIRED_FUNCTION_SIGNATURES_META,
    REQUIRED_FUNCTIONS_META,
    REQUIRED_NON_NULL_META,
    SCHEMA_META_NAME,
    SCHEMA_META_VERSION,
)
from datafusion_engine.arrow_interop import (
    ArrayLike,
    DataTypeLike,
    FieldLike,
    SchemaLike,
    TableLike,
)
from datafusion_engine.arrow_schema.dictionary import normalize_dictionaries
from datafusion_engine.arrow_schema.encoding import EncodingPolicy, EncodingSpec
from datafusion_engine.arrow_schema.encoding_metadata import (
    DICT_INDEX_META,
    DICT_ORDERED_META,
    ENCODING_DICTIONARY,
    ENCODING_META,
    dict_field_metadata,
)
from datafusion_engine.arrow_schema.nested_builders import (
    dictionary_array_from_indices as _dictionary_from_indices,
)
from storage.ipc_utils import ipc_hash, ipc_table, payload_ipc_bytes

if TYPE_CHECKING:
    from arrow_utils.core.expr_types import ScalarValue
    from datafusion_engine.arrow_interop import ScalarLike
    from schema_spec.specs import TableSchemaSpec

_POSITION_COLS: tuple[str, ...] = (
    "ast_idx",
    "parent_ast_idx",
    "child_ast_idx",
    "bstart",
    "bend",
    "start_byte",
    "end_byte",
    "lineno",
    "col_offset",
    "offset",
    "instr_index",
    "start_offset",
    "end_offset",
    "raw_line",
    "raw_column",
    "table_id",
    "name",
    *PROVENANCE_COLS,
)

METADATA_PAYLOAD_VERSION: int = 1
ORDERING_KEYS_VERSION: int = 1
OPTIONS_HASH_VERSION: int = 1
EXTRACTOR_DEFAULTS_VERSION: int = 1

_ORDERED_TRUE = {"1", "true", "yes", "y", "t"}
_INDEX_TYPES: Mapping[str, pa.DataType] = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
}

EXTRACTOR_DEFAULTS_META = b"extractor_option_defaults"

_MAP_ENTRY_TYPE = pa.struct(
    [
        pa.field("key", pa.string()),
        pa.field("value", pa.string()),
    ]
)
_MAP_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("entries", pa.list_(_MAP_ENTRY_TYPE)),
    ]
)
_LIST_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("entries", pa.list_(pa.string())),
    ]
)
_SCALAR_ENTRY_TYPE = pa.struct(
    [
        pa.field("key", pa.string()),
        pa.field("value_kind", pa.string()),
        pa.field("value_bool", pa.bool_()),
        pa.field("value_int", pa.int64()),
        pa.field("value_float", pa.float64()),
        pa.field("value_string", pa.string()),
        pa.field("value_binary", pa.binary()),
    ]
)
_SCALAR_MAP_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("entries", pa.list_(_SCALAR_ENTRY_TYPE)),
    ]
)
_ORDERING_KEYS_ENTRY = pa.struct(
    [
        pa.field("column", pa.string()),
        pa.field("order", pa.string()),
    ]
)
_ORDERING_KEYS_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("entries", pa.list_(_ORDERING_KEYS_ENTRY)),
    ]
)
_EXTRACTOR_DEFAULTS_ENTRY = pa.struct(
    [
        pa.field("key", pa.string(), nullable=False),
        pa.field("value_kind", pa.string(), nullable=False),
        pa.field("value_bool", pa.bool_(), nullable=True),
        pa.field("value_int", pa.int64(), nullable=True),
        pa.field("value_float", pa.float64(), nullable=True),
        pa.field("value_string", pa.string(), nullable=True),
        pa.field("value_strings", pa.list_(pa.string()), nullable=True),
    ]
)
_EXTRACTOR_DEFAULTS_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32(), nullable=False),
        pa.field("entries", pa.list_(_EXTRACTOR_DEFAULTS_ENTRY), nullable=False),
    ]
)


class _ListType(Protocol):
    value_field: FieldLike


class _MapType(Protocol):
    key_field: FieldLike
    item_field: FieldLike
    keys_sorted: bool


@dataclass(frozen=True)
class SchemaMetadataSpec:
    """Schema metadata mutation policy."""

    schema_metadata: dict[bytes, bytes] = field(default_factory=dict)
    field_metadata: dict[str, dict[bytes, bytes]] = field(default_factory=dict)

    @staticmethod
    def _split_metadata(
        metadata: dict[str, dict[bytes, bytes]],
    ) -> tuple[dict[str, dict[bytes, bytes]], dict[str, dict[bytes, bytes]]]:
        top_level: dict[str, dict[bytes, bytes]] = {}
        nested: dict[str, dict[bytes, bytes]] = {}
        for name, meta in metadata.items():
            if "." in name:
                nested[name] = meta
            else:
                top_level[name] = meta
        return top_level, nested

    @staticmethod
    def _apply_struct_metadata(
        field: FieldLike,
        nested: dict[str, dict[bytes, bytes]],
        prefix: str,
    ) -> FieldLike:
        children: list[FieldLike] = []
        struct_type = cast("_StructType", field.type)
        for child in struct_type:
            child_prefix = f"{prefix}.{child.name}"
            updated_child = SchemaMetadataSpec._apply_nested_metadata(
                child,
                nested,
                child_prefix,
            )
            child_meta = nested.get(child_prefix)
            if child_meta:
                merged = dict(updated_child.metadata or {})
                merged.update(child_meta)
                updated_child = updated_child.with_metadata(merged)
            children.append(updated_child)
        return pa.field(
            field.name,
            pa.struct(children),
            nullable=field.nullable,
            metadata=field.metadata,
        )

    @staticmethod
    def _apply_list_metadata(
        field: FieldLike,
        nested: dict[str, dict[bytes, bytes]],
        prefix: str,
        list_factory: Callable[[FieldLike], DataTypeLike],
    ) -> FieldLike:
        list_type = cast("_ListType", field.type)
        value_field = list_type.value_field
        child_prefix = f"{prefix}.{value_field.name}"
        updated_child = SchemaMetadataSpec._apply_nested_metadata(
            value_field,
            nested,
            child_prefix,
        )
        child_meta = nested.get(child_prefix)
        if child_meta:
            merged = dict(updated_child.metadata or {})
            merged.update(child_meta)
            updated_child = updated_child.with_metadata(merged)
        new_type = list_factory(updated_child)
        return pa.field(field.name, new_type, nullable=field.nullable, metadata=field.metadata)

    @staticmethod
    def _apply_map_metadata(
        field: FieldLike,
        nested: dict[str, dict[bytes, bytes]],
        prefix: str,
    ) -> FieldLike:
        map_type = cast("_MapType", field.type)
        key_field = map_type.key_field
        item_field = map_type.item_field

        key_prefix = f"{prefix}.{key_field.name}"
        updated_key = SchemaMetadataSpec._apply_nested_metadata(
            key_field,
            nested,
            key_prefix,
        )
        key_meta = nested.get(key_prefix)
        if key_meta:
            merged = dict(updated_key.metadata or {})
            merged.update(key_meta)
            updated_key = updated_key.with_metadata(merged)
        if updated_key.nullable:
            updated_key = pa.field(
                updated_key.name,
                updated_key.type,
                nullable=False,
                metadata=updated_key.metadata,
            )

        item_prefix = f"{prefix}.{item_field.name}"
        updated_item = SchemaMetadataSpec._apply_nested_metadata(
            item_field,
            nested,
            item_prefix,
        )
        item_meta = nested.get(item_prefix)
        if item_meta:
            merged = dict(updated_item.metadata or {})
            merged.update(item_meta)
            updated_item = updated_item.with_metadata(merged)

        new_type = pa.map_(updated_key, updated_item, keys_sorted=map_type.keys_sorted)
        return pa.field(field.name, new_type, nullable=field.nullable, metadata=field.metadata)

    @staticmethod
    def _apply_nested_metadata(
        field: FieldLike,
        nested: dict[str, dict[bytes, bytes]],
        prefix: str,
    ) -> FieldLike:
        if not nested:
            return field

        if patypes.is_struct(field.type):
            return SchemaMetadataSpec._apply_struct_metadata(field, nested, prefix)

        list_factories: list[
            tuple[Callable[[DataTypeLike], bool], Callable[[FieldLike], DataTypeLike]]
        ] = [
            (patypes.is_list, pa.list_),
            (patypes.is_large_list, pa.large_list),
            (patypes.is_list_view, pa.list_view),
            (patypes.is_large_list_view, pa.large_list_view),
        ]
        for predicate, factory in list_factories:
            if predicate(field.type):
                return SchemaMetadataSpec._apply_list_metadata(field, nested, prefix, factory)

        if patypes.is_map(field.type):
            return SchemaMetadataSpec._apply_map_metadata(field, nested, prefix)

        return field

    def apply(self, schema: SchemaLike) -> SchemaLike:
        """Return a schema with metadata updates applied.

        Returns
        -------
        SchemaLike
            Updated schema with metadata applied.
        """
        top_level, nested = self._split_metadata(self.field_metadata)
        fields: list[FieldLike] = []
        for schema_field in schema:
            meta = top_level.get(schema_field.name)
            if meta is None:
                updated = schema_field
            else:
                merged = dict(schema_field.metadata or {})
                merged.update(meta)
                updated = schema_field.with_metadata(merged)
            updated = self._apply_nested_metadata(updated, nested, updated.name)
            fields.append(updated)

        updated = pa.schema(fields)
        if self.schema_metadata:
            meta = dict(schema.metadata or {})
            meta.update(self.schema_metadata)
            return updated.with_metadata(meta)
        if schema.metadata is None:
            return updated
        return updated.with_metadata(schema.metadata)


class _StructType(Protocol):
    def __iter__(self) -> Iterator[FieldLike]: ...


class _ArrowFieldSpec(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def dtype(self) -> DataTypeLike: ...

    @property
    def nullable(self) -> bool: ...

    @property
    def metadata(self) -> Mapping[str, str]: ...

    @property
    def encoding(self) -> str | None: ...


class _TableSchemaSpec(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def fields(self) -> Sequence[_ArrowFieldSpec]: ...

    @property
    def key_fields(self) -> Sequence[str]: ...

    @property
    def required_non_null(self) -> Sequence[str]: ...

    def to_arrow_schema(self) -> SchemaLike: ...


def metadata_map_bytes(entries: Mapping[str, str]) -> bytes:
    """Encode string mapping metadata as IPC bytes.

    Parameters
    ----------
    entries:
        Mapping of string keys to string values.

    Returns
    -------
    bytes
        IPC bytes for the mapping payload.
    """
    payload = {
        "version": METADATA_PAYLOAD_VERSION,
        "entries": [
            {"key": str(key), "value": str(value)}
            for key, value in sorted(entries.items(), key=lambda item: str(item[0]))
        ],
    }
    return payload_ipc_bytes(payload, _MAP_SCHEMA)


def metadata_list_bytes(entries: Sequence[str]) -> bytes:
    """Encode string list metadata as IPC bytes.

    Parameters
    ----------
    entries:
        Sequence of string values.

    Returns
    -------
    bytes
        IPC bytes for the list payload.
    """
    payload = {"version": METADATA_PAYLOAD_VERSION, "entries": [str(item) for item in entries]}
    return payload_ipc_bytes(payload, _LIST_SCHEMA)


def metadata_scalar_map_bytes(entries: Mapping[str, ScalarValue]) -> bytes:
    """Encode scalar mapping metadata as IPC bytes.

    Parameters
    ----------
    entries:
        Mapping of scalar values keyed by string.

    Returns
    -------
    bytes
        IPC bytes for the scalar mapping payload.
    """
    payload = {
        "version": METADATA_PAYLOAD_VERSION,
        "entries": [
            _scalar_entry(str(key), value)
            for key, value in sorted(entries.items(), key=lambda item: str(item[0]))
        ],
    }
    return payload_ipc_bytes(payload, _SCALAR_MAP_SCHEMA)


def decode_metadata_map(payload: bytes) -> dict[str, str]:
    """Decode IPC metadata bytes into a string mapping.

    Parameters
    ----------
    payload:
        IPC payload bytes.

    Returns
    -------
    dict[str, str]
        Decoded string mapping.

    Raises
    ------
    TypeError
        Raised when the payload shape is invalid.
    """
    table = ipc_table(payload)
    rows = table.to_pylist()
    if not rows:
        return {}
    entry = rows[0]
    if not isinstance(entry, Mapping):
        msg = "metadata_map_bytes payload must contain a mapping."
        raise TypeError(msg)
    entries = entry.get("entries")
    if entries is None:
        return {}
    if not isinstance(entries, list):
        msg = "metadata_map_bytes entries must be a list."
        raise TypeError(msg)
    results: dict[str, str] = {}
    for item in entries:
        if not isinstance(item, Mapping):
            msg = "metadata_map_bytes entries must be mappings."
            raise TypeError(msg)
        key = item.get("key")
        value = item.get("value")
        if key is None:
            continue
        results[str(key)] = str(value) if value is not None else ""
    return results


def decode_metadata_list(payload: bytes) -> list[str]:
    """Decode IPC metadata bytes into a list of strings.

    Parameters
    ----------
    payload:
        IPC payload bytes.

    Returns
    -------
    list[str]
        Decoded list of strings.

    Raises
    ------
    TypeError
        Raised when the payload shape is invalid.
    """
    table = ipc_table(payload)
    rows = table.to_pylist()
    if not rows:
        return []
    entry = rows[0]
    if not isinstance(entry, Mapping):
        msg = "metadata_list_bytes payload must contain a mapping."
        raise TypeError(msg)
    entries = entry.get("entries")
    if entries is None:
        return []
    if not isinstance(entries, list):
        msg = "metadata_list_bytes entries must be a list."
        raise TypeError(msg)
    return [str(item) for item in entries if item is not None]


def decode_metadata_scalar_map(payload: bytes) -> dict[str, object]:
    """Decode IPC metadata bytes into a scalar mapping.

    Parameters
    ----------
    payload:
        IPC payload bytes.

    Returns
    -------
    dict[str, object]
        Decoded scalar mapping.

    Raises
    ------
    TypeError
        Raised when the payload shape is invalid.
    """
    table = ipc_table(payload)
    rows = table.to_pylist()
    if not rows:
        return {}
    entry = rows[0]
    if not isinstance(entry, Mapping):
        msg = "metadata_scalar_map_bytes payload must contain a mapping."
        raise TypeError(msg)
    entries = entry.get("entries")
    if entries is None:
        return {}
    if not isinstance(entries, list):
        msg = "metadata_scalar_map_bytes entries must be a list."
        raise TypeError(msg)
    results: dict[str, object] = {}
    for item in entries:
        if not isinstance(item, Mapping):
            msg = "metadata_scalar_map_bytes entries must be mappings."
            raise TypeError(msg)
        key = item.get("key")
        if key is None:
            continue
        results[str(key)] = _decode_scalar_value(item)
    return results


def _scalar_entry(key: str, value: ScalarValue) -> dict[str, object]:
    entry: dict[str, object] = {
        "key": key,
        "value_kind": "null",
        "value_bool": None,
        "value_int": None,
        "value_float": None,
        "value_string": None,
        "value_binary": None,
    }
    resolved = value.as_py() if isinstance(value, ScalarLike) else value
    if resolved is None:
        return entry
    if isinstance(resolved, bool):
        entry["value_kind"] = "bool"
        entry["value_bool"] = resolved
    elif isinstance(resolved, int) and not isinstance(resolved, bool):
        entry["value_kind"] = "int64"
        entry["value_int"] = resolved
    elif isinstance(resolved, float):
        entry["value_kind"] = "float64"
        entry["value_float"] = resolved
    elif isinstance(resolved, bytes):
        entry["value_kind"] = "binary"
        entry["value_binary"] = resolved
    else:
        entry["value_kind"] = "string"
        entry["value_string"] = str(resolved)
    return entry


def _decode_scalar_value(entry: Mapping[str, object]) -> object:
    kind = entry.get("value_kind")
    if kind == "bool":
        value = entry.get("value_bool")
        return value if isinstance(value, bool) else None
    if kind == "int64":
        value = entry.get("value_int")
        return int(value) if isinstance(value, int) else None
    if kind == "float64":
        value = entry.get("value_float")
        return float(value) if isinstance(value, (int, float)) else None
    if kind == "binary":
        value = entry.get("value_binary")
        return value if isinstance(value, (bytes, bytearray)) else None
    if kind == "string":
        value = entry.get("value_string")
        return str(value) if value is not None else None
    return None


def options_hash(options: object) -> str:
    """Return a stable hash for options objects.

    Returns
    -------
    str
        SHA-256 hex digest of the normalized options payload.
    """
    normalized = _normalize_option_value(options)
    payload = {"version": OPTIONS_HASH_VERSION, "options": normalized}
    table = pa.Table.from_pylist([payload])
    return ipc_hash(table)


def options_metadata_spec(
    *,
    options: object | None = None,
    repo_id: str | None = None,
    extra: dict[bytes, bytes] | None = None,
) -> SchemaMetadataSpec:
    """Return schema metadata for options/repo identifiers.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec with run-specific identifiers.
    """
    meta: dict[bytes, bytes] = {}
    if options is not None:
        meta[b"options_hash"] = options_hash(options).encode("utf-8")
    if repo_id is not None:
        meta[b"repo_id"] = repo_id.encode("utf-8")
    if extra:
        meta.update(extra)
    return SchemaMetadataSpec(schema_metadata=meta)


def extractor_option_defaults_spec(
    defaults: Mapping[str, object],
) -> SchemaMetadataSpec:
    """Return schema metadata for extractor option defaults.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec storing IPC-encoded option defaults.
    """
    if not defaults:
        return SchemaMetadataSpec()
    payload = payload_ipc_bytes(
        _extractor_defaults_payload(defaults),
        _EXTRACTOR_DEFAULTS_SCHEMA,
    )
    return SchemaMetadataSpec(schema_metadata={EXTRACTOR_DEFAULTS_META: payload})


def extractor_option_defaults_from_metadata(
    source: Mapping[bytes, bytes] | SchemaLike,
) -> dict[str, object]:
    """Return extractor option defaults from schema metadata.

    Returns
    -------
    dict[str, object]
        Decoded option defaults, or an empty dict when missing.

    Raises
    ------
    TypeError
        Raised when metadata is not an IPC mapping payload.
    """
    metadata = source if isinstance(source, Mapping) else (source.metadata or {})
    payload = metadata.get(EXTRACTOR_DEFAULTS_META)
    if not payload:
        return {}
    table = ipc_table(payload)
    rows = table.to_pylist()
    if not rows:
        return {}
    row = rows[0]
    if not isinstance(row, Mapping):
        msg = "extractor_option_defaults metadata must be a mapping payload."
        raise TypeError(msg)
    return _extractor_defaults_from_row(row)


def _extractor_defaults_payload(defaults: Mapping[str, object]) -> dict[str, object]:
    normalized = _normalize_option_value(defaults)
    if not isinstance(normalized, Mapping):
        msg = "Extractor defaults must be a mapping."
        raise TypeError(msg)
    return {
        "version": EXTRACTOR_DEFAULTS_VERSION,
        "entries": [
            _extractor_defaults_entry(str(key), value)
            for key, value in sorted(normalized.items(), key=lambda item: str(item[0]))
        ],
    }


def _extractor_defaults_entry(key: str, value: object) -> dict[str, object]:
    entry: dict[str, object] = {
        "key": key,
        "value_kind": "null",
        "value_bool": None,
        "value_int": None,
        "value_float": None,
        "value_string": None,
        "value_strings": None,
    }
    if value is None:
        return entry
    if isinstance(value, bool):
        entry["value_kind"] = "bool"
        entry["value_bool"] = value
    elif isinstance(value, int) and not isinstance(value, bool):
        entry["value_kind"] = "int64"
        entry["value_int"] = value
    elif isinstance(value, float):
        entry["value_kind"] = "float64"
        entry["value_float"] = value
    elif isinstance(value, str):
        entry["value_kind"] = "string"
        entry["value_string"] = value
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        entry["value_kind"] = "string_list"
        entry["value_strings"] = [str(item) for item in value]
    else:
        entry["value_kind"] = "string"
        entry["value_string"] = _stable_repr(value)
    return entry


def _extractor_defaults_from_row(row: Mapping[str, object]) -> dict[str, object]:
    version = row.get("version")
    if version is not None:
        if isinstance(version, int) and not isinstance(version, bool):
            version_value = version
        elif isinstance(version, (str, bytes, bytearray)):
            version_value = int(version)
        else:
            msg = "extractor_option_defaults version must be an int."
            raise TypeError(msg)
        if version_value != EXTRACTOR_DEFAULTS_VERSION:
            msg = "extractor_option_defaults metadata version mismatch."
            raise ValueError(msg)
    entries = row.get("entries")
    if entries is None:
        return {}
    if not isinstance(entries, list):
        msg = "extractor_option_defaults entries must be a list."
        raise TypeError(msg)
    results: dict[str, object] = {}
    for entry in entries:
        if not isinstance(entry, Mapping):
            msg = "extractor_option_defaults entry must be a mapping."
            raise TypeError(msg)
        key = entry.get("key")
        if key is None:
            continue
        results[str(key)] = _extractor_default_value(entry)
    return results


def _extractor_default_value(entry: Mapping[str, object]) -> object:
    kind = entry.get("value_kind")
    if kind in {None, "null"}:
        result: object = None
    elif kind == "bool":
        value = entry.get("value_bool")
        result = value if isinstance(value, bool) else None
    elif kind == "int64":
        value = entry.get("value_int")
        result = int(value) if isinstance(value, int) else None
    elif kind == "float64":
        value = entry.get("value_float")
        result = float(value) if isinstance(value, (int, float)) else None
    elif kind == "string":
        value = entry.get("value_string")
        result = str(value) if value is not None else None
    elif kind == "string_list":
        value = entry.get("value_strings")
        result = [str(item) for item in value] if isinstance(value, list) else None
    else:
        result = None
    return result


@dataclass(frozen=True)
class EvidenceMetadataSpec:
    """Specification for common evidence metadata fields."""

    evidence_family: str
    coordinate_system: str
    ambiguity_policy: str
    superior_rank: int
    span_coord_policy: bytes | None = None
    streaming_safe: bool | None = None
    pipeline_breaker: bool | None = None


def evidence_metadata(
    *,
    spec: EvidenceMetadataSpec,
    extra: Mapping[bytes, bytes] | None = None,
) -> dict[bytes, bytes]:
    """Return evidence metadata with common keys applied.

    Returns
    -------
    dict[bytes, bytes]
        Metadata payload with evidence keys applied.
    """
    meta: dict[bytes, bytes] = {
        b"evidence_family": spec.evidence_family.encode("utf-8"),
        b"coordinate_system": spec.coordinate_system.encode("utf-8"),
        b"ambiguity_policy": spec.ambiguity_policy.encode("utf-8"),
        b"superior_rank": str(spec.superior_rank).encode("utf-8"),
    }
    if spec.span_coord_policy is not None:
        meta[b"span_coord_policy"] = spec.span_coord_policy
    if spec.streaming_safe is not None:
        meta[b"streaming_safe"] = str(spec.streaming_safe).lower().encode("utf-8")
    if spec.pipeline_breaker is not None:
        meta[b"pipeline_breaker"] = str(spec.pipeline_breaker).lower().encode("utf-8")
    if extra:
        meta.update(extra)
    return meta


@dataclass(frozen=True)
class EvidenceMetadata:
    """Evidence semantics payload for schema metadata."""

    labels: Mapping[str, str] = field(default_factory=dict)
    required_columns: Sequence[str] = ()
    required_types: Mapping[str, str] = field(default_factory=dict)
    evidence_rank: int | None = None


def evidence_metadata_spec(metadata: EvidenceMetadata) -> SchemaMetadataSpec:
    """Return schema metadata for evidence semantics.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec describing evidence semantics.
    """
    meta: dict[bytes, bytes] = {}
    for key, value in metadata.labels.items():
        if value:
            meta[key.encode("utf-8")] = value.encode("utf-8")
    if metadata.evidence_rank is not None:
        meta[b"evidence_rank"] = str(metadata.evidence_rank).encode("utf-8")
    if metadata.required_columns:
        meta[b"evidence_required_columns"] = metadata_list_bytes(metadata.required_columns)
    if metadata.required_types:
        meta[b"evidence_required_types"] = metadata_map_bytes(metadata.required_types)
    return SchemaMetadataSpec(schema_metadata=meta)


def metadata_spec_from_schema(schema: SchemaLike) -> SchemaMetadataSpec:
    """Capture schema/field metadata for inheritance.

    Returns
    -------
    SchemaMetadataSpec
        Metadata specification derived from the schema.
    """
    schema_meta = dict(schema.metadata or {})
    field_meta: dict[str, dict[bytes, bytes]] = {}
    for schema_field in schema:
        if schema_field.metadata is not None:
            field_meta[schema_field.name] = dict(schema_field.metadata)
        _add_nested_metadata(schema_field, field_meta)
    return SchemaMetadataSpec(schema_metadata=schema_meta, field_metadata=field_meta)


def _add_nested_metadata(field: FieldLike, field_meta: dict[str, dict[bytes, bytes]]) -> None:
    if patypes.is_struct(field.type):
        for child in field.flatten():
            if child.metadata is not None:
                field_meta[child.name] = dict(child.metadata)
        return

    if patypes.is_map(field.type):
        map_type = cast("_MapType", field.type)
        key_field = map_type.key_field
        item_field = map_type.item_field
        if key_field.metadata is not None:
            field_meta[f"{field.name}.{key_field.name}"] = dict(key_field.metadata)
        if item_field.metadata is not None:
            field_meta[f"{field.name}.{item_field.name}"] = dict(item_field.metadata)
        return

    if (
        patypes.is_list(field.type)
        or patypes.is_large_list(field.type)
        or patypes.is_list_view(field.type)
        or patypes.is_large_list_view(field.type)
    ):
        list_type = cast("_ListType", field.type)
        value_field = list_type.value_field
        if value_field.metadata is not None:
            field_meta[f"{field.name}.{value_field.name}"] = dict(value_field.metadata)


def update_field_metadata(
    table: TableLike,
    *,
    updates: Mapping[str, Mapping[bytes, bytes]],
) -> TableLike:
    """Return a table with field metadata updates applied.

    Returns
    -------
    TableLike
        Table with updated field metadata.
    """
    fields = []
    for schema_field in table.schema:
        meta = updates.get(schema_field.name)
        fields.append(schema_field.with_metadata(meta) if meta is not None else schema_field)
    schema = pa.schema(fields, metadata=table.schema.metadata)
    return table.cast(schema)


def _meta_value(meta: Mapping[bytes, bytes] | None, key: str) -> str | None:
    if not meta:
        return None
    raw = meta.get(key.encode("utf-8"))
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _meta_list(meta: Mapping[bytes, bytes] | None, key: str) -> tuple[str, ...]:
    payload = meta.get(key.encode("utf-8")) if meta else None
    if not payload:
        return ()
    return tuple(str(item) for item in decode_metadata_list(payload) if str(item))


def _parse_signature_count_entry(entry: str) -> tuple[str, int]:
    name = entry.strip()
    if not name:
        return "", 0
    for separator in (":", "="):
        if separator in name:
            raw_name, raw_count = name.split(separator, 1)
            raw_name = raw_name.strip()
            raw_count = raw_count.strip()
            if not raw_name:
                return "", 0
            try:
                count = int(raw_count)
            except ValueError:
                count = 1
            return raw_name, count
    return name, 1


def _parse_signature_type_entry(
    entry: str,
) -> tuple[str, tuple[frozenset[str] | None, ...]]:
    name = entry.strip()
    if not name:
        return "", ()
    if ":" not in name:
        return name, ()
    raw_name, raw_args = name.split(":", 1)
    raw_name = raw_name.strip()
    raw_args = raw_args.strip()
    if not raw_name:
        return "", ()
    if not raw_args:
        return raw_name, ()
    hints: list[frozenset[str] | None] = []
    for arg in raw_args.split(","):
        token = arg.strip()
        if not token or token.lower() in {"any", "*"}:
            hints.append(None)
            continue
        tokens = frozenset(part.strip().lower() for part in token.split("|") if part.strip())
        hints.append(tokens if tokens else None)
    return raw_name, tuple(hints)


def schema_constraints_from_metadata(
    metadata: Mapping[bytes, bytes] | None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return required non-null and key fields parsed from metadata.

    Returns
    -------
    tuple[tuple[str, ...], tuple[str, ...]]
        Required non-null and key field names.
    """
    if not metadata:
        return (), ()
    required = _metadata_names(metadata.get(REQUIRED_NON_NULL_META))
    key_fields = _metadata_names(metadata.get(KEY_FIELDS_META))
    return required, key_fields


def schema_identity_from_metadata(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    """Return schema identity metadata (name/version) if present.

    Returns
    -------
    dict[str, str]
        Schema identity fields derived from metadata.
    """
    if not metadata:
        return {}
    name = metadata.get(SCHEMA_META_NAME)
    version = metadata.get(SCHEMA_META_VERSION)
    decoded_name = name.decode("utf-8", errors="replace") if name else None
    decoded_version: int | None = None
    if version is not None:
        try:
            decoded_version = int(version.decode("utf-8", errors="replace"))
        except ValueError:
            decoded_version = None
    result: dict[str, str] = {}
    if decoded_name is not None:
        result["name"] = decoded_name
    if decoded_version is not None:
        result["version"] = str(decoded_version)
    return result


def infer_ordering_keys(names: Sequence[str]) -> tuple[OrderingKey, ...]:
    """Infer ordering keys from a list of column names.

    Returns
    -------
    tuple[OrderingKey, ...]
        Ordered key list for ordering metadata.
    """
    name_list = list(names)
    name_set = set(name_list)
    id_cols = sorted(name for name in name_list if name.endswith("_id"))
    keys: list[OrderingKey] = [(name, "ascending") for name in id_cols]
    selected = {name for name, _ in keys}
    for col in _POSITION_COLS:
        if col in name_set and col not in selected:
            keys.append((col, "ascending"))
            selected.add(col)
    return tuple(keys)


def ordering_metadata_spec(
    level: OrderingLevel,
    *,
    keys: Sequence[OrderingKey] = (),
    extra: dict[bytes, bytes] | None = None,
) -> SchemaMetadataSpec:
    """Return schema metadata describing ordering semantics.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec with ordering annotations.
    """
    meta = {b"ordering_level": level.value.encode("utf-8")}
    if keys:
        meta[b"ordering_keys"] = payload_ipc_bytes(
            _ordering_keys_payload(keys),
            _ORDERING_KEYS_SCHEMA,
        )
    if extra:
        meta.update(extra)
    return SchemaMetadataSpec(schema_metadata=meta)


def ordering_from_schema(schema: SchemaLike) -> Ordering:
    """Return ordering metadata parsed from a schema.

    Returns
    -------
    Ordering
        Ordering metadata derived from schema annotations.
    """
    metadata = schema.metadata or {}
    raw_level = metadata.get(b"ordering_level")
    if raw_level is None:
        return Ordering.unordered()
    try:
        level = OrderingLevel(raw_level.decode("utf-8"))
    except ValueError:
        return Ordering.unordered()
    raw_keys = metadata.get(b"ordering_keys")
    if raw_keys is None:
        return Ordering(level, ())
    return Ordering(level, _ordering_keys_from_payload(raw_keys))


def merge_metadata_specs(*specs: SchemaMetadataSpec | None) -> SchemaMetadataSpec:
    """Merge schema metadata specs into a single combined spec.

    Returns
    -------
    SchemaMetadataSpec
        Combined schema metadata spec.
    """
    schema_metadata: dict[bytes, bytes] = {}
    field_metadata: dict[str, dict[bytes, bytes]] = {}
    for spec in specs:
        if spec is None:
            continue
        schema_metadata.update(spec.schema_metadata)
        for field_name, meta in spec.field_metadata.items():
            field_metadata.setdefault(field_name, {}).update(meta)
    return SchemaMetadataSpec(schema_metadata=schema_metadata, field_metadata=field_metadata)


def _normalize_option_value(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return _normalize_option_value(asdict(value))
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, dict):
        normalized: dict[str, object] = {}
        for key, item in sorted(value.items(), key=lambda pair: str(pair[0])):
            normalized[str(key)] = _normalize_option_value(item)
        return normalized
    if isinstance(value, set):
        normalized_items = [_normalize_option_value(item) for item in value]
        return sorted(normalized_items, key=str)
    if isinstance(value, (tuple, list)):
        return [_normalize_option_value(item) for item in value]
    return value


def _stable_repr(value: object) -> str:
    if isinstance(value, Mapping):
        items = ", ".join(
            f"{_stable_repr(key)}:{_stable_repr(val)}"
            for key, val in sorted(value.items(), key=lambda item: str(item[0]))
        )
        return f"{{{items}}}"
    if isinstance(value, (list, tuple, set)):
        rendered = [_stable_repr(item) for item in value]
        if isinstance(value, set):
            rendered = sorted(rendered)
        items = ", ".join(rendered)
        bracket = "()" if isinstance(value, tuple) else "[]"
        return f"{bracket[0]}{items}{bracket[1]}"
    return repr(value)


def _index_type_from_meta(meta: Mapping[bytes, bytes] | None) -> DataTypeLike:
    raw = _meta_value(meta, DICT_INDEX_META)
    if raw is None:
        return pa.int32()
    return _INDEX_TYPES.get(raw.strip().lower(), pa.int32())


def _ordered_from_meta(meta: Mapping[bytes, bytes] | None) -> bool:
    raw = _meta_value(meta, DICT_ORDERED_META)
    if raw is None:
        return False
    return raw.strip().lower() in _ORDERED_TRUE


def _encoding_hint(field: _ArrowFieldSpec) -> str | None:
    if field.encoding is not None:
        return field.encoding
    return field.metadata.get(ENCODING_META)


def _encoding_info_from_field(
    field: _ArrowFieldSpec,
) -> tuple[str, DataTypeLike | None, bool | None] | None:
    hint = _encoding_hint(field)
    if hint != ENCODING_DICTIONARY:
        if patypes.is_dictionary(field.dtype):
            dtype = cast("pa.DictionaryType", field.dtype)
            return field.name, dtype.index_type, dtype.ordered
        return None
    if patypes.is_dictionary(field.dtype):
        dtype = cast("pa.DictionaryType", field.dtype)
        return field.name, dtype.index_type, dtype.ordered
    return field.name, None, None


def _encoding_info_from_metadata(
    field: FieldLike,
) -> tuple[str, DataTypeLike | None, bool | None] | None:
    meta = field.metadata or {}
    if meta.get(ENCODING_META.encode("utf-8")) != ENCODING_DICTIONARY.encode("utf-8"):
        return None
    if patypes.is_dictionary(field.type):
        dtype = cast("pa.DictionaryType", field.type)
        return field.name, dtype.index_type, dtype.ordered
    idx_type = _index_type_from_meta(meta)
    ordered = _ordered_from_meta(meta)
    return field.name, idx_type, ordered


def _build_encoding_policy(
    entries: Sequence[tuple[str, DataTypeLike | None, bool | None]],
) -> EncodingPolicy:
    dictionary_cols: set[str] = set()
    index_types: dict[str, DataTypeLike] = {}
    ordered_flags: dict[str, bool] = {}
    specs: list[EncodingSpec] = []
    for name, idx_type, ordered in entries:
        dictionary_cols.add(name)
        if idx_type is not None:
            index_types[name] = idx_type
        if ordered is not None:
            ordered_flags[name] = ordered
        specs.append(EncodingSpec(column=name, index_type=idx_type, ordered=ordered))
    return EncodingPolicy(
        dictionary_cols=frozenset(dictionary_cols),
        specs=tuple(specs),
        dictionary_index_types=index_types,
        dictionary_ordered_flags=ordered_flags,
    )


def encoding_policy_from_spec(table_spec: _TableSchemaSpec) -> EncodingPolicy:
    """Return an encoding policy derived from a TableSchemaSpec.

    Returns
    -------
    EncodingPolicy
        Encoding policy derived from the schema specification.
    """
    entries = [
        info
        for field in table_spec.fields
        if (info := _encoding_info_from_field(field)) is not None
    ]
    return _build_encoding_policy(entries)


def encoding_policy_from_fields(fields: Sequence[_ArrowFieldSpec]) -> EncodingPolicy:
    """Return an encoding policy derived from ArrowFieldSpec values.

    Returns
    -------
    EncodingPolicy
        Encoding policy derived from the field specs.
    """
    entries = [info for field in fields if (info := _encoding_info_from_field(field)) is not None]
    return _build_encoding_policy(entries)


def encoding_policy_from_schema(schema: SchemaLike) -> EncodingPolicy:
    """Return an encoding policy derived from schema field metadata.

    Returns
    -------
    EncodingPolicy
        Encoding policy derived from schema metadata.
    """
    entries = [
        info for field in schema if (info := _encoding_info_from_metadata(field)) is not None
    ]
    return _build_encoding_policy(entries)


def dictionary_array_from_indices(
    indices: ArrayLike | Sequence[int | None],
    dictionary: ArrayLike | Sequence[object],
    *,
    index_type: DataTypeLike | None = None,
    dictionary_type: DataTypeLike | None = None,
    ordered: bool = False,
) -> ArrayLike:
    """Build a dictionary array from indices and dictionary values.

    Returns
    -------
    ArrayLike
        Dictionary-encoded array.
    """
    return _dictionary_from_indices(
        indices,
        dictionary,
        index_type=index_type,
        dictionary_type=dictionary_type,
        ordered=ordered,
    )


def _ordering_keys_payload(keys: Sequence[OrderingKey]) -> dict[str, object]:
    return {
        "version": ORDERING_KEYS_VERSION,
        "entries": [{"column": str(column), "order": str(order)} for column, order in keys],
    }


def schema_metadata_for_spec(spec: TableSchemaSpec) -> dict[bytes, bytes]:
    """Return schema metadata for the given schema spec.

    Returns
    -------
    dict[bytes, bytes]
        Schema metadata encoded from the table spec.
    """
    metadata_spec = metadata_spec_from_schema(spec.to_arrow_schema())
    return metadata_spec.schema_metadata


def apply_spec_metadata(spec: TableSchemaSpec) -> SchemaMetadataSpec:
    """Return schema metadata spec for a table spec.

    Returns
    -------
    SchemaMetadataSpec
        Schema metadata spec derived from the table spec.
    """
    return metadata_spec_from_schema(spec.to_arrow_schema())


def extractor_metadata_spec(
    name: str,
    version: int,
    *,
    extra: Mapping[bytes, bytes] | None = None,
) -> SchemaMetadataSpec:
    """Return schema metadata for extractor provenance.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec encoding extractor provenance.
    """
    meta = {
        b"extractor_name": name.encode("utf-8"),
        b"extractor_version": str(version).encode("utf-8"),
    }
    if extra:
        meta.update(extra)
    return SchemaMetadataSpec(schema_metadata=meta)


def evidence_metadata_for_table(
    table: TableLike,
    *,
    spec: EvidenceMetadataSpec,
) -> TableLike:
    """Return a table with evidence metadata applied.

    Returns
    -------
    TableLike
        Table with evidence metadata applied.
    """
    metadata = evidence_metadata(spec=spec)
    spec_meta = SchemaMetadataSpec(schema_metadata=metadata)
    schema = spec_meta.apply(table.schema)
    return table.cast(schema)


def required_functions_from_metadata(metadata: Mapping[bytes, bytes] | None) -> tuple[str, ...]:
    """Return required function names parsed from metadata.

    Returns
    -------
    tuple[str, ...]
        Required function names.
    """
    payload = metadata.get(REQUIRED_FUNCTIONS_META) if metadata else None
    if not payload:
        return ()
    return tuple(str(item) for item in decode_metadata_list(payload) if str(item))


def optional_functions_from_metadata(metadata: Mapping[bytes, bytes] | None) -> tuple[str, ...]:
    """Return optional function names parsed from metadata.

    Returns
    -------
    tuple[str, ...]
        Optional function names.
    """
    payload = metadata.get(OPTIONAL_FUNCTIONS_META) if metadata else None
    if not payload:
        return ()
    return tuple(str(item) for item in decode_metadata_list(payload) if str(item))


def required_function_signatures_from_metadata(
    metadata: Mapping[bytes, bytes] | None,
) -> dict[str, int]:
    """Return required function signature counts parsed from metadata.

    Returns
    -------
    dict[str, int]
        Mapping of function name to required signature counts.
    """
    payload = metadata.get(REQUIRED_FUNCTION_SIGNATURES_META) if metadata else None
    if not payload:
        return {}
    entries = [str(item) for item in decode_metadata_list(payload) if str(item)]
    counts: dict[str, int] = {}
    for entry in entries:
        name, count = _parse_signature_count_entry(entry)
        if name:
            counts[name] = max(counts.get(name, 0), count)
    return counts


def required_function_signature_types_from_metadata(
    metadata: Mapping[bytes, bytes] | None,
) -> dict[str, tuple[frozenset[str] | None, ...]]:
    """Return required signature type hints parsed from metadata.

    Returns
    -------
    dict[str, tuple[frozenset[str] | None, ...]]
        Mapping of function name to required argument type hints.
    """
    payload = metadata.get(REQUIRED_FUNCTION_SIGNATURE_TYPES_META) if metadata else None
    if not payload:
        return {}
    entries = [str(item) for item in decode_metadata_list(payload) if str(item)]
    parsed: dict[str, tuple[frozenset[str] | None, ...]] = {}
    for entry in entries:
        name, hints = _parse_signature_type_entry(entry)
        if name:
            parsed[name] = hints
    return parsed


def function_requirements_metadata_spec(
    *,
    required: Sequence[str],
    optional: Sequence[str] = (),
    signatures: Sequence[str] = (),
    signature_types: Sequence[str] = (),
) -> SchemaMetadataSpec:
    """Return schema metadata spec encoding function requirements.

    Returns
    -------
    SchemaMetadataSpec
        Schema metadata spec encoding function requirements.
    """
    meta: dict[bytes, bytes] = {}
    if required:
        meta[REQUIRED_FUNCTIONS_META] = metadata_list_bytes(required)
    if optional:
        meta[OPTIONAL_FUNCTIONS_META] = metadata_list_bytes(optional)
    if signatures:
        meta[REQUIRED_FUNCTION_SIGNATURES_META] = metadata_list_bytes(signatures)
    if signature_types:
        meta[REQUIRED_FUNCTION_SIGNATURE_TYPES_META] = metadata_list_bytes(signature_types)
    return SchemaMetadataSpec(schema_metadata=meta)


def normalize_dictionaries_for_schema(
    table: TableLike,
    *,
    schema: SchemaLike,
) -> TableLike:
    """Normalize dictionary encoding for a schema.

    Returns
    -------
    TableLike
        Table with normalized dictionary encoding.
    """
    normalized = normalize_dictionaries(table, combine_chunks=True)
    return normalized.cast(pa.schema(schema))


def _metadata_payload_for_schema(schema: SchemaLike) -> dict[str, object]:
    return {
        "schema": metadata_spec_from_schema(schema).schema_metadata,
        "field_metadata": metadata_spec_from_schema(schema).field_metadata,
    }


def metadata_payload(
    schema: SchemaLike,
) -> dict[str, object]:
    """Return metadata payload for diagnostics.

    Returns
    -------
    dict[str, object]
        Schema metadata payload for diagnostics.
    """
    return _metadata_payload_for_schema(schema)


def _ordering_keys_from_payload(payload: bytes) -> tuple[OrderingKey, ...]:
    table = ipc_table(payload)
    rows = table.to_pylist()
    if not rows:
        return ()
    row = rows[0]
    if not isinstance(row, Mapping):
        msg = "ordering_keys metadata must be a mapping payload."
        raise TypeError(msg)
    version = row.get("version")
    if version is not None and int(version) != ORDERING_KEYS_VERSION:
        msg = "ordering_keys metadata version mismatch."
        raise ValueError(msg)
    entries = row.get("entries")
    if entries is None:
        return ()
    if not isinstance(entries, list):
        msg = "ordering_keys metadata entries must be a list."
        raise TypeError(msg)
    keys: list[OrderingKey] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            msg = "ordering_keys metadata entry must be a mapping."
            raise TypeError(msg)
        column = entry.get("column")
        order = entry.get("order")
        if column is None:
            continue
        keys.append((str(column), str(order) if order is not None else "ascending"))
    return tuple(keys)


def _metadata_names(payload: bytes | None) -> tuple[str, ...]:
    if payload is None:
        return ()
    return tuple(str(item) for item in decode_metadata_list(payload) if str(item))


def __getattr__(name: str) -> object:
    if name == "TableSchemaSpec":
        module = importlib.import_module("schema_spec.specs")
        return getattr(module, name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    return sorted([*list(globals()), "TableSchemaSpec"])


__all__ = [
    "DICT_INDEX_META",
    "DICT_ORDERED_META",
    "ENCODING_DICTIONARY",
    "ENCODING_META",
    "EXTRACTOR_DEFAULTS_META",
    "KEY_FIELDS_META",
    "OPTIONAL_FUNCTIONS_META",
    "REQUIRED_FUNCTIONS_META",
    "REQUIRED_FUNCTION_SIGNATURES_META",
    "REQUIRED_FUNCTION_SIGNATURE_TYPES_META",
    "REQUIRED_NON_NULL_META",
    "SCHEMA_META_NAME",
    "SCHEMA_META_VERSION",
    "EvidenceMetadata",
    "EvidenceMetadataSpec",
    "SchemaMetadataSpec",
    "TableSchemaSpec",
    "apply_spec_metadata",
    "decode_metadata_list",
    "decode_metadata_map",
    "decode_metadata_scalar_map",
    "dict_field_metadata",
    "dictionary_array_from_indices",
    "encoding_policy_from_fields",
    "encoding_policy_from_schema",
    "encoding_policy_from_spec",
    "evidence_metadata",
    "evidence_metadata_spec",
    "extractor_metadata_spec",
    "extractor_option_defaults_from_metadata",
    "extractor_option_defaults_spec",
    "function_requirements_metadata_spec",
    "infer_ordering_keys",
    "merge_metadata_specs",
    "metadata_list_bytes",
    "metadata_map_bytes",
    "metadata_scalar_map_bytes",
    "metadata_spec_from_schema",
    "normalize_dictionaries",
    "optional_functions_from_metadata",
    "options_hash",
    "options_metadata_spec",
    "ordering_from_schema",
    "ordering_metadata_spec",
    "required_function_signature_types_from_metadata",
    "required_function_signatures_from_metadata",
    "required_functions_from_metadata",
    "schema_constraints_from_metadata",
    "schema_identity_from_metadata",
    "schema_metadata_for_spec",
    "update_field_metadata",
]
