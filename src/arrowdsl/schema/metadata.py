"""Schema metadata helpers for ordering and provenance."""

from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.context import Ordering, OrderingKey, OrderingLevel
from arrowdsl.core.interop import ArrayLike, DataTypeLike, FieldLike, SchemaLike, TableLike
from arrowdsl.core.schema_constants import (
    KEY_FIELDS_META,
    PROVENANCE_COLS,
    REQUIRED_NON_NULL_META,
    SCHEMA_META_NAME,
    SCHEMA_META_VERSION,
)
from arrowdsl.json_factory import JsonPolicy, dumps_bytes, loads
from arrowdsl.schema.encoding_policy import EncodingPolicy, EncodingSpec
from arrowdsl.schema.nested_builders import (
    dictionary_array_from_indices as _dictionary_from_indices,
)
from arrowdsl.schema.schema import SchemaMetadataSpec

if TYPE_CHECKING:
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


class _ListType(Protocol):
    value_field: FieldLike


class _MapType(Protocol):
    key_field: FieldLike
    item_field: FieldLike


class _ArrowFieldSpec(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def dtype(self) -> DataTypeLike: ...

    @property
    def metadata(self) -> Mapping[str, str]: ...

    @property
    def encoding(self) -> str | None: ...


class _TableSchemaSpec(Protocol):
    @property
    def fields(self) -> Sequence[_ArrowFieldSpec]: ...


ENCODING_META = "encoding"
ENCODING_DICTIONARY = "dictionary"
DICT_INDEX_META = "dictionary_index_type"
DICT_ORDERED_META = "dictionary_ordered"
EXTRACTOR_DEFAULTS_META = b"extractor_option_defaults"

_ORDERED_TRUE = {"1", "true", "yes", "y", "t"}
_INDEX_TYPES: Mapping[str, pa.DataType] = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
}


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


def options_hash(options: object) -> str:
    """Return a stable hash for options objects.

    Returns
    -------
    str
        SHA-256 hex digest of the normalized options payload.
    """
    normalized = _normalize_option_value(options)
    payload = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


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
        Metadata spec storing JSON-encoded option defaults.
    """
    if not defaults:
        return SchemaMetadataSpec()
    payload = dumps_bytes(defaults, policy=JsonPolicy.canonical_ascii())
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
        Raised when metadata is not a JSON object.
    """
    metadata = source if isinstance(source, Mapping) else (source.metadata or {})
    payload = metadata.get(EXTRACTOR_DEFAULTS_META)
    if not payload:
        return {}
    parsed = loads(payload)
    if isinstance(parsed, Mapping):
        return {str(key): value for key, value in parsed.items()}
    msg = "extractor_option_defaults metadata must be a JSON object."
    raise TypeError(msg)


def merge_metadata_specs(*specs: SchemaMetadataSpec | None) -> SchemaMetadataSpec:
    """Merge multiple metadata specs into a single spec.

    Returns
    -------
    SchemaMetadataSpec
        Combined metadata spec with later specs overriding earlier ones.
    """
    schema_metadata: dict[bytes, bytes] = {}
    field_metadata: dict[str, dict[bytes, bytes]] = {}
    for spec in specs:
        if spec is None:
            continue
        if spec.schema_metadata:
            schema_metadata.update(spec.schema_metadata)
        for name, meta in spec.field_metadata.items():
            merged = field_metadata.setdefault(name, {})
            merged.update(meta)
    return SchemaMetadataSpec(schema_metadata=schema_metadata, field_metadata=field_metadata)


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
        key_text = ",".join(f"{col}:{order}" for col, order in keys)
        meta[b"ordering_keys"] = key_text.encode("utf-8")
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
    text = raw_keys.decode("utf-8")
    keys: list[OrderingKey] = []
    for entry in text.split(","):
        col, _, order = entry.partition(":")
        if not col:
            continue
        keys.append((col, order or "ascending"))
    return Ordering(level, tuple(keys))


def extractor_metadata_spec(
    name: str,
    version: int,
    *,
    extra: dict[bytes, bytes] | None = None,
) -> SchemaMetadataSpec:
    """Return schema metadata for extractor provenance.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec with extractor provenance fields.
    """
    meta = {
        b"extractor_name": name.encode("utf-8"),
        b"extractor_version": str(version).encode("utf-8"),
    }
    if extra:
        meta.update(extra)
    return SchemaMetadataSpec(schema_metadata=meta)


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
        meta[b"evidence_required_columns"] = ",".join(metadata.required_columns).encode("utf-8")
    if metadata.required_types:
        payload = json.dumps(metadata.required_types, sort_keys=True, separators=(",", ":"))
        meta[b"evidence_required_types"] = payload.encode("utf-8")
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


def _index_type_from_meta(meta: Mapping[bytes, bytes] | None) -> pa.DataType:
    raw = _meta_value(meta, DICT_INDEX_META)
    if raw is None:
        return pa.int32()
    return _INDEX_TYPES.get(raw.strip().lower(), pa.int32())


def _ordered_from_meta(meta: Mapping[bytes, bytes] | None) -> bool:
    raw = _meta_value(meta, DICT_ORDERED_META)
    if raw is None:
        return False
    return raw.strip().lower() in _ORDERED_TRUE


def dict_field_metadata(
    *,
    index_type: pa.DataType | None = None,
    ordered: bool = False,
    metadata: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return metadata for dictionary-encoded field specs.

    Returns
    -------
    dict[str, str]
        Metadata mapping for dictionary encoding.
    """
    idx_type = index_type or pa.int32()
    meta = {
        ENCODING_META: ENCODING_DICTIONARY,
        DICT_INDEX_META: str(idx_type),
        DICT_ORDERED_META: "1" if ordered else "0",
    }
    if metadata is not None:
        meta.update(metadata)
    return meta


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
        Encoding policy derived from the table spec.
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
        Encoding policy derived from field specs.
    """
    entries = [info for field in fields if (info := _encoding_info_from_field(field)) is not None]
    return _build_encoding_policy(entries)


def encoding_policy_from_schema(schema: SchemaLike) -> EncodingPolicy:
    """Return an encoding policy derived from schema field metadata.

    Returns
    -------
    EncodingPolicy
        Encoding policy for dictionary-encoded columns.
    """
    entries = [
        info for field in schema if (info := _encoding_info_from_metadata(field)) is not None
    ]
    return _build_encoding_policy(entries)


def normalize_dictionaries(
    table: TableLike,
    *,
    combine_chunks: bool = True,
) -> TableLike:
    """Return a table with unified dictionaries and normalized chunks.

    Returns
    -------
    TableLike
        Table with unified dictionary columns.
    """
    out = table.combine_chunks() if combine_chunks else table
    return out.unify_dictionaries()


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
        Dictionary array with explicit dictionary values.
    """
    return _dictionary_from_indices(
        indices,
        dictionary,
        index_type=index_type,
        dictionary_type=dictionary_type,
        ordered=ordered,
    )


def _split_names(raw: bytes | None) -> tuple[str, ...]:
    if raw is None:
        return ()
    text = raw.decode("utf-8", errors="replace")
    return tuple(name for name in text.split(",") if name)


def schema_metadata_for_spec(spec: TableSchemaSpec) -> dict[bytes, bytes]:
    """Return schema metadata for the given schema spec.

    Returns
    -------
    dict[bytes, bytes]
        Encoded schema metadata mapping.
    """
    module = importlib.import_module("schema_spec.specs")
    metadata_fn = cast(
        "Callable[[TableSchemaSpec], dict[bytes, bytes]]",
        module.schema_metadata_for_spec,
    )
    return metadata_fn(spec)


def apply_spec_metadata(spec: TableSchemaSpec) -> SchemaMetadataSpec:
    """Return a metadata spec for the provided schema spec.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for the schema.
    """
    return SchemaMetadataSpec(schema_metadata=schema_metadata_for_spec(spec))


def schema_identity_from_metadata(
    metadata: Mapping[bytes, bytes] | None,
) -> tuple[str | None, int | None]:
    """Return schema name/version derived from metadata.

    Returns
    -------
    tuple[str | None, int | None]
        Schema name and version, when available.
    """
    if not metadata:
        return None, None
    name = metadata.get(SCHEMA_META_NAME)
    version = metadata.get(SCHEMA_META_VERSION)
    decoded_name = name.decode("utf-8", errors="replace") if name is not None else None
    decoded_version = None
    if version is not None:
        try:
            decoded_version = int(version.decode("utf-8", errors="replace"))
        except ValueError:
            decoded_version = None
    return decoded_name, decoded_version


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
    required = _split_names(metadata.get(REQUIRED_NON_NULL_META))
    key_fields = _split_names(metadata.get(KEY_FIELDS_META))
    return required, key_fields


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
    "REQUIRED_NON_NULL_META",
    "SCHEMA_META_NAME",
    "SCHEMA_META_VERSION",
    "EvidenceMetadata",
    "TableSchemaSpec",
    "apply_spec_metadata",
    "dict_field_metadata",
    "dictionary_array_from_indices",
    "encoding_policy_from_fields",
    "encoding_policy_from_schema",
    "encoding_policy_from_spec",
    "evidence_metadata_spec",
    "extractor_metadata_spec",
    "extractor_option_defaults_from_metadata",
    "extractor_option_defaults_spec",
    "infer_ordering_keys",
    "merge_metadata_specs",
    "metadata_spec_from_schema",
    "normalize_dictionaries",
    "options_hash",
    "options_metadata_spec",
    "ordering_from_schema",
    "ordering_metadata_spec",
    "schema_constraints_from_metadata",
    "schema_identity_from_metadata",
    "schema_metadata_for_spec",
    "update_field_metadata",
]
