"""Schema metadata helpers for ordering and provenance."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.context import OrderingKey, OrderingLevel
from arrowdsl.core.interop import FieldLike, SchemaLike, TableLike
from arrowdsl.schema.schema import SchemaMetadataSpec
from schema_spec import PROVENANCE_COLS

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


def metadata_spec_from_schema(schema: SchemaLike) -> SchemaMetadataSpec:
    """Capture schema/field metadata for inheritance.

    Returns
    -------
    SchemaMetadataSpec
        Metadata specification derived from the schema.
    """
    schema_meta = dict(schema.metadata or {})
    field_meta: dict[str, dict[bytes, bytes]] = {}
    for field in schema:
        if field.metadata is not None:
            field_meta[field.name] = dict(field.metadata)
        _add_nested_metadata(field, field_meta)
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
    for field in table.schema:
        meta = updates.get(field.name)
        fields.append(field.with_metadata(meta) if meta is not None else field)
    schema = pa.schema(fields, metadata=table.schema.metadata)
    return table.cast(schema)


__all__ = [
    "extractor_metadata_spec",
    "infer_ordering_keys",
    "merge_metadata_specs",
    "metadata_spec_from_schema",
    "options_hash",
    "options_metadata_spec",
    "ordering_metadata_spec",
    "update_field_metadata",
]
