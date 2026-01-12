"""Schema unification helpers with metadata preservation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.interop import FieldLike, SchemaLike, TableLike
from arrowdsl.schema.schema import SchemaEvolutionSpec, SchemaMetadataSpec


class _ListType(Protocol):
    value_field: FieldLike


class _MapType(Protocol):
    key_field: FieldLike
    item_field: FieldLike


def _is_advanced_type(dtype: object) -> bool:
    return any(
        (
            patypes.is_struct(dtype),
            patypes.is_list(dtype),
            patypes.is_large_list(dtype),
            patypes.is_list_view(dtype),
            patypes.is_large_list_view(dtype),
            patypes.is_map(dtype),
            patypes.is_dictionary(dtype),
        )
    )


def _prefer_base_nested(base: SchemaLike, merged: SchemaLike) -> SchemaLike:
    fields: list[FieldLike] = []
    base_map = {field.name: field for field in base}
    for field in merged:
        base_field = base_map.get(field.name)
        if base_field is not None and _is_advanced_type(base_field.type):
            fields.append(base_field)
        else:
            fields.append(field)
    return pa.schema(fields)


def _metadata_spec_from_schema(schema: SchemaLike) -> SchemaMetadataSpec:
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


def unify_schemas(
    schemas: Sequence[SchemaLike],
    *,
    promote_options: str = "permissive",
    prefer_nested: bool = True,
) -> SchemaLike:
    """Unify schemas while preserving metadata from the first schema.

    Returns
    -------
    SchemaLike
        Unified schema with metadata preserved.
    """
    if not schemas:
        return pa.schema([])
    evolution = SchemaEvolutionSpec(promote_options=promote_options)
    unified = evolution.unify_schema_from_schemas(schemas)
    if prefer_nested:
        unified = _prefer_base_nested(schemas[0], unified)
    return _metadata_spec_from_schema(schemas[0]).apply(unified)


def unify_tables(
    tables: Sequence[TableLike],
    *,
    promote_options: str = "permissive",
) -> TableLike:
    """Unify and concatenate tables with metadata-aware schema alignment.

    Returns
    -------
    TableLike
        Concatenated table aligned to the unified schema.
    """
    if not tables:
        return pa.Table.from_arrays([], names=[])
    schema = unify_schemas([table.schema for table in tables], promote_options=promote_options)
    aligned = [table.cast(schema) for table in tables]
    return pa.concat_tables(aligned)


__all__ = ["unify_schemas", "unify_tables"]
