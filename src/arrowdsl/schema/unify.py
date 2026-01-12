"""Schema unification helpers with metadata preservation."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.interop import FieldLike, SchemaLike, TableLike
from arrowdsl.schema.metadata import metadata_spec_from_schema
from arrowdsl.schema.schema import SchemaEvolutionSpec


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
    return metadata_spec_from_schema(schemas[0]).apply(unified)


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
