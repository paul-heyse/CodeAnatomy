"""Infer and align Arrow schemas across extraction tables."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.schema.infer import infer_schema_from_tables as arrowdsl_infer_tables
from arrowdsl.schema.metadata import metadata_spec_from_schema
from arrowdsl.schema.schema import SchemaTransform
from arrowdsl.schema.unify import unify_schemas as arrowdsl_unify_schemas
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY


@dataclass(frozen=True)
class SchemaInferOptions:
    """Schema inference and alignment policy.

    promote_options="permissive" is the key setting that makes this system robust to
    missing columns, type drift, and partial extraction results.
    """

    promote_options: str = "permissive"
    keep_extra_columns: bool = False
    safe_cast: bool = False  # False is usually what you want for “accept ambiguity” systems.


def unify_schemas(
    schemas: Sequence[SchemaLike], opts: SchemaInferOptions | None = None
) -> SchemaLike:
    """Unify schemas across tables or fragments with permissive promotion.

    Notes
    -----
      - pa.unify_schemas supports promote_options in modern pyarrow.
      - we fall back if the runtime pyarrow is older.
      - metadata is inherited from the first schema in the list.

    Returns
    -------
    SchemaLike
        Unified schema.
    """
    opts = opts or SchemaInferOptions()
    return arrowdsl_unify_schemas(
        schemas,
        promote_options=opts.promote_options,
        prefer_nested=True,
    )


def infer_schema_from_tables(
    tables: Sequence[TableLike], opts: SchemaInferOptions | None = None
) -> SchemaLike:
    """Compute a unified schema for a set of tables.

    Returns
    -------
    SchemaLike
        Unified schema inferred from the input tables.
    """
    opts = opts or SchemaInferOptions()
    present = [t for t in tables if t is not None]
    arrow_schema = arrowdsl_infer_tables(present, promote_options=opts.promote_options)
    if not present:
        return arrow_schema
    sample = next((t for t in present if t.num_rows > 0), None) or present[0]
    return metadata_spec_from_schema(sample.schema).apply(arrow_schema)


def infer_schema_or_registry(
    name: str,
    tables: Sequence[TableLike],
    *,
    opts: SchemaInferOptions | None = None,
) -> SchemaLike:
    """Infer a schema from tables with a registry fallback.

    Returns
    -------
    SchemaLike
        Inferred schema when evidence exists, otherwise registry or empty schema.
    """
    present = [table for table in tables if table is not None and table.column_names]
    if present:
        return infer_schema_from_tables(present, opts=opts)
    spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(name)
    if spec is not None:
        return spec.table_spec.to_arrow_schema()
    return pa.schema([])


def align_table_to_schema(
    table: TableLike,
    schema: SchemaLike,
    *,
    opts: SchemaInferOptions | None = None,
) -> TableLike:
    """Align a table to a target schema.

    Aligns the table by:
      - adding missing columns as nulls
      - casting existing columns to the target types (safe_cast configurable)
      - optionally dropping extra columns
      - ensuring the output column order matches schema

    This is a foundational building block for “inference-driven, schema-flexible” pipelines.

    Returns
    -------
    TableLike
        Table aligned to the provided schema.
    """
    opts = opts or SchemaInferOptions()
    transform = SchemaTransform(
        schema=schema,
        safe_cast=opts.safe_cast,
        keep_extra_columns=opts.keep_extra_columns,
        on_error="keep",
    )
    return transform.apply(table)


def align_tables_to_unified_schema(
    tables: Sequence[TableLike],
    *,
    opts: SchemaInferOptions | None = None,
) -> tuple[SchemaLike, list[TableLike]]:
    """Infer a unified schema and align all tables to it.

    Returns
    -------
    tuple[SchemaLike, list[TableLike]]
        Unified schema and aligned tables.
    """
    opts = opts or SchemaInferOptions()
    schema = unify_schemas([table.schema for table in tables], opts=opts)
    transform = SchemaTransform(
        schema=schema,
        safe_cast=opts.safe_cast,
        keep_extra_columns=opts.keep_extra_columns,
        on_error="keep",
    )
    aligned = [transform.apply(t) for t in tables]
    return schema, aligned
