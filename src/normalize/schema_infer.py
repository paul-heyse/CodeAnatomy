"""Infer and align Arrow schemas across extraction tables."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.types as patypes


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
    schemas: Sequence[pa.Schema], opts: SchemaInferOptions | None = None
) -> pa.Schema:
    """Unify schemas across tables or fragments with permissive promotion.

    Notes
    -----
      - pa.unify_schemas supports promote_options in modern pyarrow.
      - we fall back if the runtime pyarrow is older.

    Returns
    -------
    pa.Schema
        Unified schema.
    """
    opts = opts or SchemaInferOptions()
    if not schemas:
        return pa.schema([])

    try:
        return pa.unify_schemas(list(schemas), promote_options=opts.promote_options)
    except TypeError:
        # Older pyarrow without promote_options
        return pa.unify_schemas(list(schemas))


def infer_schema_from_tables(
    tables: Sequence[pa.Table], opts: SchemaInferOptions | None = None
) -> pa.Schema:
    """Compute a unified schema for a set of tables.

    Returns
    -------
    pa.Schema
        Unified schema inferred from the input tables.
    """
    opts = opts or SchemaInferOptions()
    schemas = [t.schema for t in tables if t is not None]
    return unify_schemas(schemas, opts=opts)


def _nulls(n: int, typ: pa.DataType) -> pa.Array:
    return pa.nulls(n, type=typ)


def align_table_to_schema(
    table: pa.Table,
    schema: pa.Schema,
    *,
    opts: SchemaInferOptions | None = None,
) -> pa.Table:
    """Align a table to a target schema.

    Aligns the table by:
      - adding missing columns as nulls
      - casting existing columns to the target types (safe_cast configurable)
      - optionally dropping extra columns
      - ensuring the output column order matches schema

    This is a foundational building block for “inference-driven, schema-flexible” pipelines.

    Returns
    -------
    pa.Table
        Table aligned to the provided schema.
    """
    opts = opts or SchemaInferOptions()
    n = table.num_rows
    out_arrays: list[pa.Array] = []
    out_names: list[str] = []

    table_cols = set(table.column_names)

    for field in schema:
        name = field.name
        typ = field.type
        out_names.append(name)

        if name not in table_cols:
            out_arrays.append(_nulls(n, typ))
            continue

        col = table[name]
        if patypes.is_null(col.type):
            out_arrays.append(_nulls(n, typ))
            continue

        if col.type == typ:
            out_arrays.append(col.combine_chunks() if hasattr(col, "combine_chunks") else col)
            continue

        # cast
        try:
            casted = pc.cast(col, typ, safe=opts.safe_cast)
            out_arrays.append(casted)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            out_arrays.append(col)

    out = pa.Table.from_arrays(out_arrays, names=out_names)

    if opts.keep_extra_columns:
        # append extra columns at the end
        for name in table.column_names:
            if name not in out.column_names:
                out = out.append_column(name, table[name])

    return out


def align_tables_to_unified_schema(
    tables: Sequence[pa.Table],
    *,
    opts: SchemaInferOptions | None = None,
) -> tuple[pa.Schema, list[pa.Table]]:
    """Infer a unified schema and align all tables to it.

    Returns
    -------
    tuple[pa.Schema, list[pa.Table]]
        Unified schema and aligned tables.
    """
    opts = opts or SchemaInferOptions()
    schema = infer_schema_from_tables(list(tables), opts=opts)
    aligned = [align_table_to_schema(t, schema, opts=opts) for t in tables]
    return schema, aligned
