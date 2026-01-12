"""Infer and align Arrow schemas across extraction tables."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd
import pandera.pandas as pa_pd
import pyarrow.types as patypes

import arrowdsl.pyarrow_core as pa
from arrowdsl.pyarrow_protocols import DataTypeLike, FieldLike, SchemaLike, TableLike
from arrowdsl.schema_ops import SchemaTransform


@dataclass(frozen=True)
class SchemaInferOptions:
    """Schema inference and alignment policy.

    promote_options="permissive" is the key setting that makes this system robust to
    missing columns, type drift, and partial extraction results.
    """

    promote_options: str = "permissive"
    keep_extra_columns: bool = False
    safe_cast: bool = False  # False is usually what you want for “accept ambiguity” systems.
    use_pandera_infer: bool = True


def _pandera_infer_schema(table: TableLike) -> SchemaLike | None:
    if table.num_rows == 0:
        return None
    df = pd.DataFrame(table.to_pydict())
    pandera_schema = pa_pd.infer_schema(df)
    return _pandera_pandas_schema_to_arrow(pandera_schema)


def _pandas_dtype_to_arrow(dtype: object) -> DataTypeLike:
    dtype_value = getattr(dtype, "type", None)
    if dtype_value is None:
        dtype_value = dtype
    try:
        pandas_dtype = pd.api.types.pandas_dtype(dtype_value)
    except TypeError:
        return pa.binary()
    try:
        arrow_dtype = pa.from_numpy_dtype(pandas_dtype)
    except (TypeError, ValueError):
        arrow_dtype = None
    if arrow_dtype is None:
        if pd.api.types.is_string_dtype(pandas_dtype):
            arrow_dtype = pa.string()
        elif pd.api.types.is_bool_dtype(pandas_dtype):
            arrow_dtype = pa.bool_()
        elif pd.api.types.is_integer_dtype(pandas_dtype):
            arrow_dtype = pa.int64()
        elif pd.api.types.is_float_dtype(pandas_dtype):
            arrow_dtype = pa.float64()
        else:
            arrow_dtype = pa.binary()
    return arrow_dtype


def _pandera_pandas_schema_to_arrow(schema: pa_pd.DataFrameSchema) -> SchemaLike:
    fields: list[FieldLike] = []
    for name, column in schema.columns.items():
        arrow_dtype = _pandas_dtype_to_arrow(column.dtype)
        fields.append(pa.field(name, arrow_dtype, nullable=column.nullable))
    return pa.schema(fields)


def _prefer_arrow_nested(base: SchemaLike, merged: SchemaLike) -> SchemaLike:
    fields: list[FieldLike] = []
    base_map = {field.name: field for field in base}
    for field in merged:
        base_field = base_map.get(field.name)
        if base_field and (patypes.is_struct(base_field.type) or patypes.is_list(base_field.type)):
            fields.append(base_field)
        else:
            fields.append(field)
    return pa.schema(fields)


def unify_schemas(
    schemas: Sequence[SchemaLike], opts: SchemaInferOptions | None = None
) -> SchemaLike:
    """Unify schemas across tables or fragments with permissive promotion.

    Notes
    -----
      - pa.unify_schemas supports promote_options in modern pyarrow.
      - we fall back if the runtime pyarrow is older.

    Returns
    -------
    SchemaLike
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
    tables: Sequence[TableLike], opts: SchemaInferOptions | None = None
) -> SchemaLike:
    """Compute a unified schema for a set of tables.

    Returns
    -------
    SchemaLike
        Unified schema inferred from the input tables.
    """
    opts = opts or SchemaInferOptions()
    schemas = [t.schema for t in tables if t is not None]
    arrow_schema = unify_schemas(schemas, opts=opts)
    if not opts.use_pandera_infer:
        return arrow_schema

    sample = next((t for t in tables if t is not None and t.num_rows > 0), None)
    if sample is None:
        return arrow_schema

    inferred = _pandera_infer_schema(sample)
    if inferred is None:
        return arrow_schema
    try:
        merged = pa.unify_schemas([arrow_schema, inferred], promote_options=opts.promote_options)
    except TypeError:
        merged = pa.unify_schemas([arrow_schema, inferred])
    return _prefer_arrow_nested(arrow_schema, merged)


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
    schema = infer_schema_from_tables(list(tables), opts=opts)
    transform = SchemaTransform(
        schema=schema,
        safe_cast=opts.safe_cast,
        keep_extra_columns=opts.keep_extra_columns,
        on_error="keep",
    )
    aligned = [transform.apply(t) for t in tables]
    return schema, aligned
