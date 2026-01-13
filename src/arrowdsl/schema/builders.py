"""Canonical table and column builders for ArrowDSL."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.compute.macros import ColumnOrNullExpr
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    DataTypeLike,
    SchemaLike,
    TableLike,
    pc,
)
from arrowdsl.schema.nested_builders import nested_array_factory


def column_or_null(
    table: TableLike,
    col: str,
    dtype: DataTypeLike,
) -> ArrayLike:
    """Return a column array or a typed null array when missing.

    Returns
    -------
    ArrayLike
        Column array or a typed null array.
    """
    return ColumnOrNullExpr(name=col, dtype=dtype).materialize(table)


def maybe_dictionary(
    values: ArrayLike | ChunkedArrayLike,
    dtype: DataTypeLike,
) -> ArrayLike | ChunkedArrayLike:
    """Return dictionary-encoded values when requested by dtype.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Dictionary-encoded values when needed.
    """
    if patypes.is_dictionary(values.type):
        return values
    if patypes.is_dictionary(dtype):
        return pc.dictionary_encode(values)
    return values


def pick_first(
    table: TableLike,
    cols: Sequence[str],
    *,
    default_type: DataTypeLike,
) -> ArrayLike:
    """Return the first available column or a typed null array.

    Returns
    -------
    ArrayLike
        Column array or typed nulls.
    """
    for col in cols:
        if col in table.column_names:
            return table[col]
    return pa.nulls(table.num_rows, type=default_type)


def resolve_string_col(table: TableLike, col: str, *, default_value: str) -> ArrayLike:
    """Resolve a string column with nulls filled.

    Returns
    -------
    ArrayLike
        Column array with nulls filled.
    """
    arr = pick_first(table, [col], default_type=pa.string())
    if arr.null_count == 0:
        return arr
    return pc.fill_null(arr, fill_value=default_value)


def resolve_float_col(table: TableLike, col: str, *, default_value: float) -> ArrayLike:
    """Resolve a float column with nulls filled.

    Returns
    -------
    ArrayLike
        Column array with nulls filled.
    """
    arr = pick_first(table, [col], default_type=pa.float32())
    if arr.null_count == 0:
        return arr
    return pc.fill_null(arr, fill_value=default_value)


def table_from_schema(
    schema: SchemaLike,
    *,
    columns: Mapping[str, ArrayLike],
    num_rows: int,
) -> TableLike:
    """Build a table from a schema and a column mapping.

    Returns
    -------
    TableLike
        Table with missing columns filled as typed nulls.
    """
    arrays = [columns.get(field.name, pa.nulls(num_rows, type=field.type)) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def table_from_rows(
    schema: SchemaLike,
    rows: Iterable[Mapping[str, object]],
) -> TableLike:
    """Build a table from row mappings aligned to the provided schema.

    Returns
    -------
    TableLike
        Table built from row mappings.
    """
    row_list = [dict(row) for row in rows]
    arrays = [
        nested_array_factory(field, [row.get(field.name) for row in row_list]) for field in schema
    ]
    return pa.Table.from_arrays(arrays, schema=pa.schema(schema))


def table_from_arrays(
    schema: SchemaLike,
    *,
    columns: Mapping[str, ArrayLike],
    num_rows: int,
) -> TableLike:
    """Build a table from arrays aligned to the provided schema.

    Returns
    -------
    TableLike
        Table aligned to the schema with typed nulls for missing columns.
    """
    return table_from_schema(schema, columns=columns, num_rows=num_rows)


def empty_table(schema: SchemaLike) -> TableLike:
    """Return an empty table with the provided schema.

    Returns
    -------
    TableLike
        Empty table with the schema.
    """
    return pa.Table.from_arrays([pa.array([], type=field.type) for field in schema], schema=schema)


__all__ = [
    "column_or_null",
    "empty_table",
    "maybe_dictionary",
    "pick_first",
    "resolve_float_col",
    "resolve_string_col",
    "table_from_arrays",
    "table_from_rows",
    "table_from_schema",
]
