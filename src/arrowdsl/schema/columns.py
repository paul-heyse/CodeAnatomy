"""Canonical column helpers for Arrow tables."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa

from arrowdsl.core.interop import ArrayLike, DataTypeLike, SchemaLike, TableLike


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
    if col in table.column_names:
        return table[col]
    return pa.nulls(table.num_rows, type=dtype)


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


__all__ = ["column_or_null", "table_from_schema"]
