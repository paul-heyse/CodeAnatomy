"""Column helpers for Arrow tables."""

from __future__ import annotations

from collections.abc import Sequence

import arrowdsl.pyarrow_core as pa
from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import DataTypeLike, TableLike


def add_const_column(
    table: TableLike,
    name: str,
    value: object,
    *,
    data_type: DataTypeLike | None = None,
) -> TableLike:
    """Append a constant-valued column if missing.

    Parameters
    ----------
    table:
        Input table.
    name:
        Column name to add.
    value:
        Constant value to repeat.
    data_type:
        Optional Arrow type override.

    Returns
    -------
    pyarrow.Table
        Table with the constant column appended.
    """
    if name in table.column_names:
        return table
    scalar = pa.scalar(value) if data_type is None else pa.scalar(value, type=data_type)
    arr = pa.array([value] * table.num_rows, type=scalar.type)
    return table.append_column(name, arr)


def coalesce_string(table: TableLike, cols: Sequence[str], *, out_col: str) -> TableLike:
    """Coalesce multiple columns into a single string column.

    Parameters
    ----------
    table:
        Input table.
    cols:
        Candidate columns.
    out_col:
        Output column name.

    Returns
    -------
    pyarrow.Table
        Table with the coalesced column appended.
    """
    if out_col in table.column_names:
        return table
    expr = pc.cast(table[cols[0]], pa.string())
    for col in cols[1:]:
        expr = pc.coalesce(expr, pc.cast(table[col], pa.string()))
    return table.append_column(out_col, expr)


def select_columns(table: TableLike, cols: Sequence[str]) -> TableLike:
    """Select the subset of columns that exist.

    Parameters
    ----------
    table:
        Input table.
    cols:
        Column names to select.

    Returns
    -------
    pyarrow.Table
        Table containing only available columns.
    """
    keep = [col for col in cols if col in table.column_names]
    return table.select(keep) if keep else table
