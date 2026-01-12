"""Column helpers for Arrow tables."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.column_ops import CoalesceExpr, ConstExpr, FieldExpr
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
    expr = ConstExpr(value=value, dtype=data_type)
    return table.append_column(name, expr.materialize(table))


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
    exprs = tuple(FieldExpr(name=col) for col in cols)
    expr = CoalesceExpr(exprs=exprs).materialize(table)
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
