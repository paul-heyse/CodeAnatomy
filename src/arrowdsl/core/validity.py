"""Validity mask helpers for Arrow tables and expressions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import pyarrow as pa

from arrowdsl.core.expr_ops import and_expr
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    TableLike,
    ensure_expression,
    pc,
)


def _false_mask(num_rows: int) -> ArrayLike:
    """Return a boolean mask of all False values.

    Returns
    -------
    ArrayLike
        Boolean mask with all values set to ``False``.
    """
    return pc.is_valid(pa.nulls(num_rows, type=pa.bool_()))


def valid_mask_array(
    values: Sequence[ArrayLike | ChunkedArrayLike],
) -> ArrayLike | ChunkedArrayLike:
    """Return a validity mask for a sequence of arrays.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Boolean mask where all inputs are valid.

    Raises
    ------
    ValueError
        Raised when no arrays are provided.
    """
    if not values:
        msg = "valid_mask_array requires at least one array."
        raise ValueError(msg)
    mask = pc.is_valid(values[0])
    for value in values[1:]:
        mask = pc.and_(mask, pc.is_valid(value))
    return mask


def valid_mask_for_columns(table: TableLike, cols: Sequence[str]) -> ArrayLike | ChunkedArrayLike:
    """Return a validity mask for columns, treating missing columns as invalid.

    Returns
    -------
    ArrayLike | ChunkedArrayLike
        Boolean mask where all referenced columns are valid.

    Raises
    ------
    ValueError
        Raised when no column names are provided.
    """
    if not cols:
        msg = "valid_mask_for_columns requires at least one column."
        raise ValueError(msg)
    mask: ArrayLike | ChunkedArrayLike | None = None
    for name in cols:
        if name in table.column_names:
            next_mask = pc.is_valid(table[name])
        else:
            next_mask = _false_mask(table.num_rows)
        mask = next_mask if mask is None else pc.and_(mask, next_mask)
    return mask if mask is not None else _false_mask(table.num_rows)


def valid_mask_expr(
    cols: Sequence[str],
    *,
    available: Sequence[str] | None = None,
) -> ComputeExpression:
    """Return a validity mask expression for the provided columns.

    Returns
    -------
    ComputeExpression
        Boolean expression marking rows with all columns valid.

    Raises
    ------
    ValueError
        Raised when no column names are provided.
    """
    if not cols:
        msg = "valid_mask_expr requires at least one column."
        raise ValueError(msg)

    def _expr_for(name: str) -> ComputeExpression:
        if available is not None and name not in available:
            return ensure_expression(pc.scalar(pa.scalar(value=False)))
        return ensure_expression(cast("ComputeExpression", pc.is_valid(pc.field(name))))

    mask = _expr_for(cols[0])
    for name in cols[1:]:
        mask = and_expr(mask, _expr_for(name))
    return mask


__all__ = [
    "valid_mask_array",
    "valid_mask_expr",
    "valid_mask_for_columns",
]
