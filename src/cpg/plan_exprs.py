"""Shared plan-lane expression helpers for CPG builders."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.interop import ComputeExpression, DataTypeLike, ensure_expression, pc
from arrowdsl.plan_helpers import (
    coalesce_expr as plan_coalesce_expr,
)
from arrowdsl.plan_helpers import (
    column_or_null_expr as plan_column_or_null_expr,
)


def column_or_null_expr(
    name: str,
    dtype: DataTypeLike,
    *,
    available: set[str],
) -> ComputeExpression:
    """Return a typed field expression or a typed null when missing.

    Returns
    -------
    ComputeExpression
        Expression for the column or a typed null literal.
    """
    return plan_column_or_null_expr(name, dtype=dtype, available=available, cast=True, safe=False)


def coalesce_expr(
    cols: Sequence[str],
    *,
    dtype: DataTypeLike,
    available: set[str],
) -> ComputeExpression:
    """Return a coalesced expression over available columns.

    Returns
    -------
    ComputeExpression
        Coalesced expression or typed null when none are available.
    """
    return plan_coalesce_expr(cols, dtype=dtype, available=available, cast=True, safe=False)


def null_if_empty_or_zero(expr: ComputeExpression) -> ComputeExpression:
    """Return ``expr`` with empty/zero strings normalized to null.

    Returns
    -------
    ComputeExpression
        Expression with empty/zero strings mapped to null.
    """
    empty = ensure_expression(pc.equal(expr, pc.scalar("")))
    zero = ensure_expression(pc.equal(expr, pc.scalar("0")))
    return ensure_expression(
        pc.if_else(
            pc.or_(empty, zero),
            pc.cast(pc.scalar(None), pa.string(), safe=False),
            expr,
        )
    )


def zero_expr(values: ComputeExpression, *, dtype: DataTypeLike) -> ComputeExpression:
    """Return a boolean expression testing for zero values.

    Returns
    -------
    ComputeExpression
        Boolean expression indicating zero values.
    """
    if patypes.is_dictionary(dtype):
        values = ensure_expression(pc.cast(values, pa.string(), safe=False))
        dtype = pa.string()
    if patypes.is_string(dtype) or patypes.is_large_string(dtype):
        return ensure_expression(pc.equal(values, pc.scalar("0")))
    if patypes.is_integer(dtype):
        return ensure_expression(pc.equal(values, pa.scalar(0, type=dtype)))
    if patypes.is_floating(dtype):
        return ensure_expression(pc.equal(values, pa.scalar(0.0, type=dtype)))
    values = ensure_expression(pc.cast(values, pa.string(), safe=False))
    return ensure_expression(pc.equal(values, pc.scalar("0")))


def invalid_id_expr(values: ComputeExpression, *, dtype: DataTypeLike) -> ComputeExpression:
    """Return an expression for null-or-zero identifier checks.

    Returns
    -------
    ComputeExpression
        Expression identifying null or zero identifiers.
    """
    return ensure_expression(pc.or_(pc.is_null(values), zero_expr(values, dtype=dtype)))


def bitmask_is_set_expr(values: ComputeExpression, *, mask: int) -> ComputeExpression:
    """Return an expression indicating whether a bitmask flag is set.

    Returns
    -------
    ComputeExpression
        Expression indicating whether the mask bit is set.
    """
    roles = pc.cast(values, pa.int64(), safe=False)
    hit = pc.not_equal(pc.bit_wise_and(roles, pa.scalar(mask)), pa.scalar(0))
    return ensure_expression(pc.fill_null(hit, fill_value=False))


__all__ = [
    "bitmask_is_set_expr",
    "coalesce_expr",
    "column_or_null_expr",
    "invalid_id_expr",
    "null_if_empty_or_zero",
    "zero_expr",
]
