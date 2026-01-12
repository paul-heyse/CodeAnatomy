"""Scalar expression helpers for plan projections."""

from __future__ import annotations

from arrowdsl.core.interop import ComputeExpression, DataTypeLike, ensure_expression, pc


def null_expr(dtype: DataTypeLike) -> ComputeExpression:
    """Return a typed null expression.

    Returns
    -------
    ComputeExpression
        Expression producing nulls of the requested type.
    """
    return ensure_expression(pc.cast(pc.scalar(None), dtype, safe=False))


def scalar_expr(value: object, *, dtype: DataTypeLike) -> ComputeExpression:
    """Return a typed scalar expression.

    Returns
    -------
    ComputeExpression
        Expression producing the typed scalar value.
    """
    return ensure_expression(pc.cast(pc.scalar(value), dtype, safe=False))


__all__ = ["null_expr", "scalar_expr"]
