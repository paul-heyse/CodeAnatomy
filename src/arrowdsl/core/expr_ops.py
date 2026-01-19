"""Compute expression helpers for Arrow-backed plans."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.interop import ComputeExpression, DataTypeLike, ensure_expression, pc


def cast_expr(expr: ComputeExpression, dtype: DataTypeLike, *, safe: bool = True) -> ComputeExpression:
    """Return a cast expression for the requested dtype.

    Returns
    -------
    ComputeExpression
        Cast expression for the provided dtype.
    """
    return ensure_expression(pc.cast(expr, dtype, safe=safe))


def and_expr(left: ComputeExpression, right: ComputeExpression) -> ComputeExpression:
    """Return a logical AND expression.

    Returns
    -------
    ComputeExpression
        Expression computing the logical AND.
    """
    return ensure_expression(left & right)


def or_expr(left: ComputeExpression, right: ComputeExpression) -> ComputeExpression:
    """Return a logical OR expression.

    Returns
    -------
    ComputeExpression
        Expression computing the logical OR.
    """
    return ensure_expression(left | right)


def and_exprs(exprs: Sequence[ComputeExpression]) -> ComputeExpression:
    """Return a logical AND over a sequence of expressions.

    Returns
    -------
    ComputeExpression
        Combined logical AND expression.

    Raises
    ------
    ValueError
        Raised when no expressions are provided.
    """
    if not exprs:
        msg = "and_exprs requires at least one expression."
        raise ValueError(msg)
    combined = exprs[0]
    for expr in exprs[1:]:
        combined = ensure_expression(combined & expr)
    return combined


def or_exprs(exprs: Sequence[ComputeExpression]) -> ComputeExpression:
    """Return a logical OR over a sequence of expressions.

    Returns
    -------
    ComputeExpression
        Combined logical OR expression.

    Raises
    ------
    ValueError
        Raised when no expressions are provided.
    """
    if not exprs:
        msg = "or_exprs requires at least one expression."
        raise ValueError(msg)
    combined = exprs[0]
    for expr in exprs[1:]:
        combined = ensure_expression(combined | expr)
    return combined


__all__ = [
    "and_expr",
    "and_exprs",
    "cast_expr",
    "or_expr",
    "or_exprs",
]
