"""Compute transforms used across plan and kernel lanes."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.compute.udfs import ensure_expr_context_udf
from arrowdsl.core.interop import ComputeExpression, ensure_expression, pc


def expr_context_value(value: object) -> str | None:
    """Normalize expression context strings.

    Returns
    -------
    str | None
        Normalized context value or ``None``.
    """
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if "." in raw:
        raw = raw.rsplit(".", 1)[-1]
    return raw.upper()


def flag_to_bool(value: object | None) -> bool | None:
    """Normalize integer/bool flags to optional bools.

    Returns
    -------
    bool | None
        Normalized boolean flag or ``None``.
    """
    if isinstance(value, bool):
        return True if value else None
    if isinstance(value, int):
        return True if value == 1 else None
    return None


def expr_context_expr(expr: ComputeExpression) -> ComputeExpression:
    """Return a compute expression for expr-context normalization.

    Returns
    -------
    ComputeExpression
        Expression normalizing expr-context values.
    """
    func_name = ensure_expr_context_udf()
    return ensure_expression(pc.call_function(func_name, [expr]))


def flag_to_bool_expr(expr: ComputeExpression) -> ComputeExpression:
    """Return a compute expression for optional boolean flags.

    Returns
    -------
    ComputeExpression
        Expression normalizing optional boolean flags.
    """
    casted = pc.cast(expr, pa.int64(), safe=False)
    hit = pc.equal(casted, pa.scalar(1))
    return ensure_expression(
        pc.if_else(
            hit,
            pc.cast(pc.scalar(1), pa.bool_(), safe=False),
            pc.cast(pc.scalar(None), pa.bool_(), safe=False),
        )
    )


__all__ = [
    "expr_context_expr",
    "expr_context_value",
    "flag_to_bool",
    "flag_to_bool_expr",
]
