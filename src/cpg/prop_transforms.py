"""Property transform helpers for CPG specs."""

from __future__ import annotations

import ibis
import pyarrow as pa
from ibis.expr.types import StringValue, Value

from ibis_engine.schema_utils import ibis_null_literal


def expr_context_value(value: object) -> str | None:
    """Normalize expression context strings.

    Returns
    -------
    str | None
        Normalized context value when valid.
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
        True when flagged, otherwise None.
    """
    if isinstance(value, bool):
        return True if value else None
    if isinstance(value, int):
        return True if value == 1 else None
    return None


def expr_context_expr(expr: Value) -> Value:
    """Return an Ibis expression for expr-context normalization.

    Returns
    -------
    ibis.expr.types.Value
        Ibis expression for normalized context values.
    """
    text: StringValue = expr.cast("string").strip()
    parts = text.split(".")
    last = parts[-1].cast("string")
    upper = last.upper()
    empty = upper.length() == 0
    return ibis.ifelse(empty, ibis_null_literal(pa.string()), upper)


def flag_to_bool_expr(expr: Value) -> Value:
    """Return an Ibis expression for optional boolean flags.

    Returns
    -------
    ibis.expr.types.Value
        Ibis expression returning True or NULL.
    """
    casted = expr.cast("int64")
    hit = casted == ibis.literal(1)
    return ibis.ifelse(hit, ibis.literal(value=True), ibis_null_literal(pa.bool_()))


__all__ = [
    "expr_context_expr",
    "expr_context_value",
    "flag_to_bool",
    "flag_to_bool_expr",
]
