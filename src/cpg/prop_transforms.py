"""Property transform helpers for CPG specs."""

from __future__ import annotations

from datafusion import functions as f
from datafusion import lit
from datafusion.expr import Expr

from datafusion_engine.expr.cast import safe_cast
from datafusion_engine.udf.shims import utf8_normalize, utf8_null_if_blank


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


def expr_context_expr(expr: Expr) -> Expr:
    """Return a DataFusion expression for expr-context normalization.

    Returns
    -------
    datafusion.expr.Expr
        Expression for normalized context values.
    """
    normalized = utf8_null_if_blank(utf8_normalize(expr, collapse_ws=True))
    last = f.regexp_replace(normalized, lit(r".*\\."), lit(""))
    upper = f.upper(last)
    null_expr = safe_cast(lit(None), "Utf8")
    return f.when(normalized.is_null(), null_expr).otherwise(upper)


def flag_to_bool_expr(expr: Expr) -> Expr:
    """Return a DataFusion expression for optional boolean flags.

    Returns
    -------
    datafusion.expr.Expr
        Expression returning True or NULL.
    """
    casted = safe_cast(expr, "Int64")
    hit = casted == lit(1)
    null_bool = safe_cast(lit(None), "Boolean")
    return f.when(hit, lit(value=True)).otherwise(null_bool)


__all__ = [
    "expr_context_expr",
    "expr_context_value",
    "flag_to_bool",
    "flag_to_bool_expr",
]
