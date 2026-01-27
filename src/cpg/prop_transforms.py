"""Property transform helpers for CPG specs."""

from __future__ import annotations

from datafusion import functions as f
from datafusion import lit
from datafusion.expr import Expr


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
    text = f.trim(f.arrow_cast(expr, lit("Utf8")))
    last = f.regexp_replace(text, lit(r".*\\."), lit(""))
    upper = f.upper(last)
    empty = f.length(upper) == lit(0)
    return f.when(empty, f.arrow_cast(lit(None), lit("Utf8"))).otherwise(upper)


def flag_to_bool_expr(expr: Expr) -> Expr:
    """Return a DataFusion expression for optional boolean flags.

    Returns
    -------
    datafusion.expr.Expr
        Expression returning True or NULL.
    """
    casted = f.arrow_cast(expr, lit("Int64"))
    hit = casted == lit(1)
    null_bool = f.arrow_cast(lit(None), lit("Boolean"))
    return f.when(hit, lit(True)).otherwise(null_bool)


__all__ = [
    "expr_context_expr",
    "expr_context_value",
    "flag_to_bool",
    "flag_to_bool_expr",
]
