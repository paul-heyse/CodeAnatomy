"""Shared expression helpers for semantic catalog analysis builders."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from datafusion import col, lit
from datafusion import functions as f

from datafusion_engine.expr.cast import safe_cast
from datafusion_engine.udf.expr import udf_expr

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame
    from datafusion.expr import Expr


def _arrow_cast(expr: Expr, data_type: str) -> Expr:
    return safe_cast(expr, data_type)


def _null_expr(data_type: str) -> Expr:
    return _arrow_cast(lit(None), data_type)


def _coalesce_cols(df: DataFrame, *col_names: str, default_expr: Expr | None = None) -> Expr:
    exprs = [col(name) for name in col_names if name in df.schema().names]
    if not exprs:
        return default_expr if default_expr is not None else _null_expr("Utf8")
    result = exprs[0]
    for expr in exprs[1:]:
        result = f.coalesce(result, expr)
    if default_expr is not None:
        result = f.coalesce(result, default_expr)
    return result


def _hash_part(expr: Expr, *, null_sentinel: str) -> Expr:
    return f.coalesce(_arrow_cast(expr, "Utf8"), lit(null_sentinel))


def _normalized_text(expr: Expr) -> Expr:
    return udf_expr("utf8_normalize", expr, collapse_ws=True)


def _stable_id_expr(prefix: str, parts: Sequence[Expr], *, null_sentinel: str) -> Expr:
    if not parts:
        msg = "stable identifiers require at least one part."
        raise ValueError(msg)
    normalized = [_hash_part(part, null_sentinel=null_sentinel) for part in parts]
    return udf_expr("stable_id_parts", prefix, normalized[0], *normalized[1:])


def _span_expr(
    *,
    bstart: Expr,
    bend: Expr,
    col_unit: Expr | None = None,
) -> Expr:
    _ = col_unit
    return udf_expr("span_make", bstart, bend)


__all__ = [
    "_arrow_cast",
    "_coalesce_cols",
    "_normalized_text",
    "_null_expr",
    "_span_expr",
    "_stable_id_expr",
]
