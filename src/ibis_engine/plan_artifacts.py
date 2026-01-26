"""Ibis diagnostics helpers for plan artifacts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import cast

import ibis
from ibis.expr.types import Expr, Value


def _hash_sql(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def _safe_decompile_expr(expr: Expr) -> str:
    try:
        return ibis.decompile(expr)
    except (AttributeError, NotImplementedError, RuntimeError, TypeError, ValueError) as exc:
        return f"ERROR: {exc}"


def _safe_sql(expr: Expr, *, dialect: str | None, pretty: bool) -> str:
    try:
        return expr.to_sql(dialect=dialect, pretty=pretty)
    except (NotImplementedError, RuntimeError, TypeError, ValueError) as exc:
        return f"ERROR: {exc}"


def _compile_params_for_expr(
    params: Mapping[Value, object] | Mapping[str, object] | None,
) -> Mapping[Value, object] | None:
    if not params:
        return None
    if all(isinstance(key, Value) for key in params):
        return cast("Mapping[Value, object]", params)
    return None


def _safe_compile_expr(
    expr: Expr,
    *,
    params: Mapping[Value, object] | Mapping[str, object] | None,
    limit: int | None,
) -> tuple[str | None, str | None]:
    compile_params = _compile_params_for_expr(params)
    try:
        compiled_value = expr.compile(params=compile_params, limit=limit)
    except (AttributeError, NotImplementedError, RuntimeError, TypeError, ValueError) as exc:
        return f"ERROR: {exc}", None
    if compiled_value is None:
        return None, None
    if not isinstance(compiled_value, str):
        compiled_value = str(compiled_value)
    if compiled_value.startswith("ERROR: "):
        return compiled_value, None
    return compiled_value, _hash_sql(compiled_value)


def _compile_params_payload(
    params: Mapping[Value, object] | Mapping[str, object] | None,
) -> str | None:
    if not params:
        return None
    payload = {str(key): value for key, value in params.items()}
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str)


def _compile_limit(limit: int | None) -> int | None:
    if limit is None or isinstance(limit, bool):
        return None
    return limit


def ibis_plan_artifacts(
    expr: Expr,
    *,
    dialect: str | None = None,
    params: Mapping[Value, object] | Mapping[str, object] | None = None,
    limit: int | None = None,
) -> dict[str, object]:
    """Return Ibis-level artifacts for diagnostics.

    Returns
    -------
    dict[str, object]
        Ibis decompile, SQL, compile, and optional graphviz artifacts.
    """
    decompile = _safe_decompile_expr(expr)
    sql = _safe_sql(expr, dialect=dialect, pretty=False)
    sql_pretty = _safe_sql(expr, dialect=dialect, pretty=True)
    compiled_sql, compiled_hash = _safe_compile_expr(expr, params=params, limit=limit)
    compile_params_payload = _compile_params_payload(params)
    compile_limit = _compile_limit(limit)
    graphviz: str | None = None
    try:
        from ibis.expr.visualize import to_graph
    except (ImportError, ModuleNotFoundError):
        graphviz = None
    else:
        try:
            graph = to_graph(expr)
            graphviz = graph.source if hasattr(graph, "source") else str(graph)
        except (NotImplementedError, RuntimeError, TypeError, ValueError) as exc:
            graphviz = f"ERROR: {exc}"
    return {
        "ibis_decompile": decompile,
        "ibis_sql": sql,
        "ibis_sql_pretty": sql_pretty,
        "ibis_graphviz": graphviz,
        "ibis_compiled_sql": compiled_sql,
        "ibis_compiled_sql_hash": compiled_hash,
        "ibis_compile_params": compile_params_payload,
        "ibis_compile_limit": compile_limit,
    }


__all__ = ["ibis_plan_artifacts"]
