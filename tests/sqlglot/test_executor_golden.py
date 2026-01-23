"""Executor-backed golden tests for SQLGlot transform semantics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from sqlglot.executor import execute

from sqlglot_tools.compat import exp
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    default_sqlglot_policy,
    normalize_expr,
    parse_sql_strict,
)


def _execute_sql(
    sql: str,
    *,
    tables: Mapping[str, Sequence[Mapping[str, object]]],
) -> tuple[tuple[object, ...], ...]:
    result = execute(sql, tables={name: list(rows) for name, rows in tables.items()})
    return tuple(sorted(result.rows))


def _execute_expr(
    expr: exp.Expression,
    *,
    tables: Mapping[str, Sequence[Mapping[str, object]]],
) -> tuple[tuple[object, ...], ...]:
    result = execute(expr, tables={name: list(rows) for name, rows in tables.items()})
    return tuple(sorted(result.rows))


def test_pushdown_transform_semantics() -> None:
    """Ensure predicate/projection pushdown preserves results."""
    sql = "SELECT a FROM (SELECT a, b FROM t) AS sub WHERE b > 1"
    tables = {"t": [{"a": "x", "b": 0}, {"a": "y", "b": 2}, {"a": "z", "b": 3}]}
    policy = default_sqlglot_policy()
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    normalized = normalize_expr(
        expr,
        options=NormalizeExprOptions(
            schema={"t": {"a": "string", "b": "int"}},
            policy=policy,
            sql=sql,
        ),
    )
    assert _execute_sql(sql, tables=tables) == _execute_expr(normalized, tables=tables)


def test_recursive_cte_columns_added() -> None:
    """Ensure recursive CTEs receive column names after normalization."""
    sql = (
        "WITH RECURSIVE t AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM t WHERE x < 3) "
        "SELECT x FROM t"
    )
    policy = default_sqlglot_policy()
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    normalized = normalize_expr(
        expr,
        options=NormalizeExprOptions(
            schema=None,
            policy=policy,
            sql=sql,
        ),
    )
    cte = next(normalized.find_all(exp.CTE))
    alias = cte.args.get("alias")
    assert alias is not None
    assert getattr(alias, "columns", None)
