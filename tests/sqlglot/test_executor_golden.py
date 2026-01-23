"""Executor-backed golden tests for SQLGlot transform semantics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import NamedTuple

from sqlglot.errors import ExecuteError
from sqlglot.executor import execute

from sqlglot_tools.compat import Expression, exp
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
    expr: Expression,
    *,
    tables: Mapping[str, Sequence[Mapping[str, object]]],
) -> tuple[tuple[object, ...], ...]:
    result = execute(expr, tables={name: list(rows) for name, rows in tables.items()})
    return tuple(sorted(result.rows))


class ExecuteOutcome(NamedTuple):
    """Captured outcome from SQLGlot executor runs."""

    ok: bool
    rows: tuple[tuple[object, ...], ...] | None
    error: str | None


def _execute_outcome(
    statement: str | Expression,
    *,
    tables: Mapping[str, Sequence[Mapping[str, object]]],
) -> ExecuteOutcome:
    try:
        result = execute(statement, tables={name: list(rows) for name, rows in tables.items()})
    except ExecuteError as exc:
        return ExecuteOutcome(ok=False, rows=None, error=exc.__class__.__name__)
    return ExecuteOutcome(ok=True, rows=tuple(sorted(result.rows)), error=None)


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


def test_join_normalization_semantics() -> None:
    """Ensure join rewrites preserve executor semantics."""
    sql = "SELECT t.a, u.c FROM t JOIN u ON t.b = u.b WHERE u.c > 1"
    tables = {
        "t": [{"a": "x", "b": 1}, {"a": "y", "b": 2}],
        "u": [{"b": 1, "c": 0}, {"b": 2, "c": 3}],
    }
    policy = default_sqlglot_policy()
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    normalized = normalize_expr(
        expr,
        options=NormalizeExprOptions(
            schema={"t": {"a": "string", "b": "int"}, "u": {"b": "int", "c": "int"}},
            policy=policy,
            sql=sql,
        ),
    )
    assert _execute_sql(sql, tables=tables) == _execute_expr(normalized, tables=tables)


def test_aggregate_normalization_semantics() -> None:
    """Ensure aggregate + HAVING rewrites preserve executor semantics."""
    sql = "SELECT a, SUM(b) AS total FROM t GROUP BY a HAVING SUM(b) > 1"
    tables = {"t": [{"a": "x", "b": 1}, {"a": "x", "b": 2}, {"a": "y", "b": 1}]}
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


def test_window_function_normalization_equivalence() -> None:
    """Ensure window functions behave equivalently under executor support."""
    sql = "SELECT a, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS rn FROM t"
    tables = {"t": [{"a": "x", "b": 1}, {"a": "x", "b": 2}, {"a": "y", "b": 1}]}
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
    assert _execute_outcome(sql, tables=tables) == _execute_outcome(
        normalized,
        tables=tables,
    )


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


def test_recursive_cte_executor_equivalence() -> None:
    """Ensure recursive CTEs execute equivalently under executor support."""
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
    assert _execute_outcome(sql, tables={}) == _execute_outcome(normalized, tables={})
