"""Unit tests for SQLGlot executor rewrites."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from sqlglot.executor import execute
from sqlglot.transforms import eliminate_full_outer_join, eliminate_semi_and_anti_joins

from sqlglot_tools.compat import Expression, parse_one


def _execute(
    expr: Expression | str,
    tables: Mapping[str, Sequence[Mapping[str, object]]],
) -> tuple[tuple[tuple[object, ...], ...], tuple[str, ...]]:
    result = execute(expr, tables={name: list(rows) for name, rows in tables.items()})
    return tuple(result.rows), tuple(result.columns)


def test_full_outer_join_rewrite_equivalence() -> None:
    """Ensure full outer join elimination preserves results."""
    sql = "SELECT * FROM left_table FULL OUTER JOIN right_table USING(id)"
    tables = {
        "left_table": [{"id": 1, "l": "a"}, {"id": 2, "l": "b"}],
        "right_table": [{"id": 1, "r": "c"}],
    }
    expr = parse_one(sql)
    rewritten = eliminate_full_outer_join(expr.copy())
    assert _execute(sql, tables) == _execute(rewritten, tables)


def test_semi_join_rewrite_equivalence() -> None:
    """Ensure semi join elimination preserves results."""
    sql = "SELECT * FROM left_table SEMI JOIN right_table USING(id)"
    tables = {
        "left_table": [{"id": 1, "l": "a"}, {"id": 2, "l": "b"}],
        "right_table": [{"id": 1, "r": "c"}],
    }
    expr = parse_one(sql)
    rewritten = eliminate_semi_and_anti_joins(expr.copy())
    assert _execute(sql, tables) == _execute(rewritten, tables)
