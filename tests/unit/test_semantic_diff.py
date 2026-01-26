"""Unit tests for semantic diff classification."""

from __future__ import annotations

from datafusion_engine.semantic_diff import ChangeCategory, SemanticDiff
from sqlglot_tools.optimizer import parse_sql_strict, register_datafusion_dialect


def _category(old_sql: str, new_sql: str) -> ChangeCategory:
    register_datafusion_dialect()
    old_ast = parse_sql_strict(old_sql, dialect="datafusion")
    new_ast = parse_sql_strict(new_sql, dialect="datafusion")
    diff = SemanticDiff.compute(old_ast, new_ast)
    return diff.overall_category


def test_semantic_diff_join_row_multiplying() -> None:
    """Joins are classified as row-multiplying changes."""
    old_sql = "SELECT * FROM left_table"
    new_sql = "SELECT * FROM left_table JOIN right_table ON left_table.id = right_table.id"
    assert _category(old_sql, new_sql) == ChangeCategory.ROW_MULTIPLYING


def test_semantic_diff_unnest_row_multiplying() -> None:
    """Unnest operations are classified as row-multiplying changes."""
    old_sql = "SELECT id FROM events"
    new_sql = "SELECT events.id, item FROM events CROSS JOIN UNNEST(events.items) AS u(item)"
    assert _category(old_sql, new_sql) == ChangeCategory.ROW_MULTIPLYING


def test_semantic_diff_window_is_breaking() -> None:
    """Window changes are classified as breaking changes."""
    old_sql = "SELECT id, ROW_NUMBER() OVER (PARTITION BY id) AS rn FROM events"
    new_sql = "SELECT id, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts) AS rn FROM events"
    assert _category(old_sql, new_sql) == ChangeCategory.BREAKING
