"""SQLGlot policy normalization and diff tests."""

from __future__ import annotations

import pytest
from sqlglot import exp, parse_one

from ibis_engine.plan_diff import semantic_diff_sql
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    canonical_ast_fingerprint,
    default_sqlglot_policy,
    normalize_expr,
    normalize_expr_with_stats,
    plan_fingerprint,
    sqlglot_policy_snapshot,
)


def _column_tables(expr: exp.Expression) -> tuple[str | None, ...]:
    """Return table qualifiers for columns in an expression.

    Returns
    -------
    tuple[str | None, ...]
        Table qualifiers for each column expression.
    """
    return tuple(column.table for column in expr.find_all(exp.Column))


def test_sqlglot_policy_snapshot_payload() -> None:
    """Capture the default SQLGlot policy snapshot."""
    snapshot = sqlglot_policy_snapshot()
    payload = snapshot.payload()
    assert payload["read_dialect"] == snapshot.read_dialect
    assert payload["write_dialect"] == snapshot.write_dialect
    assert payload["policy_hash"] == snapshot.policy_hash


def test_normalize_expr_qualifies_columns() -> None:
    """Normalize a SQLGlot expression with qualification."""
    policy = default_sqlglot_policy()
    expr = parse_one("SELECT a FROM t", dialect=policy.read_dialect)
    schema = {"t": {"a": "int"}}
    normalized = normalize_expr(
        expr,
        options=NormalizeExprOptions(
            schema=schema,
            policy=policy,
            sql="SELECT a FROM t",
        ),
    )
    tables = _column_tables(normalized)
    assert tables
    assert all(table is not None for table in tables)


def test_normalize_expr_reports_stats() -> None:
    """Record predicate normalization stats during normalization."""
    policy = default_sqlglot_policy()
    expr = parse_one("SELECT a FROM t WHERE a > 1", dialect=policy.read_dialect)
    result = normalize_expr_with_stats(
        expr,
        options=NormalizeExprOptions(
            schema={"t": {"a": "int"}},
            policy=policy,
        ),
    )
    assert result.stats.max_distance == policy.normalization_distance
    assert isinstance(result.stats.distance, int)


@pytest.mark.parametrize(
    ("dialect_left", "dialect_right"),
    [
        ("datafusion", "duckdb"),
        ("datafusion", "datafusion_ext"),
    ],
)
def test_plan_fingerprint_depends_on_dialect(dialect_left: str, dialect_right: str) -> None:
    """Include dialect in plan fingerprints."""
    expr = parse_one("SELECT 1", dialect=dialect_left)
    left = plan_fingerprint(expr, dialect=dialect_left)
    right = plan_fingerprint(expr, dialect=dialect_right)
    assert left != right


@pytest.mark.parametrize(
    ("left_sql", "right_sql", "expected_breaking"),
    [
        ("SELECT a FROM t", "SELECT a, b FROM t", "nonbreaking"),
        ("SELECT a, b FROM t", "SELECT a FROM t", "breaking"),
    ],
)
def test_semantic_diff_breaking_flag(left_sql: str, right_sql: str, expected_breaking: str) -> None:
    """Flag breaking changes from semantic diffs."""
    policy = default_sqlglot_policy()
    diff = semantic_diff_sql(
        left_sql,
        right_sql,
        policy=policy,
        dialect=policy.read_dialect,
    )
    assert diff.changed is True
    assert diff.breaking is (expected_breaking == "breaking")


def test_canonical_ast_fingerprint_is_stable() -> None:
    """Fingerprint a canonical SQLGlot AST payload."""
    expr = parse_one("SELECT a FROM t", dialect="datafusion")
    fingerprint = canonical_ast_fingerprint(expr)
    assert fingerprint == canonical_ast_fingerprint(expr)
