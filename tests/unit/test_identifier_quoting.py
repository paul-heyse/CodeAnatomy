"""Tests for identifier quoting policy."""

from __future__ import annotations

from sqlglot_tools.optimizer import NormalizeExprOptions, normalize_expr, parse_sql_strict


def test_mixed_case_schema_forces_quoted_identifiers() -> None:
    """Quote identifiers when schema contains mixed-case names."""
    schema_map = {"Events": {"UserID": "int", "region": "string"}}
    expr = parse_sql_strict("SELECT UserID, region FROM Events", dialect="datafusion")
    normalized = normalize_expr(expr, options=NormalizeExprOptions(schema=schema_map))
    sql = normalized.sql(dialect="datafusion")
    assert '"UserID"' in sql
    assert '"Events"' in sql
