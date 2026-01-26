"""Tests for identifier quoting policy."""

from __future__ import annotations

from datafusion_engine.sql_policy_engine import SQLPolicyProfile, compile_sql_policy
from sqlglot_tools.optimizer import default_sqlglot_policy, parse_sql_strict


def test_mixed_case_schema_forces_quoted_identifiers() -> None:
    """Quote identifiers when schema contains mixed-case names."""
    schema_map = {"Events": {"UserID": "int", "region": "string"}}
    policy = default_sqlglot_policy()
    expr = parse_sql_strict("SELECT UserID, region FROM Events", dialect=policy.read_dialect)
    normalized, _ = compile_sql_policy(
        expr,
        schema=schema_map,
        profile=SQLPolicyProfile(
            policy=policy,
            read_dialect=policy.read_dialect,
            write_dialect=policy.write_dialect,
        ),
    )
    sql = normalized.sql(dialect=policy.write_dialect)
    assert '"UserID"' in sql
    assert '"Events"' in sql
