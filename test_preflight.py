"""Simple test for preflight_sql implementation."""

from sqlglot_tools.optimizer import (
    default_sqlglot_policy,
    emit_preflight_diagnostics,
    preflight_sql,
    register_datafusion_dialect,
)

register_datafusion_dialect()


def test_preflight_sql_success() -> None:
    """Test successful preflight."""
    sql = "SELECT a, b FROM t WHERE a > 1"
    schema = {"t": {"a": "int", "b": "int"}}
    policy = default_sqlglot_policy()

    result = preflight_sql(
        sql,
        schema=schema,
        dialect=policy.read_dialect,
        strict=True,
        policy=policy,
    )

    assert len(result.errors) == 0
    assert result.qualified_expr is not None
    assert result.annotated_expr is not None
    assert result.canonicalized_expr is not None
    assert result.policy_hash is not None
    assert result.schema_map_hash is not None


def test_preflight_sql_parse_failure() -> None:
    """Test preflight with parse error."""
    sql = "SELECT FROM"  # Invalid SQL
    policy = default_sqlglot_policy()

    result = preflight_sql(
        sql,
        schema=None,
        dialect=policy.read_dialect,
        strict=True,
        policy=policy,
    )

    assert len(result.errors) > 0
    assert result.qualified_expr is None


def test_preflight_sql_qualification_failure() -> None:
    """Test preflight with qualification error."""
    sql = "SELECT missing FROM t"  # Column not in schema
    schema = {"t": {"a": "int"}}
    policy = default_sqlglot_policy()

    result = preflight_sql(
        sql,
        schema=schema,
        dialect=policy.read_dialect,
        strict=True,
        policy=policy,
    )

    assert len(result.errors) > 0
    assert result.qualified_expr is None


def test_emit_preflight_diagnostics() -> None:
    """Test diagnostic emission."""
    sql = "SELECT a FROM t"
    schema = {"t": {"a": "int"}}
    policy = default_sqlglot_policy()

    result = preflight_sql(
        sql,
        schema=schema,
        dialect=policy.read_dialect,
        strict=True,
        policy=policy,
    )

    diagnostics = emit_preflight_diagnostics(result)

    assert diagnostics["original_sql"] == sql
    assert diagnostics["policy_hash"] is not None
    assert diagnostics["schema_map_hash"] is not None
    assert diagnostics["has_errors"] is False
    assert diagnostics["stages_completed"]["qualified"] is True
    assert diagnostics["stages_completed"]["annotated"] is True
    assert diagnostics["stages_completed"]["canonicalized"] is True


def test_preflight_lenient_mode() -> None:
    """Test lenient mode continues on errors."""
    sql = "SELECT missing FROM t"
    schema = {"t": {"a": "int"}}
    policy = default_sqlglot_policy()

    result = preflight_sql(
        sql,
        schema=schema,
        dialect=policy.read_dialect,
        strict=False,
        policy=policy,
    )

    # In lenient mode, errors become warnings
    assert len(result.warnings) > 0 or len(result.errors) > 0
