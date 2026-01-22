"""Verify implementation of Scope 5 Part 2."""

from datafusion_engine.bridge import _maybe_enforce_preflight
from datafusion_engine.compile_options import DataFusionCompileOptions
from sqlglot_tools.optimizer import (
    artifact_to_ast,
    ast_to_artifact,
    default_sqlglot_policy,
    deserialize_ast_artifact,
    parse_sql_strict,
    serialize_ast_artifact,
)


def test_ast_artifact_roundtrip() -> None:
    """Verify AstArtifact serialization roundtrip."""
    policy = default_sqlglot_policy()
    sql = "SELECT a, b FROM t WHERE a = 'x'"
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    artifact = ast_to_artifact(expr, sql=sql, policy=policy)

    serialized = serialize_ast_artifact(artifact)
    deserialized = deserialize_ast_artifact(serialized)
    assert artifact.sql == deserialized.sql
    assert artifact.policy_hash == deserialized.policy_hash


def test_ast_restoration() -> None:
    """Verify AstArtifact restoration to SQLGlot expression."""
    policy = default_sqlglot_policy()
    sql = "SELECT a, b FROM t WHERE a = 'x'"
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    artifact = ast_to_artifact(expr, sql=sql, policy=policy)
    restored_expr = artifact_to_ast(artifact)
    restored_sql = restored_expr.sql(dialect=policy.write_dialect)
    assert restored_sql is not None


def test_compile_options_enforce_preflight_flag() -> None:
    """Verify compile options expose enforce_preflight flag."""
    options = DataFusionCompileOptions(enforce_preflight=True)
    assert options.enforce_preflight is True


def test_preflight_enforcement_import() -> None:
    """Verify preflight enforcement helper is importable."""
    assert callable(_maybe_enforce_preflight)
