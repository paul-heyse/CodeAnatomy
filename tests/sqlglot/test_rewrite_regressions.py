"""Regression tests for SQLGlot rewrite semantics using executor-based validation."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence

from sqlglot.executor import execute

from sqlglot_tools.compat import Expression
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    artifact_to_ast,
    ast_to_artifact,
    default_sqlglot_policy,
    deserialize_ast_artifact,
    normalize_expr,
    parse_sql_strict,
    serialize_ast_artifact,
)


def _execute_sql(
    sql: str,
    *,
    tables: Mapping[str, Sequence[Mapping[str, object]]],
) -> tuple[tuple[object, ...], ...]:
    """Execute SQL using SQLGlot executor and return sorted rows.

    Parameters
    ----------
    sql
        SQL query to execute.
    tables
        Table data as mapping of table names to row dictionaries.

    Returns
    -------
    tuple[tuple[object, ...], ...]
        Sorted result rows.
    """
    result = execute(sql, tables={name: list(rows) for name, rows in tables.items()})
    return tuple(sorted(result.rows))


def _execute_expr(
    expr: Expression,
    *,
    tables: Mapping[str, Sequence[Mapping[str, object]]],
) -> tuple[tuple[object, ...], ...]:
    """Execute SQLGlot expression using executor and return sorted rows.

    Parameters
    ----------
    expr
        SQLGlot expression to execute.
    tables
        Table data as mapping of table names to row dictionaries.

    Returns
    -------
    tuple[tuple[object, ...], ...]
        Sorted result rows.
    """
    result = execute(expr, tables={name: list(rows) for name, rows in tables.items()})
    return tuple(sorted(result.rows))


def test_rewrite_semantics_basic() -> None:
    """Ensure basic SQL execution produces expected results."""
    tables = {"t": [{"a": "x", "b": 1}, {"a": "y", "b": 2}]}
    result = _execute_sql("SELECT a, SUM(b) AS s FROM t GROUP BY a", tables=tables)
    assert result == (("x", 1), ("y", 2))


def test_rewrite_semantics_with_normalization() -> None:
    """Ensure normalization preserves query semantics."""
    sql = "SELECT a, SUM(b) AS s FROM t WHERE b > 0 GROUP BY a"
    tables = {"t": [{"a": "x", "b": 1}, {"a": "y", "b": 2}, {"a": "z", "b": 0}]}
    policy = default_sqlglot_policy()
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    normalized = normalize_expr(
        expr,
        options=NormalizeExprOptions(
            schema=None,
            rules=None,
            rewrite_hook=None,
            enable_rewrites=True,
            policy=policy,
            sql=sql,
        ),
    )
    original_result = _execute_sql(sql, tables=tables)
    normalized_result = _execute_expr(normalized, tables=tables)
    assert original_result == normalized_result
    assert normalized_result == (("x", 1), ("y", 2))


def test_ast_artifact_roundtrip() -> None:
    """Ensure AstArtifact serialization roundtrip preserves AST structure."""
    sql = "SELECT a, b FROM t WHERE a = 'x'"
    policy = default_sqlglot_policy()
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    artifact = ast_to_artifact(expr, sql=sql, policy=policy)
    serialized = serialize_ast_artifact(artifact)
    deserialized = deserialize_ast_artifact(serialized)
    restored_expr = artifact_to_ast(deserialized)
    assert artifact.sql == deserialized.sql
    assert artifact.policy_hash == deserialized.policy_hash
    tables = {"t": [{"a": "x", "b": 1}, {"a": "y", "b": 2}]}
    original_result = _execute_expr(expr, tables=tables)
    restored_result = _execute_expr(restored_expr, tables=tables)
    assert original_result == restored_result
    assert restored_result == (("x", 1),)


def test_ast_artifact_preserves_semantics() -> None:
    """Ensure AST artifacts preserve query semantics across serialization."""
    sql = "SELECT a, COUNT(*) AS cnt FROM t GROUP BY a HAVING COUNT(*) > 1"
    tables = {"t": [{"a": "x"}, {"a": "x"}, {"a": "y"}]}
    policy = default_sqlglot_policy()
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    artifact = ast_to_artifact(expr, sql=sql, policy=policy)
    serialized = serialize_ast_artifact(artifact)
    deserialized = deserialize_ast_artifact(serialized)
    restored_expr = artifact_to_ast(deserialized)
    original_result = _execute_expr(expr, tables=tables)
    restored_result = _execute_expr(restored_expr, tables=tables)
    assert original_result == restored_result
    assert restored_result == (("x", 2),)


def test_ast_artifact_metadata() -> None:
    """Ensure AstArtifact captures expected metadata fields."""
    sql = "SELECT * FROM t"
    policy = default_sqlglot_policy()
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    artifact = ast_to_artifact(expr, sql=sql, policy=policy)
    assert artifact.sql == sql
    assert isinstance(artifact.ast_json, str)
    assert len(artifact.ast_json) > 0
    assert isinstance(artifact.policy_hash, str)
    assert len(artifact.policy_hash) > 0


def test_serialization_format() -> None:
    """Ensure serialized artifact is valid JSON with expected structure."""
    sql = "SELECT a FROM t"
    policy = default_sqlglot_policy()
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    artifact = ast_to_artifact(expr, sql=sql, policy=policy)
    serialized = serialize_ast_artifact(artifact)
    parsed = json.loads(serialized)
    assert isinstance(parsed, dict)
    assert "sql" in parsed
    assert "ast_json" in parsed
    assert "policy_hash" in parsed
    assert parsed["sql"] == sql


def test_join_rewrite_semantics() -> None:
    """Ensure join rewrites preserve result semantics."""
    sql = "SELECT l.id, l.val, r.data FROM left_table l INNER JOIN right_table r ON l.id = r.id"
    tables = {
        "left_table": [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}],
        "right_table": [{"id": 1, "data": "x"}, {"id": 3, "data": "z"}],
    }
    policy = default_sqlglot_policy()
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    normalized = normalize_expr(
        expr,
        options=NormalizeExprOptions(
            schema=None,
            rules=None,
            rewrite_hook=None,
            enable_rewrites=True,
            policy=policy,
            sql=sql,
        ),
    )
    original_result = _execute_sql(sql, tables=tables)
    normalized_result = _execute_expr(normalized, tables=tables)
    assert original_result == normalized_result
    assert normalized_result == ((1, "a", "x"),)


def test_subquery_rewrite_semantics() -> None:
    """Ensure subquery rewrites preserve result semantics."""
    sql = "SELECT a, b FROM t WHERE b IN (SELECT MAX(b) FROM t)"
    tables = {"t": [{"a": "x", "b": 1}, {"a": "y", "b": 2}, {"a": "z", "b": 2}]}
    policy = default_sqlglot_policy()
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    normalized = normalize_expr(
        expr,
        options=NormalizeExprOptions(
            schema=None,
            rules=None,
            rewrite_hook=None,
            enable_rewrites=True,
            policy=policy,
            sql=sql,
        ),
    )
    original_result = _execute_sql(sql, tables=tables)
    normalized_result = _execute_expr(normalized, tables=tables)
    assert original_result == normalized_result
    assert normalized_result == (("y", 2), ("z", 2))
