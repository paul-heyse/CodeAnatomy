"""SQLGlot policy normalization and diff tests."""

from __future__ import annotations

from dataclasses import replace

import pyarrow as pa
import pytest

from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.plan_diff import semantic_diff_sql
from sqlglot_tools.compat import diff, exp, parse_one
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    ParseSqlOptions,
    SqlGlotCompileOptions,
    SqlGlotQualificationError,
    canonical_ast_fingerprint,
    compile_expr,
    default_sqlglot_policy,
    normalize_expr,
    normalize_expr_with_stats,
    parse_sql,
    parse_sql_strict,
    plan_fingerprint,
    register_datafusion_dialect,
    resolve_sqlglot_policy,
    sqlglot_emit,
    sqlglot_policy_snapshot,
    sqlglot_sql,
)

register_datafusion_dialect()
register_datafusion_dialect("datafusion")


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


def test_normalize_expr_pushes_predicates() -> None:
    """Push predicates into subqueries during normalization."""
    policy = default_sqlglot_policy()
    sql = "SELECT * FROM (SELECT a, b FROM t) WHERE a > 1"
    expr = parse_one(sql, dialect=policy.read_dialect)
    normalized = normalize_expr(
        expr,
        options=NormalizeExprOptions(
            schema={"t": {"a": "int", "b": "int"}},
            policy=policy,
            sql=sql,
        ),
    )
    inner_wheres = [
        select.args.get("where")
        for select in normalized.find_all(exp.Select)
        if select is not normalized
    ]
    assert any(
        where is not None
        and "a" in where.sql(dialect=policy.read_dialect)
        and "> 1" in where.sql(dialect=policy.read_dialect)
        for where in inner_wheres
    )
    outer_where = normalized.args.get("where")
    if outer_where is not None:
        assert isinstance(outer_where.this, exp.Boolean)
        assert outer_where.this.this is True


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


def test_plan_fingerprint_depends_on_policy_hash() -> None:
    """Include policy hash in plan fingerprints."""
    expr = parse_one("SELECT 1", dialect="datafusion")
    left = plan_fingerprint(expr, dialect="datafusion", policy_hash="policy-a")
    right = plan_fingerprint(expr, dialect="datafusion", policy_hash="policy-b")
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


def test_semantic_diff_records_edit_script() -> None:
    """Capture edit script entries for semantic diffs."""
    policy = default_sqlglot_policy()
    diff = semantic_diff_sql(
        "SELECT a FROM t",
        "SELECT b FROM t",
        policy=policy,
        dialect=policy.read_dialect,
    )
    assert diff.changed is True
    assert diff.edit_script


def test_canonical_ast_fingerprint_is_stable() -> None:
    """Fingerprint a canonical SQLGlot AST payload."""
    expr = parse_one("SELECT a FROM t", dialect="datafusion")
    fingerprint = canonical_ast_fingerprint(expr)
    assert fingerprint == canonical_ast_fingerprint(expr)


def test_transformed_sql_parses_in_datafusion() -> None:
    """Ensure transformed SQL parses in DataFusion."""
    policy = default_sqlglot_policy()
    ctx = DataFusionRuntimeProfile().session_context()
    table = pa.table({"a": [1, 2]})
    ctx.register_record_batches("t", [table.to_batches()])
    sql = "SELECT a FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY a) = 1"
    expr = parse_one(sql, dialect="duckdb")
    normalized = normalize_expr(
        expr,
        options=NormalizeExprOptions(
            schema={"t": {"a": "int64"}},
            policy=policy,
            sql=sql,
        ),
    )
    df = ctx.sql(sqlglot_sql(normalized, policy=policy))
    assert df is not None


def test_parse_diff_stability() -> None:
    """Ensure parse+diff outputs are stable across runs."""
    policy = default_sqlglot_policy()
    left = parse_sql(
        "SELECT a FROM t",
        options=ParseSqlOptions(dialect=policy.read_dialect, sanitize_templated=True),
    )
    right = parse_sql(
        "SELECT b FROM t",
        options=ParseSqlOptions(dialect=policy.read_dialect, sanitize_templated=True),
    )
    script_a = tuple(change.__class__.__name__ for change in diff(left, right))
    script_b = tuple(change.__class__.__name__ for change in diff(left, right))
    assert script_a == script_b


def test_sqlglot_emit_honors_identify() -> None:
    """Quote identifiers when policy.identify is enabled."""
    policy = replace(default_sqlglot_policy(), identify=True)
    expr = parse_one("SELECT foo FROM bar", dialect=policy.read_dialect)
    rendered = sqlglot_emit(expr, policy=policy)
    lowered = rendered.lower()
    assert '"foo"' in lowered
    assert '"bar"' in lowered


def test_duckdb_ir_emits_datafusion_sql() -> None:
    """Ensure canonical IR emits SQL that parses in DataFusion dialect."""
    canonical = default_sqlglot_policy()
    emit_policy = resolve_sqlglot_policy(name="datafusion_compile")
    sql_cases = (
        "SELECT a FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY a) = 1",
        "SELECT * FROM left_table FULL OUTER JOIN right_table ON left_table.id = right_table.id",
        "SELECT payload['key'] AS val FROM t",
    )
    for sql in sql_cases:
        expr = parse_sql(
            sql,
            options=ParseSqlOptions(dialect=canonical.read_dialect, sanitize_templated=True),
        )
        compiled = compile_expr(
            expr,
            options=SqlGlotCompileOptions(policy=canonical, sql=sql),
        )
        rendered = sqlglot_emit(compiled, policy=emit_policy)
        parse_sql_strict(rendered, dialect=emit_policy.write_dialect)


def test_qualification_failure_payload() -> None:
    """Capture payload details on qualification failures."""
    policy = default_sqlglot_policy()
    expr = parse_one("SELECT missing FROM t", dialect=policy.read_dialect)
    with pytest.raises(SqlGlotQualificationError) as excinfo:
        normalize_expr(
            expr,
            options=NormalizeExprOptions(
                schema={"t": {"a": "int64"}},
                policy=policy,
                sql="SELECT missing FROM t",
            ),
        )
    payload = excinfo.value.payload
    assert payload["dialect"] == policy.read_dialect
    assert payload["schema"] == {"t": {"a": "int64"}}
