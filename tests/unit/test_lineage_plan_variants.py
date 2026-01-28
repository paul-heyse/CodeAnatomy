"""Lineage extraction tests for DataFusion plan variants."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from datafusion_engine.lineage_datafusion import LineageReport, extract_lineage
from datafusion_engine.plan_bundle import build_plan_bundle
from datafusion_engine.runtime import DataFusionRuntimeProfile

pytest.importorskip("datafusion")

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.runtime import SessionRuntime


def _runtime_context() -> tuple[SessionContext, SessionRuntime]:
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    return ctx, profile.session_runtime()


def _register_base_tables(ctx: SessionContext) -> None:
    events = pa.table({"id": [1, 2], "label": ["a", "b"]})
    users = pa.table({"id": [1, 3], "name": ["alpha", "beta"]})
    ctx.register_record_batches("events", [events.to_batches()])
    ctx.register_record_batches("users", [users.to_batches()])


def _lineage_for_sql(
    ctx: SessionContext,
    session_runtime: SessionRuntime,
    *,
    sql: str,
) -> LineageReport:
    df = ctx.sql(sql)
    bundle = build_plan_bundle(ctx, df, session_runtime=session_runtime)
    return extract_lineage(bundle.optimized_logical_plan)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM events LIMIT 1",
        "EXPLAIN SELECT id FROM events",
        "EXPLAIN ANALYZE SELECT id FROM events",
    ],
)
def test_lineage_handles_limit_and_explain_variants(sql: str) -> None:
    """Preserve dependencies when plans include explain or limit nodes."""
    ctx, session_runtime = _runtime_context()
    _register_base_tables(ctx)
    lineage = _lineage_for_sql(ctx, session_runtime, sql=sql)
    assert "events" in lineage.referenced_tables


def test_lineage_includes_subquery_dependencies() -> None:
    """Capture dependencies referenced inside subqueries."""
    ctx, session_runtime = _runtime_context()
    _register_base_tables(ctx)
    lineage = _lineage_for_sql(
        ctx,
        session_runtime,
        sql="SELECT id FROM events WHERE id IN (SELECT id FROM users)",
    )
    assert set(lineage.referenced_tables) == {"events", "users"}


def test_lineage_includes_subquery_alias_dependencies() -> None:
    """Capture dependencies referenced inside subquery aliases."""
    ctx, session_runtime = _runtime_context()
    _register_base_tables(ctx)
    lineage = _lineage_for_sql(
        ctx,
        session_runtime,
        sql="SELECT sub.id FROM (SELECT id FROM events) AS sub",
    )
    assert "events" in lineage.referenced_tables


def test_lineage_handles_repartition_variants() -> None:
    """Handle repartition nodes without losing table dependencies."""
    ctx, session_runtime = _runtime_context()
    _register_base_tables(ctx)
    df = ctx.sql("SELECT id FROM events").repartition(2)
    bundle = build_plan_bundle(ctx, df, session_runtime=session_runtime)
    lineage = extract_lineage(bundle.optimized_logical_plan)
    assert "events" in lineage.referenced_tables


def test_lineage_captures_join_keys() -> None:
    """Capture join key expressions in lineage output."""
    ctx, session_runtime = _runtime_context()
    _register_base_tables(ctx)
    lineage = _lineage_for_sql(
        ctx,
        session_runtime,
        sql="SELECT events.id FROM events JOIN users ON events.id = users.id",
    )
    assert lineage.joins
    join = lineage.joins[0]
    assert any("id" in key for key in join.left_keys)
    assert any("id" in key for key in join.right_keys)


def test_lineage_handles_unnest_variants() -> None:
    """Keep base-table lineage when UNNEST appears in the plan."""
    ctx, session_runtime = _runtime_context()
    _register_base_tables(ctx)
    sql = "SELECT id, x FROM events CROSS JOIN UNNEST([1, 2]) AS t(x)"
    lineage = _lineage_for_sql(ctx, session_runtime, sql=sql)
    assert "events" in lineage.referenced_tables


def test_lineage_handles_recursive_query_variants() -> None:
    """Traverse recursive query plans without errors."""
    ctx, session_runtime = _runtime_context()
    sql = (
        "WITH RECURSIVE nums(n) AS ("
        "SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 3"
        ") SELECT * FROM nums"
    )
    lineage = _lineage_for_sql(ctx, session_runtime, sql=sql)
    assert "nums" in lineage.referenced_tables
