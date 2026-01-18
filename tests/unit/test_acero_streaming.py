"""Tests for Acero streaming and pipeline breaker behavior."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.core.context import DeterminismTier, ExecutionContext, RuntimeProfile
from arrowdsl.core.interop import pc
from arrowdsl.plan.builder import PlanBuilder
from arrowdsl.plan.plan import Plan, execute_plan


def _build_plan(builder: PlanBuilder) -> Plan:
    plan_ir, ordering, pipeline_breakers = builder.build()
    return Plan(ir=plan_ir, ordering=ordering, pipeline_breakers=pipeline_breakers)


def test_execute_plan_streams_without_pipeline_breakers() -> None:
    """Return readers when the plan has no pipeline breakers."""
    table = pa.table({"a": [2, 1]})
    builder = PlanBuilder()
    builder.table_source(table=table)
    builder.project(expressions=[pc.field("a")], names=["a"])
    plan = _build_plan(builder)
    ctx = ExecutionContext(
        runtime=RuntimeProfile(
            name="acero_stream",
            determinism=DeterminismTier.BEST_EFFORT,
            datafusion=None,
        )
    )
    result = execute_plan(plan, ctx=ctx, prefer_reader=True)
    assert isinstance(result.value, pa.RecordBatchReader)


def test_execute_plan_materializes_with_order_by() -> None:
    """Materialize plans containing order-by pipeline breakers."""
    table = pa.table({"a": [2, 1]})
    builder = PlanBuilder()
    builder.table_source(table=table)
    builder.order_by(sort_keys=(("a", "asc"),))
    plan = _build_plan(builder)
    ctx = ExecutionContext(
        runtime=RuntimeProfile(
            name="acero_breakers",
            determinism=DeterminismTier.BEST_EFFORT,
            datafusion=None,
        )
    )
    result = execute_plan(plan, ctx=ctx, prefer_reader=True)
    assert isinstance(result.value, pa.Table)
