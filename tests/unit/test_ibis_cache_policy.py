"""Tests for Ibis cache policy behavior."""

from __future__ import annotations

from collections.abc import Mapping

import ibis

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.runtime_profiles import RuntimeProfile
from datafusion_engine.runtime import DataFusionRuntimeProfile
from engine.materialize import build_plan_product
from engine.plan_policy import ExecutionSurfacePolicy
from ibis_engine.execution import IbisExecutionContext
from ibis_engine.plan import IbisPlan
from ibis_engine.runner import (
    IbisCachePolicy,
    IbisPlanExecutionOptions,
    materialize_plan,
    stream_plan,
)
from obs.diagnostics import DiagnosticsCollector


def test_cache_policy_disables_for_datafusion_writer() -> None:
    """Record cache policy decisions for DataFusion writers."""
    diagnostics = DiagnosticsCollector()
    runtime = RuntimeProfile(
        name="cache_policy",
        determinism=DeterminismTier.CANONICAL,
        datafusion=DataFusionRuntimeProfile(diagnostics_sink=diagnostics),
    )
    ctx = ExecutionContext(runtime=runtime)
    execution = IbisExecutionContext(ctx=ctx, ibis_backend=ibis.datafusion.connect())
    plan = IbisPlan(expr=ibis.memtable({"a": [1]}))
    policy = ExecutionSurfacePolicy(writer_strategy="datafusion")
    _ = build_plan_product(plan, execution=execution, policy=policy)
    events = diagnostics.events_snapshot().get("ibis_cache_events_v1", [])
    assert events
    assert events[0]["enabled"] is False
    assert events[0]["reason"] == "writer_strategy_datafusion"
    assert events[0]["writer_strategy"] == "datafusion"


def test_cache_policy_enables_for_arrow_writer() -> None:
    """Enable caching for materialized Arrow outputs."""
    diagnostics = DiagnosticsCollector()
    runtime = RuntimeProfile(
        name="cache_policy",
        determinism=DeterminismTier.CANONICAL,
        datafusion=DataFusionRuntimeProfile(diagnostics_sink=diagnostics),
    )
    ctx = ExecutionContext(runtime=runtime)
    execution = IbisExecutionContext(ctx=ctx, ibis_backend=ibis.datafusion.connect())
    plan = IbisPlan(expr=ibis.memtable({"a": [1, 2]}))
    policy = ExecutionSurfacePolicy(writer_strategy="arrow")
    _ = build_plan_product(plan, execution=execution, policy=policy)
    events = diagnostics.events_snapshot().get("ibis_cache_events_v1", [])
    assert events
    assert events[0]["enabled"] is True
    assert events[0]["reason"] == "materialize"
    assert events[0]["writer_strategy"] == "arrow"


def test_cache_context_releases_plan() -> None:
    """Release cached tables after scope exit."""
    backend = ibis.datafusion.connect()
    plan = IbisPlan(expr=ibis.memtable({"a": [1]}))
    with plan.cache() as cached:
        result = backend.execute(cached.expr)
        assert result["a"].tolist() == [1]
    result = backend.execute(plan.expr)
    assert result["a"].tolist() == [1]


def test_cache_policy_records_events() -> None:
    """Record cache decisions for materialize and stream paths."""
    events: list[dict[str, object]] = []

    def _record(event: Mapping[str, object]) -> None:
        events.append(dict(event))

    policy = IbisCachePolicy(enabled=False, reason="test", reporter=_record)
    execution = IbisPlanExecutionOptions(cache_policy=policy)
    plan = IbisPlan(expr=ibis.memtable({"a": [1, 2]}))

    materialize_plan(plan, execution=execution)
    assert events[0]["mode"] == "materialize"
    assert not events[0]["enabled"]

    events.clear()
    stream_plan(plan, execution=execution)
    assert events[0]["mode"] == "stream"
    assert not events[0]["enabled"]
