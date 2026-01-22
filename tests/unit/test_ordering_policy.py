"""Ordering policy checks for determinism tiers."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.core.runtime_profiles import runtime_profile_factory
from arrowdsl.schema.metadata import ordering_from_schema
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan
from ibis_engine.sources import plan_from_source
from tests.utils import values_as_list


def _scan_plan(table: pa.Table, *, ctx: ExecutionContext) -> pa.Table:
    runtime = ctx.runtime
    backend = build_backend(
        IbisBackendConfig(
            datafusion_profile=runtime.datafusion,
            fuse_selects=runtime.ibis_fuse_selects,
            default_limit=runtime.ibis_default_limit,
            default_dialect=runtime.ibis_default_dialect,
            interactive=runtime.ibis_interactive,
        )
    )
    plan = plan_from_source(table, ctx=ctx, backend=backend, name="ordering_scan")
    execution = IbisExecutionContext(ctx=ctx, ibis_backend=backend)
    return materialize_ibis_plan(plan, execution=execution)


def test_stable_tier_implicit_ordering() -> None:
    """Keep implicit ordering for Tier 1 determinism."""
    table = pa.table({"entity_id": [2, 1], "name": ["b", "a"]})
    runtime = runtime_profile_factory("default").with_determinism(DeterminismTier.STABLE_SET)
    ctx = ExecutionContext(runtime=runtime)
    result = _scan_plan(table, ctx=ctx)
    ordering = ordering_from_schema(result.schema)
    assert ordering.level == OrderingLevel.IMPLICIT
    assert values_as_list(result.column("entity_id")) == [2, 1]


def test_canonical_tier_explicit_ordering() -> None:
    """Apply canonical sort for Tier 2 determinism."""
    table = pa.table({"entity_id": [2, 1], "name": ["b", "a"]})
    runtime = runtime_profile_factory("default").with_determinism(DeterminismTier.CANONICAL)
    ctx = ExecutionContext(runtime=runtime)
    result = _scan_plan(table, ctx=ctx)
    ordering = ordering_from_schema(result.schema)
    assert ordering.level == OrderingLevel.EXPLICIT
    assert values_as_list(result.column("entity_id")) == [1, 2]
