"""Ordering policy checks for determinism tiers."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.dataset as ds

from arrowdsl.core.context import (
    DeterminismTier,
    ExecutionContext,
    OrderingLevel,
    runtime_profile_factory,
)
from arrowdsl.plan.query import QuerySpec
from arrowdsl.schema.metadata import ordering_from_schema


def _scan_plan(table: pa.Table, *, ctx: ExecutionContext) -> pa.Table:
    dataset = ds.dataset(table)
    spec = QuerySpec.simple(*table.column_names)
    plan = spec.to_plan(dataset=dataset, ctx=ctx, label="ordering_scan")
    return plan.to_table(ctx=ctx)


def test_stable_tier_implicit_ordering() -> None:
    """Keep implicit ordering for Tier 1 determinism."""
    table = pa.table({"entity_id": [2, 1], "name": ["b", "a"]})
    runtime = runtime_profile_factory("default").with_determinism(DeterminismTier.STABLE_SET)
    ctx = ExecutionContext(runtime=runtime)
    result = _scan_plan(table, ctx=ctx)
    ordering = ordering_from_schema(result.schema)
    assert ordering.level == OrderingLevel.IMPLICIT
    assert result.column("entity_id").to_pylist() == [2, 1]


def test_canonical_tier_explicit_ordering() -> None:
    """Apply canonical sort for Tier 2 determinism."""
    table = pa.table({"entity_id": [2, 1], "name": ["b", "a"]})
    runtime = runtime_profile_factory("default").with_determinism(DeterminismTier.CANONICAL)
    ctx = ExecutionContext(runtime=runtime)
    result = _scan_plan(table, ctx=ctx)
    ordering = ordering_from_schema(result.schema)
    assert ordering.level == OrderingLevel.EXPLICIT
    assert result.column("entity_id").to_pylist() == [1, 2]
