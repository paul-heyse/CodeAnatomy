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
from arrowdsl.plan.scan_builder import ScanBuildSpec
from arrowdsl.schema.metadata import ordering_from_schema
from ibis_engine.query_compiler import IbisQuerySpec
from tests.utils import values_as_list


def _scan_plan(table: pa.Table, *, ctx: ExecutionContext) -> pa.Table:
    dataset = ds.dataset(table)
    spec = IbisQuerySpec.simple(*table.column_names)
    plan = ScanBuildSpec(dataset=dataset, query=spec, ctx=ctx).to_plan(label="ordering_scan")
    return plan.to_table(ctx=ctx)


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
