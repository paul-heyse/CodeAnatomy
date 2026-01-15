"""Conformance tests for ArrowDSL fallback segmentation and metadata."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.core.context import (
    DeterminismTier,
    ExecutionContext,
    Ordering,
    execution_context_factory,
)
from arrowdsl.core.interop import pc
from arrowdsl.ir.plan import OpNode, PlanIR
from arrowdsl.ops.catalog import OP_CATALOG
from arrowdsl.plan.ordering_policy import apply_canonical_sort, ordering_metadata_for_plan
from arrowdsl.plan.planner import segment_plan

EXPECTED_SEGMENT_COUNT = 2


def _ctx_without_datafusion(ctx: ExecutionContext) -> ExecutionContext:
    runtime = ctx.runtime.with_datafusion(None)
    return ExecutionContext(
        runtime=runtime,
        mode=ctx.mode,
        provenance=ctx.provenance,
        safe_cast=ctx.safe_cast,
        debug=ctx.debug,
        schema_validation=ctx.schema_validation,
    )


def test_pipeline_breaker_segmentation() -> None:
    """Ensure pipeline breakers segment plans into distinct segments."""
    ctx = _ctx_without_datafusion(execution_context_factory("default"))
    plan = PlanIR(
        (
            OpNode(name="filter", args={"predicate": pc.greater(pc.field("a"), pc.scalar(1))}),
            OpNode(name="order_by", args={"sort_keys": (("a", "ascending"),)}),
            OpNode(
                name="project",
                args={"expressions": [pc.field("a")], "names": ["a"]},
            ),
        )
    )
    segmented = segment_plan(plan, catalog=OP_CATALOG, ctx=ctx)
    assert segmented.pipeline_breakers == ("order_by",)
    assert len(segmented.segments) == EXPECTED_SEGMENT_COUNT
    assert segmented.segments[0].ops[0].name == "filter"
    assert segmented.segments[0].ops[-1].name == "order_by"
    assert segmented.segments[1].ops[0].name == "project"


def test_ordering_metadata_consistency() -> None:
    """Ensure canonical ordering attaches ordering metadata."""
    table = pa.table({"id": [2, 1], "value": [10, 20]})
    sorted_table, keys = apply_canonical_sort(table, determinism=DeterminismTier.CANONICAL)
    spec = ordering_metadata_for_plan(
        Ordering.implicit(),
        schema=sorted_table.schema,
        canonical_keys=keys,
    )
    assert b"ordering_level" in spec.schema_metadata
    if keys:
        assert b"ordering_keys" in spec.schema_metadata
