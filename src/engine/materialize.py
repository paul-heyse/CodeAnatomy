"""Materialization helpers for plan outputs."""

from __future__ import annotations

from dataclasses import replace
from typing import cast

import pyarrow as pa

from arrowdsl.core.context import DeterminismTier, ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import AdapterRunOptions, run_plan_adapter
from engine.plan_policy import ExecutionSurfacePolicy
from engine.plan_product import PlanProduct
from ibis_engine.plan import IbisPlan


def _default_plan_id(plan: Plan | IbisPlan) -> str:
    label = getattr(plan, "label", "")
    if isinstance(label, str) and label:
        return label
    return "plan"


def _resolve_prefer_reader(
    *,
    ctx: ExecutionContext,
    policy: ExecutionSurfacePolicy,
) -> bool:
    if ctx.determinism == DeterminismTier.CANONICAL:
        return False
    return policy.prefer_streaming


def build_plan_product(
    plan: Plan | IbisPlan,
    *,
    ctx: ExecutionContext,
    policy: ExecutionSurfacePolicy,
    plan_id: str | None = None,
    options: AdapterRunOptions | None = None,
) -> PlanProduct:
    """Execute a plan and return a PlanProduct wrapper.

    Returns
    -------
    PlanProduct
        Plan output with schema and materialization metadata.

    Raises
    ------
    ValueError
        Raised when a reader materialization is missing the expected stream.
    """
    prefer_reader = _resolve_prefer_reader(ctx=ctx, policy=policy)
    resolved = options or AdapterRunOptions()
    resolved = replace(resolved, prefer_reader=prefer_reader)
    result = run_plan_adapter(plan, ctx=ctx, options=resolved)
    value = result.value
    schema = value.schema
    stream: RecordBatchReaderLike | None = None
    table: TableLike | None = None
    if result.kind == "reader":
        stream = cast("RecordBatchReaderLike", value)
        if not isinstance(stream, pa.RecordBatchReader):
            msg = "Expected RecordBatchReader for reader materialization."
            raise ValueError(msg)
    elif isinstance(value, pa.RecordBatchReader):
        reader = cast("pa.RecordBatchReader", value)
        table = cast("TableLike", reader.read_all())
    else:
        table = cast("TableLike", value)
    return PlanProduct(
        plan_id=plan_id or _default_plan_id(plan),
        schema=schema,
        determinism_tier=ctx.determinism,
        writer_strategy=policy.writer_strategy,
        stream=stream,
        table=table,
    )


__all__ = ["build_plan_product"]
