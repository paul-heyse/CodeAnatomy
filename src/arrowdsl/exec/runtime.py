"""Fallback execution runtime for segmented plans."""

from __future__ import annotations

from collections.abc import Callable

from arrowdsl.compile.kernel_compiler import KernelCompiler
from arrowdsl.compile.plan_compiler import PlanCompiler
from arrowdsl.core.context import DeterminismTier, ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.ir.plan import PlanIR
from arrowdsl.ops.catalog import OP_CATALOG
from arrowdsl.plan.planner import Segment, SegmentPlan

DefExecutor = Callable[[Segment, ExecutionContext], TableLike]


def streamable(plan: SegmentPlan, *, ctx: ExecutionContext) -> bool:
    """Return whether the segmented plan can be streamed.

    Returns
    -------
    bool
        ``True`` when the plan can be streamed.
    """
    return not plan.pipeline_breakers and ctx.determinism != DeterminismTier.CANONICAL


def run_segments(
    plan: SegmentPlan,
    *,
    ctx: ExecutionContext,
    df_executor: DefExecutor | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Run segmented plan using the appropriate fallback lanes.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Materialized table or streaming reader for the segments.

    Raises
    ------
    ValueError
        Raised when a required lane executor is missing or no segments run.
    """
    current: TableLike | RecordBatchReaderLike | None = None
    for segment in plan.segments:
        if segment.lane == "datafusion":
            if df_executor is None:
                msg = "DataFusion execution must be provided by the primary pipeline."
                raise ValueError(msg)
            current = df_executor(segment, ctx)
            continue
        if segment.lane == "acero":
            decl = PlanCompiler(catalog=OP_CATALOG).to_acero(PlanIR(segment.ops), ctx=ctx)
            if streamable(plan, ctx=ctx):
                current = decl.to_reader(use_threads=ctx.use_threads)
            else:
                current = decl.to_table(use_threads=ctx.use_threads)
            continue
        if segment.lane == "kernel":
            if current is None or isinstance(current, RecordBatchReaderLike):
                msg = "Kernel lane requires a materialized table."
                raise ValueError(msg)
            current = KernelCompiler(catalog=OP_CATALOG).apply(
                PlanIR(segment.ops),
                table=current,
                ctx=ctx,
            )
            continue
        msg = f"Unsupported lane: {segment.lane!r}."
        raise ValueError(msg)
    if current is None:
        msg = "No segments executed."
        raise ValueError(msg)
    return current


__all__ = ["DefExecutor", "run_segments", "streamable"]
