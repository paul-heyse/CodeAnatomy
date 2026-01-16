"""Fallback execution runtime for segmented plans."""

from __future__ import annotations

from collections.abc import Callable

import pyarrow as pa

from arrowdsl.compile.kernel_compiler import KernelCompiler
from arrowdsl.compile.plan_compiler import PlanCompiler
from arrowdsl.core.context import DeterminismTier, ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.ir.plan import OpNode, PlanIR
from arrowdsl.ops.catalog import OP_CATALOG
from arrowdsl.plan.planner import Segment, SegmentPlan, segment_plan

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
        current = _run_segment(
            segment,
            ctx=ctx,
            plan=plan,
            current=current,
            df_executor=df_executor,
        )
    if current is None:
        msg = "No segments executed."
        raise ValueError(msg)
    return current


def _run_segment(
    segment: Segment,
    *,
    ctx: ExecutionContext,
    plan: SegmentPlan,
    current: TableLike | RecordBatchReaderLike | None,
    df_executor: DefExecutor | None,
) -> TableLike | RecordBatchReaderLike:
    if segment.lane == "datafusion":
        return _run_datafusion_segment(segment, ctx=ctx, df_executor=df_executor)
    if segment.lane == "acero":
        return _run_acero_segment(
            segment,
            ctx=ctx,
            plan=plan,
            current=current,
            df_executor=df_executor,
        )
    if segment.lane == "kernel":
        return _run_kernel_segment(segment, ctx=ctx, current=current)
    msg = f"Unsupported lane: {segment.lane!r}."
    raise ValueError(msg)


def _run_datafusion_segment(
    segment: Segment,
    *,
    ctx: ExecutionContext,
    df_executor: DefExecutor | None,
) -> TableLike | RecordBatchReaderLike:
    if df_executor is None:
        msg = "DataFusion execution must be provided by the primary pipeline."
        raise ValueError(msg)
    return df_executor(segment, ctx)


def _run_acero_segment(
    segment: Segment,
    *,
    ctx: ExecutionContext,
    plan: SegmentPlan,
    current: TableLike | RecordBatchReaderLike | None,
    df_executor: DefExecutor | None,
) -> TableLike | RecordBatchReaderLike:
    if _is_union_all_segment(segment):
        return _run_union_all(segment.ops[0], ctx=ctx, df_executor=df_executor)
    ops = segment.ops
    if current is not None and _needs_input_source(ops):
        ops = (_table_source_op(current), *ops)
    decl = PlanCompiler(catalog=OP_CATALOG).to_acero(PlanIR(ops), ctx=ctx)
    if streamable(plan, ctx=ctx):
        return decl.to_reader(use_threads=ctx.use_threads)
    return decl.to_table(use_threads=ctx.use_threads)


def _run_kernel_segment(
    segment: Segment,
    *,
    ctx: ExecutionContext,
    current: TableLike | RecordBatchReaderLike | None,
) -> TableLike:
    if current is None or isinstance(current, RecordBatchReaderLike):
        msg = "Kernel lane requires a materialized table."
        raise ValueError(msg)
    return KernelCompiler(catalog=OP_CATALOG).apply(
        PlanIR(segment.ops),
        table=current,
        ctx=ctx,
    )


def _is_union_all_segment(segment: Segment) -> bool:
    return len(segment.ops) == 1 and segment.ops[0].name == "union_all"


def _table_source_op(table: TableLike | RecordBatchReaderLike) -> OpNode:
    return OpNode(name="table_source", args={"table": table})


def _needs_input_source(ops: tuple[OpNode, ...]) -> bool:
    if not ops:
        return False
    first = ops[0].name
    return first not in {"scan", "table_source", "union_all", "hash_join", "winner_select"}


def _run_union_all(
    node: OpNode,
    *,
    ctx: ExecutionContext,
    df_executor: DefExecutor | None,
) -> TableLike:
    inputs = node.inputs
    if not inputs:
        msg = "union_all requires at least one input plan."
        raise ValueError(msg)
    tables: list[TableLike] = []
    for input_ir in inputs:
        segmented = segment_plan(input_ir, catalog=OP_CATALOG, ctx=ctx)
        result = run_segments(segmented, ctx=ctx, df_executor=df_executor)
        table = result.read_all() if isinstance(result, RecordBatchReaderLike) else result
        tables.append(table)
    return pa.concat_tables(tables)


__all__ = ["DefExecutor", "run_segments", "streamable"]
