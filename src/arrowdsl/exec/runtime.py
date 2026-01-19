"""Fallback execution runtime for segmented plans."""

from __future__ import annotations

import time
from collections.abc import Callable

import pyarrow as pa

from arrowdsl.compile.kernel_compiler import KernelCompiler
from arrowdsl.compile.plan_compiler import PlanCompiler
from arrowdsl.compute.kernels import apply_join
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.ir.plan import OpNode, PlanIR
from arrowdsl.ops.catalog import OP_CATALOG
from arrowdsl.plan.ops import JoinSpec
from arrowdsl.plan.planner import Segment, SegmentPlan, segment_plan

JOIN_FALLBACK_MAX_ROWS = 100_000

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
    current: TableLike | RecordBatchReaderLike | None,
    df_executor: DefExecutor | None,
) -> TableLike | RecordBatchReaderLike:
    if segment.lane == "datafusion":
        return _run_datafusion_segment(segment, ctx=ctx, df_executor=df_executor)
    if segment.lane == "acero":
        return _run_acero_segment(
            segment,
            ctx=ctx,
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
    current: TableLike | RecordBatchReaderLike | None,
    df_executor: DefExecutor | None,
) -> TableLike | RecordBatchReaderLike:
    if _is_union_all_segment(segment):
        return _run_union_all(segment.ops[0], ctx=ctx, df_executor=df_executor)
    fallback = _hash_join_fallback(
        segment,
        ctx=ctx,
        current=current,
        df_executor=df_executor,
    )
    if fallback is not None:
        return fallback
    ops = segment.ops
    if current is not None and _needs_input_source(ops):
        ops = (_table_source_op(current), *ops)
    decl = PlanCompiler(catalog=OP_CATALOG).to_acero(PlanIR(ops), ctx=ctx)
    if _segment_streamable(segment, ctx=ctx):
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


def _segment_streamable(segment: Segment, *, ctx: ExecutionContext) -> bool:
    if ctx.determinism == DeterminismTier.CANONICAL:
        return False
    return not _segment_has_pipeline_breaker(segment)


def _segment_has_pipeline_breaker(segment: Segment) -> bool:
    return any(OP_CATALOG[node.name].pipeline_breaker for node in segment.ops)


def _hash_join_fallback(
    segment: Segment,
    *,
    ctx: ExecutionContext,
    current: TableLike | RecordBatchReaderLike | None,
    df_executor: DefExecutor | None,
) -> TableLike | None:
    if len(segment.ops) != 1:
        return None
    node = segment.ops[0]
    if node.name != "hash_join":
        return None
    if current is None or isinstance(current, RecordBatchReaderLike):
        return None
    spec = node.args.get("spec")
    if not isinstance(spec, JoinSpec):
        return None
    if not node.inputs:
        msg = "hash_join requires a right-hand input plan."
        raise ValueError(msg)
    right_segmented = segment_plan(node.inputs[0], catalog=OP_CATALOG, ctx=ctx)
    right_result = run_segments(right_segmented, ctx=ctx, df_executor=df_executor)
    right_table = (
        right_result.read_all() if isinstance(right_result, RecordBatchReaderLike) else right_result
    )
    if current.num_rows > JOIN_FALLBACK_MAX_ROWS or right_table.num_rows > JOIN_FALLBACK_MAX_ROWS:
        return None
    joined = apply_join(current, right_table, spec=spec, use_threads=ctx.use_threads)
    _record_join_fallback(ctx, left_rows=current.num_rows, right_rows=right_table.num_rows)
    return joined


def _record_join_fallback(ctx: ExecutionContext, *, left_rows: int, right_rows: int) -> None:
    runtime = ctx.runtime.datafusion
    if runtime is None or runtime.diagnostics_sink is None:
        return
    runtime.diagnostics_sink.record_events(
        "datafusion_fallbacks_v1",
        [
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "reason": "arrowdsl_join_fallback",
                "error": f"left_rows={left_rows} right_rows={right_rows}",
                "expression_type": "hash_join",
                "sql": "",
                "dialect": "",
                "policy_violations": [],
            }
        ],
    )


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
