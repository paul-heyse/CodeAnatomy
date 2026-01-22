"""Materialization helpers for plan outputs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace

import pyarrow as pa
from datafusion.dataframe import DataFrame
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.bridge import datafusion_to_reader
from engine.plan_policy import ExecutionSurfacePolicy
from engine.plan_product import PlanProduct
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan, stream_ibis_plan
from ibis_engine.params_bridge import param_binding_mode, param_binding_signature
from ibis_engine.plan import IbisPlan
from ibis_engine.runner import IbisCachePolicy


def _default_plan_id(plan: IbisPlan) -> str:
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


def _cache_event_reporter(
    ctx: ExecutionContext,
) -> Callable[[Mapping[str, object]], None] | None:
    profile = ctx.runtime.datafusion
    if profile is None:
        return None
    diagnostics = profile.diagnostics_sink
    if diagnostics is None:
        return None
    record_events = diagnostics.record_events

    def _record(event: Mapping[str, object]) -> None:
        record_events("ibis_cache_events_v1", [event])

    return _record


def _resolve_cache_policy(
    *,
    ctx: ExecutionContext,
    policy: ExecutionSurfacePolicy,
    prefer_reader: bool,
    params: Mapping[IbisValue, object] | Mapping[str, object] | None,
) -> IbisCachePolicy:
    writer_strategy = policy.writer_strategy
    param_mode = param_binding_mode(params)
    param_signature = param_binding_signature(params)
    if param_mode != "none":
        return IbisCachePolicy(
            enabled=False,
            reason="params",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    if writer_strategy != "arrow":
        return IbisCachePolicy(
            enabled=False,
            reason=f"writer_strategy_{writer_strategy}",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    if prefer_reader:
        return IbisCachePolicy(
            enabled=False,
            reason="prefer_streaming",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    if ctx.determinism == DeterminismTier.BEST_EFFORT:
        return IbisCachePolicy(
            enabled=False,
            reason="best_effort",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    if policy.determinism_tier == DeterminismTier.BEST_EFFORT:
        return IbisCachePolicy(
            enabled=False,
            reason="policy_best_effort",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    return IbisCachePolicy(
        enabled=True,
        reason="materialize",
        writer_strategy=writer_strategy,
        param_mode=param_mode,
        param_signature=param_signature,
    )


def resolve_cache_policy(
    *,
    ctx: ExecutionContext,
    policy: ExecutionSurfacePolicy,
    prefer_reader: bool,
    params: Mapping[IbisValue, object] | Mapping[str, object] | None,
) -> IbisCachePolicy:
    """Return the resolved cache policy for plan materialization.

    Returns
    -------
    IbisCachePolicy
        Cache policy for the materialization.
    """
    return _resolve_cache_policy(
        ctx=ctx,
        policy=policy,
        prefer_reader=prefer_reader,
        params=params,
    )


def resolve_prefer_reader(*, ctx: ExecutionContext, policy: ExecutionSurfacePolicy) -> bool:
    """Return the prefer_reader flag for plan execution.

    Returns
    -------
    bool
        ``True`` when plan execution should prefer streaming readers.
    """
    return _resolve_prefer_reader(ctx=ctx, policy=policy)


def build_plan_product(
    plan: IbisPlan,
    *,
    execution: IbisExecutionContext,
    policy: ExecutionSurfacePolicy,
    plan_id: str | None = None,
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
    ctx = execution.ctx
    prefer_reader = _resolve_prefer_reader(ctx=ctx, policy=policy)
    cache_policy = _resolve_cache_policy(
        ctx=ctx,
        policy=policy,
        prefer_reader=prefer_reader,
        params=execution.params,
    )
    reporter = _cache_event_reporter(ctx)
    if reporter is not None:
        cache_policy = replace(cache_policy, reporter=reporter)
    execution = replace(execution, cache_policy=cache_policy)
    stream: RecordBatchReaderLike | None = None
    table: TableLike | None = None
    if prefer_reader:
        stream = stream_ibis_plan(plan, execution=execution)
        if not isinstance(stream, pa.RecordBatchReader):
            msg = "Expected RecordBatchReader for reader materialization."
            raise ValueError(msg)
        schema = stream.schema
    else:
        table = materialize_ibis_plan(plan, execution=execution)
        schema = table.schema
    return PlanProduct(
        plan_id=plan_id or _default_plan_id(plan),
        schema=schema,
        determinism_tier=ctx.determinism,
        writer_strategy=policy.writer_strategy,
        stream=stream,
        table=table,
    )


def df_to_reader(df: DataFrame) -> pa.RecordBatchReader:
    """Convert a DataFusion DataFrame to a streaming RecordBatchReader.

    Prefers the __arrow_c_stream__ protocol for zero-copy streaming.

    Parameters
    ----------
    df : DataFrame
        DataFusion DataFrame to convert.

    Returns
    -------
    pa.RecordBatchReader
        Streaming reader for the DataFrame results.
    """
    return datafusion_to_reader(df)


__all__ = [
    "build_plan_product",
    "df_to_reader",
    "resolve_prefer_reader",
]
