"""Unified materialization and output writing helpers."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReader, RecordBatchReaderLike, TableLike
from cache.diskcache_factory import DiskCacheKind, cache_for_kind, diskcache_stats_snapshot
from datafusion_engine.dataset_locations import resolve_dataset_location
from datafusion_engine.diagnostics import record_artifact, record_events
from datafusion_engine.execution_facade import ExecutionResult
from datafusion_engine.runtime import (
    DataFusionRuntimeProfile,
)
from datafusion_engine.streaming_executor import StreamingExecutionResult
from engine.plan_policy import ExecutionSurfacePolicy
from engine.plan_product import PlanProduct
from ibis_engine.execution_factory import ibis_execution_from_ctx
from ibis_engine.io_bridge import (
    IbisDatasetWriteOptions,
    IbisDeltaWriteOptions,
    write_ibis_dataset_delta,
)
from ibis_engine.params_bridge import param_binding_mode, param_binding_signature
from ibis_engine.plan import IbisPlan
from ibis_engine.registry import resolve_delta_log_storage_options
from ibis_engine.runner import IbisCachePolicy
from storage.deltalake import DeltaWriteResult

if TYPE_CHECKING:
    from datafusion_engine.view_artifacts import DataFusionViewArtifact, ViewArtifact
    from ibis_engine.execution import IbisExecutionContext
    from ibis_engine.registry import DatasetLocation


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
    """Raise because plan materialization is deprecated.

    Raises
    ------
    ValueError
        Always raised to enforce view-only materialization.
    """
    _ = (plan, execution, policy, plan_id)
    msg = "Plan materialization is deprecated; use build_view_product instead."
    raise ValueError(msg)


def build_view_product(
    view_name: str,
    *,
    execution: IbisExecutionContext,
    policy: ExecutionSurfacePolicy,
    view_id: str | None = None,
) -> PlanProduct:
    """Execute a registered view and return a PlanProduct wrapper.

    Parameters
    ----------
    view_name
        Registered view name to materialize.
    execution
        Execution context containing runtime profile and backend configuration.
    policy
        Execution policy controlling streaming preferences.
    view_id
        Optional identifier to use for diagnostics.

    Returns
    -------
    PlanProduct
        Plan product with stream or table output.

    Raises
    ------
    ValueError
        Raised when the runtime profile or view registration is missing.
    """
    ctx = execution.ctx
    profile = ctx.runtime.datafusion
    if profile is None:
        msg = "DataFusion runtime profile is required for view materialization."
        raise ValueError(msg)
    session = profile.session_context()
    if not session.table_exist(view_name):
        msg = f"View {view_name!r} is not registered for materialization."
        raise ValueError(msg)
    from datafusion_engine.schema_registry import validate_nested_types

    validate_nested_types(session, view_name)
    prefer_reader = _resolve_prefer_reader(ctx=ctx, policy=policy)
    stream: RecordBatchReaderLike | None = None
    table: TableLike | None = None
    view_artifact = (
        profile.view_registry.entries.get(view_name) if profile.view_registry is not None else None
    )
    df = session.table(view_name)
    if prefer_reader:
        stream = cast("RecordBatchReaderLike", StreamingExecutionResult(df).to_arrow_stream())
        schema = stream.schema
        result = ExecutionResult.from_reader(stream)
    else:
        table = cast("TableLike", df.to_arrow_table())
        schema = table.schema
        result = ExecutionResult.from_table(table)
    _record_plan_execution(
        ctx,
        plan_id=view_id or view_name,
        result=result,
        view_artifact=view_artifact,
    )
    return PlanProduct(
        plan_id=view_id or view_name,
        schema=schema,
        determinism_tier=ctx.determinism,
        writer_strategy=policy.writer_strategy,
        view_artifact=view_artifact,
        stream=stream,
        table=table,
        execution_result=result,
    )


def _record_plan_execution(
    ctx: ExecutionContext,
    *,
    plan_id: str,
    result: ExecutionResult,
    view_artifact: ViewArtifact | DataFusionViewArtifact | None = None,
) -> None:
    profile = ctx.runtime.datafusion
    if profile is None:
        return
    rows: int | None = None
    if result.table is not None:
        rows = result.table.num_rows
    payload = {
        "plan_id": plan_id,
        "result_kind": result.kind.value,
        "rows": rows,
    }
    if view_artifact is not None:
        payload["plan_fingerprint"] = view_artifact.plan_fingerprint
        ast_fingerprint = getattr(view_artifact, "ast_fingerprint", None)
        if isinstance(ast_fingerprint, str):
            payload["ast_fingerprint"] = ast_fingerprint
        policy_hash = getattr(view_artifact, "policy_hash", None)
        if isinstance(policy_hash, str):
            payload["policy_hash"] = policy_hash
    record_artifact(profile, "plan_execute_v1", payload)


@dataclass(frozen=True)
class _ExtractWriteRecord:
    dataset: str
    mode: str
    path: str
    file_format: str
    rows: int | None
    copy_sql: str | None
    copy_options: Mapping[str, object] | None
    delta_result: DeltaWriteResult | None


def _record_extract_write(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    record: _ExtractWriteRecord,
) -> None:
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset": record.dataset,
        "mode": record.mode,
        "path": record.path,
        "format": record.file_format,
        "rows": record.rows,
        "copy_sql": record.copy_sql,
        "copy_options": dict(record.copy_options) if record.copy_options is not None else None,
        "delta_version": record.delta_result.version if record.delta_result is not None else None,
    }
    record_artifact(runtime_profile, "datafusion_extract_output_writes_v1", payload)


def _record_diskcache_stats(runtime_profile: DataFusionRuntimeProfile) -> None:
    profile = runtime_profile.diskcache_profile
    if profile is None:
        return
    events: list[dict[str, object]] = []
    for kind in ("plan", "extract", "schema", "repo_scan", "runtime", "coordination"):
        cache = cache_for_kind(profile, cast("DiskCacheKind", kind))
        settings = profile.settings_for(cast("DiskCacheKind", kind))
        payload = diskcache_stats_snapshot(cache)
        payload.update(
            {
                "kind": kind,
                "profile_key": runtime_profile.context_cache_key(),
                "size_limit_bytes": settings.size_limit_bytes,
                "eviction_policy": settings.eviction_policy,
                "cull_limit": settings.cull_limit,
                "shards": settings.shards,
                "statistics": settings.statistics,
                "tag_index": settings.tag_index,
                "disk_min_file_size": settings.disk_min_file_size,
                "sqlite_journal_mode": settings.sqlite_journal_mode,
                "sqlite_mmap_size": settings.sqlite_mmap_size,
                "sqlite_synchronous": settings.sqlite_synchronous,
            }
        )
        events.append(payload)
    if events:
        record_events(runtime_profile, "diskcache_stats_v1", events)


def _ibis_execution_from_ctx(ctx: ExecutionContext) -> IbisExecutionContext:
    if ctx.runtime.datafusion is None:
        runtime_profile = ctx.runtime.with_datafusion(DataFusionRuntimeProfile())
        ctx = ExecutionContext(runtime=runtime_profile)
    return ibis_execution_from_ctx(ctx)


def _coerce_reader(
    data: TableLike | RecordBatchReaderLike | Iterable[pa.RecordBatch],
) -> tuple[RecordBatchReaderLike | None, int | None]:
    if isinstance(data, pa.Table):
        table = cast("pa.Table", data)
        return (
            pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
            int(table.num_rows),
        )
    if isinstance(data, RecordBatchReader):
        return cast("RecordBatchReaderLike", data), None
    if isinstance(data, Sequence):
        batches = list(cast("Sequence[pa.RecordBatch]", data))
        if not batches:
            return None, 0
        rows = sum(batch.num_rows for batch in batches)
        reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
        return reader, rows
    iterator = iter(cast("Iterable[pa.RecordBatch]", data))
    try:
        first = next(iterator)
    except StopIteration:
        return None, 0

    def _iter_batches() -> Iterable[pa.RecordBatch]:
        yield first
        yield from iterator

    reader = pa.RecordBatchReader.from_batches(first.schema, _iter_batches())
    return reader, None


@dataclass(frozen=True)
class _DeltaWriteContext:
    dataset: str
    runtime_profile: DataFusionRuntimeProfile
    location: DatasetLocation
    rows: int | None
    execution: IbisExecutionContext


def _write_delta(
    reader: RecordBatchReaderLike,
    *,
    context: _DeltaWriteContext,
) -> None:
    log_storage_options = resolve_delta_log_storage_options(context.location)
    datafusion_result = write_ibis_dataset_delta(
        reader,
        str(context.location.path),
        options=IbisDatasetWriteOptions(
            execution=context.execution,
            writer_strategy="datafusion",
            delta_options=IbisDeltaWriteOptions(
                mode="append",
                storage_options=context.location.storage_options,
                log_storage_options=log_storage_options,
            ),
        ),
        table_name=context.dataset,
    )
    _record_extract_write(
        context.runtime_profile,
        record=_ExtractWriteRecord(
            dataset=context.dataset,
            mode="insert",
            path=str(context.location.path),
            file_format="delta",
            rows=context.rows,
            copy_sql=None,
            copy_options=None,
            delta_result=datafusion_result,
        ),
    )


def write_extract_outputs(
    name: str,
    data: TableLike | RecordBatchReaderLike | Iterable[pa.RecordBatch],
    *,
    ctx: ExecutionContext,
) -> None:
    """Write extract outputs using DataFusion-native paths when configured.

    Raises
    ------
    ValueError
        Raised when the DataFusion runtime profile is missing, a dataset location is not
        registered, or the output yields no rows.
    """
    runtime_profile = ctx.runtime.datafusion
    if runtime_profile is None:
        msg = "DataFusion runtime profile is required for extract outputs."
        raise ValueError(msg)
    runtime_profile.record_schema_snapshots()
    location = resolve_dataset_location(name, runtime_profile=runtime_profile)
    if location is None:
        msg = f"No dataset location registered for extract output {name!r}."
        raise ValueError(msg)
    reader, rows = _coerce_reader(data)
    if reader is None:
        msg = f"Extract output {name!r} yielded no rows."
        raise ValueError(msg)
    if location.format.lower() != "delta":
        msg = f"Delta-only extract writes are enforced; got {location.format!r}."
        raise ValueError(msg)
    execution = _ibis_execution_from_ctx(ctx)
    _write_delta(
        reader,
        context=_DeltaWriteContext(
            dataset=name,
            runtime_profile=runtime_profile,
            location=location,
            rows=rows,
            execution=execution,
        ),
    )
    _record_diskcache_stats(runtime_profile)


__all__ = [
    "build_view_product",
    "resolve_cache_policy",
    "resolve_prefer_reader",
    "write_extract_outputs",
]
