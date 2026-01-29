"""Unified materialization and output writing helpers."""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from cache.diskcache_factory import DiskCacheKind, cache_for_kind, diskcache_stats_snapshot
from core_types import DeterminismTier
from datafusion_engine.arrow_interop import RecordBatchReader, RecordBatchReaderLike, TableLike
from datafusion_engine.dataset_locations import resolve_dataset_location
from datafusion_engine.diagnostics import record_artifact, record_events, recorder_for_profile
from datafusion_engine.execution_facade import DataFusionExecutionFacade, ExecutionResult
from datafusion_engine.ingest import datafusion_from_arrow
from datafusion_engine.param_binding import resolve_param_bindings
from datafusion_engine.param_tables import scalar_param_signature
from datafusion_engine.plan_bundle import DataFusionPlanBundle
from datafusion_engine.runtime import (
    DataFusionRuntimeProfile,
)
from datafusion_engine.streaming_executor import StreamingExecutionResult
from datafusion_engine.write_pipeline import WriteFormat, WriteMode, WritePipeline, WriteRequest
from engine.plan_policy import ExecutionSurfacePolicy
from engine.plan_product import PlanProduct
from obs.otel import OtelBootstrapOptions, configure_otel
from storage.deltalake import DeltaWriteResult

if TYPE_CHECKING:
    from datafusion_engine.runtime import SessionRuntime
    from datafusion_engine.scan_planner import ScanUnit
    from datafusion_engine.view_artifacts import DataFusionViewArtifact


def _resolve_prefer_reader(
    *,
    policy: ExecutionSurfacePolicy,
) -> bool:
    if policy.determinism_tier == DeterminismTier.CANONICAL:
        return False
    return policy.prefer_streaming


def _cache_event_reporter(
    runtime_profile: DataFusionRuntimeProfile | None,
) -> Callable[[Mapping[str, object]], None] | None:
    if runtime_profile is None:
        return None
    diagnostics = runtime_profile.diagnostics_sink
    if diagnostics is None:
        return None
    record_events = diagnostics.record_events

    def _record(event: Mapping[str, object]) -> None:
        record_events("datafusion_cache_events_v1", [event])

    return _record


@dataclass(frozen=True)
class CachePolicy:
    """Cache policy for DataFusion materialization surfaces."""

    enabled: bool
    reason: str
    writer_strategy: str
    param_mode: str
    param_signature: str | None


def _param_binding_state(params: Mapping[str, object] | None) -> tuple[str, str | None]:
    if not params:
        return "none", None
    bindings = resolve_param_bindings(params)
    has_scalar = bool(bindings.param_values)
    has_tables = bool(bindings.named_tables)
    if has_scalar and has_tables:
        return "mixed", None
    if has_tables:
        return "table", None
    if has_scalar:
        return "scalar", scalar_param_signature(bindings.param_values)
    return "none", None


def _resolve_cache_policy(
    *,
    policy: ExecutionSurfacePolicy,
    prefer_reader: bool,
    params: Mapping[str, object] | None,
) -> CachePolicy:
    writer_strategy = policy.writer_strategy
    param_mode, param_signature = _param_binding_state(params)
    if param_mode != "none":
        return CachePolicy(
            enabled=False,
            reason="params",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    if writer_strategy != "arrow":
        return CachePolicy(
            enabled=False,
            reason=f"writer_strategy_{writer_strategy}",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    if prefer_reader:
        return CachePolicy(
            enabled=False,
            reason="prefer_streaming",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    if policy.determinism_tier == DeterminismTier.BEST_EFFORT:
        return CachePolicy(
            enabled=False,
            reason="policy_best_effort",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    return CachePolicy(
        enabled=True,
        reason="materialize",
        writer_strategy=writer_strategy,
        param_mode=param_mode,
        param_signature=param_signature,
    )


def resolve_cache_policy(
    *,
    policy: ExecutionSurfacePolicy,
    prefer_reader: bool,
    params: Mapping[str, object] | None,
) -> CachePolicy:
    """Return the resolved cache policy for plan materialization.

    Returns
    -------
    CachePolicy
        Cache policy for the materialization.
    """
    return _resolve_cache_policy(
        policy=policy,
        prefer_reader=prefer_reader,
        params=params,
    )


def resolve_prefer_reader(*, policy: ExecutionSurfacePolicy) -> bool:
    """Return the prefer_reader flag for plan execution.

    Returns
    -------
    bool
        ``True`` when plan execution should prefer streaming readers.
    """
    return _resolve_prefer_reader(policy=policy)


def build_plan_product(
    plan: DataFusionPlanBundle,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    policy: ExecutionSurfacePolicy,
    plan_id: str | None = None,
) -> PlanProduct:
    """Raise because plan materialization is deprecated.

    Raises
    ------
    ValueError
        Always raised to enforce view-only materialization.
    """
    _ = (plan, runtime_profile, policy, plan_id)
    msg = "Plan materialization is deprecated; use build_view_product instead."
    raise ValueError(msg)


def _plan_view_bundle(
    view_name: str,
    *,
    session_runtime: SessionRuntime,
    runtime_profile: DataFusionRuntimeProfile,
) -> DataFusionPlanBundle:
    ctx = session_runtime.ctx
    df = ctx.table(view_name)
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
    return facade.build_plan_bundle(df)


def _plan_view_scan_units(
    bundle: DataFusionPlanBundle,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> tuple[tuple[ScanUnit, ...], tuple[str, ...]]:
    from datafusion_engine.lineage_datafusion import extract_lineage
    from datafusion_engine.scan_planner import plan_scan_unit

    session_runtime = runtime_profile.session_runtime()
    scan_units: dict[str, ScanUnit] = {}
    for scan in extract_lineage(
        bundle.optimized_logical_plan,
        udf_snapshot=bundle.artifacts.udf_snapshot,
    ).scans:
        location = runtime_profile.dataset_location(scan.dataset_name)
        if location is None:
            continue
        unit = plan_scan_unit(
            session_runtime.ctx,
            dataset_name=scan.dataset_name,
            location=location,
            lineage=scan,
        )
        scan_units[unit.key] = unit
    units = tuple(sorted(scan_units.values(), key=lambda unit: unit.key))
    return units, tuple(unit.key for unit in units)


def build_view_product(
    view_name: str,
    *,
    session_runtime: SessionRuntime,
    policy: ExecutionSurfacePolicy,
    view_id: str | None = None,
) -> PlanProduct:
    """Execute a registered view and return a PlanProduct wrapper.

    Parameters
    ----------
    view_name
        Registered view name to materialize.
    session_runtime
        Planning-ready DataFusion session runtime.
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
    configure_otel(
        service_name="codeanatomy",
        options=OtelBootstrapOptions(
            resource_overrides={"codeanatomy.view_name": view_name}
        ),
    )
    profile = session_runtime.profile
    session = session_runtime.ctx
    if not session.table_exist(view_name):
        msg = f"View {view_name!r} is not registered for materialization."
        raise ValueError(msg)
    from datafusion_engine.schema_registry import validate_nested_types

    validate_nested_types(session, view_name)
    bundle = _plan_view_bundle(
        view_name,
        session_runtime=session_runtime,
        runtime_profile=profile,
    )
    scan_units, scan_keys = _plan_view_scan_units(bundle, runtime_profile=profile)
    if scan_units:
        from datafusion_engine.scan_overrides import apply_scan_unit_overrides

        apply_scan_unit_overrides(
            session,
            scan_units=scan_units,
            runtime_profile=profile,
        )
    prefer_reader = _resolve_prefer_reader(policy=policy)
    stream: RecordBatchReaderLike | None = None
    table: TableLike | None = None
    view_artifact = (
        profile.view_registry.entries.get(view_name) if profile.view_registry is not None else None
    )
    facade = DataFusionExecutionFacade(ctx=session, runtime_profile=profile)
    df = facade.execute_plan_bundle(
        bundle,
        view_name=view_name,
        scan_units=scan_units,
        scan_keys=scan_keys,
    ).require_dataframe()
    if prefer_reader:
        stream = cast("RecordBatchReaderLike", StreamingExecutionResult(df).to_arrow_stream())
        schema = stream.schema
        result = ExecutionResult.from_reader(stream)
    else:
        table = cast("TableLike", df.to_arrow_table())
        schema = table.schema
        result = ExecutionResult.from_table(table)
    _record_plan_execution(
        profile,
        plan_id=view_id or view_name,
        result=result,
        view_artifact=view_artifact,
    )
    return PlanProduct(
        plan_id=view_id or view_name,
        schema=schema,
        determinism_tier=policy.determinism_tier,
        writer_strategy=policy.writer_strategy,
        view_artifact=view_artifact,
        stream=stream,
        table=table,
        execution_result=result,
    )


def _record_plan_execution(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    plan_id: str,
    result: ExecutionResult,
    view_artifact: DataFusionViewArtifact | None = None,
) -> None:
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
    record_artifact(runtime_profile, "plan_execute_v1", payload)


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


def write_extract_outputs(
    name: str,
    data: TableLike | RecordBatchReaderLike | Iterable[pa.RecordBatch],
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    """Write extract outputs using DataFusion-native paths when configured.

    Raises
    ------
    ValueError
        Raised when the DataFusion runtime profile is missing, the output yields no
        rows, or the location format is unsupported.
    """
    runtime_profile.record_schema_snapshots()
    location = resolve_dataset_location(name, runtime_profile=runtime_profile)
    if location is None:
        return
    reader, rows = _coerce_reader(data)
    if reader is None:
        msg = f"Extract output {name!r} yielded no rows."
        raise ValueError(msg)
    if location.format.lower() != "delta":
        msg = f"Delta-only extract writes are enforced; got {location.format!r}."
        raise ValueError(msg)
    session_runtime = runtime_profile.session_runtime()
    df = datafusion_from_arrow(
        session_runtime.ctx,
        name=f"__extract_output_{uuid.uuid4().hex}",
        value=reader,
    )
    commit_metadata = {
        "operation": "extract_output",
        "dataset": name,
        "row_count": str(rows) if rows is not None else "unknown",
    }
    pipeline = WritePipeline(
        session_runtime.ctx,
        sql_options=runtime_profile.sql_options(),
        recorder=recorder_for_profile(runtime_profile, operation_id=f"extract_write::{name}"),
        runtime_profile=runtime_profile,
    )
    write_result = pipeline.write(
        WriteRequest(
            source=df,
            destination=str(location.path),
            format=WriteFormat.DELTA,
            mode=WriteMode.APPEND,
            format_options={"commit_metadata": commit_metadata},
        )
    )
    _record_extract_write(
        runtime_profile,
        record=_ExtractWriteRecord(
            dataset=name,
            mode="append",
            path=str(location.path),
            file_format="delta",
            rows=rows,
            copy_sql=None,
            copy_options=None,
            delta_result=write_result.delta_result,
        ),
    )
    _record_diskcache_stats(runtime_profile)


__all__ = [
    "build_view_product",
    "resolve_cache_policy",
    "resolve_prefer_reader",
    "write_extract_outputs",
]
