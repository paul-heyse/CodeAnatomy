"""Unified materialization and output writing helpers."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from core.config_base import FingerprintableConfig, config_fingerprint
from core_types import DeterminismTier
from datafusion_engine.arrow.interop import RecordBatchReader, RecordBatchReaderLike, TableLike
from datafusion_engine.io.ingest import datafusion_from_arrow
from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from datafusion_engine.lineage.diagnostics import recorder_for_profile
from datafusion_engine.materialize_policy import MaterializationPolicy
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
from datafusion_engine.plan.execution_runtime import (
    PlanExecutionOptions,
    PlanScanOverrides,
)
from datafusion_engine.plan.execution_runtime import (
    execute_plan_artifact as execute_plan_artifact_helper,
)
from datafusion_engine.session.facade import DataFusionExecutionFacade, ExecutionResult
from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    record_schema_snapshots_for_profile,
)
from datafusion_engine.session.streaming import StreamingExecutionResult
from datafusion_engine.tables.param import resolve_param_bindings, scalar_param_signature
from extraction.diagnostics import EngineEventRecorder, ExtractQualityEvent, ExtractWriteEvent
from extraction.plan_product import PlanProduct
from extraction.semantic_boundary import ensure_semantic_views_registered, is_semantic_view
from obs.otel import OtelBootstrapOptions, configure_otel
from utils.uuid_factory import uuid7_hex
from utils.value_coercion import coerce_to_recordbatch_reader

if TYPE_CHECKING:
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.session.runtime import SessionRuntime
    from datafusion_engine.views.artifacts import CachePolicy as SemanticCachePolicy
    from semantics.compile_context import SemanticExecutionContext
    from semantics.program_manifest import ManifestDatasetResolver


def _resolve_prefer_reader(
    *,
    policy: MaterializationPolicy,
) -> bool:
    if policy.determinism_tier == DeterminismTier.CANONICAL:
        return False
    return policy.prefer_streaming


def _cache_event_reporter(
    runtime_profile: DataFusionRuntimeProfile | None,
) -> Callable[[Mapping[str, object]], None] | None:
    if runtime_profile is None:
        return None
    diagnostics = runtime_profile.diagnostics.diagnostics_sink
    if diagnostics is None:
        return None
    record_events = diagnostics.record_events

    def _record(event: Mapping[str, object]) -> None:
        record_events("datafusion_cache_events_v1", [event])

    return _record


@dataclass(frozen=True)
class MaterializationCacheDecision(FingerprintableConfig):
    """Cache policy for DataFusion materialization surfaces."""

    enabled: bool
    reason: str
    writer_strategy: str
    param_mode: str
    param_signature: str | None

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the cache decision.

        Returns:
        -------
        Mapping[str, object]
            Payload for fingerprint computation.
        """
        return {
            "enabled": self.enabled,
            "reason": self.reason,
            "writer_strategy": self.writer_strategy,
            "param_mode": self.param_mode,
            "param_signature": self.param_signature,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the cache decision.

        Returns:
        -------
        str
            Deterministic fingerprint.
        """
        return config_fingerprint(self.fingerprint_payload())


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


def _resolve_materialization_cache_decision(
    *,
    policy: MaterializationPolicy,
    prefer_reader: bool,
    params: Mapping[str, object] | None,
    semantic_cache_policy: SemanticCachePolicy | None = None,
) -> MaterializationCacheDecision:
    writer_strategy = policy.writer_strategy
    param_mode, param_signature = _param_binding_state(params)
    if param_mode != "none":
        return MaterializationCacheDecision(
            enabled=False,
            reason="params",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )

    # Semantic policy (authoritative) overrides engine heuristics
    if semantic_cache_policy is not None:
        return MaterializationCacheDecision(
            enabled=semantic_cache_policy != "none",
            reason=f"semantic_policy_{semantic_cache_policy}",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )

    if writer_strategy != "arrow":
        return MaterializationCacheDecision(
            enabled=False,
            reason=f"writer_strategy_{writer_strategy}",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    if prefer_reader:
        return MaterializationCacheDecision(
            enabled=False,
            reason="prefer_streaming",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    if policy.determinism_tier == DeterminismTier.BEST_EFFORT:
        return MaterializationCacheDecision(
            enabled=False,
            reason="policy_best_effort",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    return MaterializationCacheDecision(
        enabled=True,
        reason="materialize",
        writer_strategy=writer_strategy,
        param_mode=param_mode,
        param_signature=param_signature,
    )


def resolve_materialization_cache_decision(
    *,
    policy: MaterializationPolicy,
    prefer_reader: bool,
    params: Mapping[str, object] | None,
    semantic_cache_policy: SemanticCachePolicy | None = None,
) -> MaterializationCacheDecision:
    """Return the resolved cache policy for plan materialization.

    When semantic_cache_policy is provided, it is authoritative and originates
    from semantics (definition authority). Engine only adapts execution.

    Parameters
    ----------
    policy
        Materialization policy from planning_engine.
    prefer_reader
        Whether to prefer streaming readers.
    params
        Optional parameter bindings.
    semantic_cache_policy
        Optional semantic cache policy. When provided, takes precedence over
        engine heuristics for cache decisions.

    Returns:
    -------
    MaterializationCacheDecision
        Cache policy for the materialization.
    """
    return _resolve_materialization_cache_decision(
        policy=policy,
        prefer_reader=prefer_reader,
        params=params,
        semantic_cache_policy=semantic_cache_policy,
    )


def resolve_prefer_reader(*, policy: MaterializationPolicy) -> bool:
    """Return the prefer_reader flag for plan execution.

    Returns:
    -------
    bool
        ``True`` when plan execution should prefer streaming readers.
    """
    return _resolve_prefer_reader(policy=policy)


def _plan_view_bundle(
    view_name: str,
    *,
    session_runtime: SessionRuntime,
    runtime_profile: DataFusionRuntimeProfile,
) -> DataFusionPlanArtifact:
    ctx = session_runtime.ctx
    df = ctx.table(view_name)
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
    return facade.build_plan_artifact(df)


def _plan_view_scan_units(
    bundle: DataFusionPlanArtifact,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> tuple[tuple[ScanUnit, ...], tuple[str, ...]]:
    from datafusion_engine.lineage.datafusion import extract_lineage
    from datafusion_engine.lineage.scan import plan_scan_unit

    if dataset_resolver is None:
        msg = "dataset_resolver is required for _plan_view_scan_units; the dataset_catalog_from_profile fallback has been removed"
        raise ValueError(msg)
    session_runtime = runtime_profile.session_runtime()
    scan_units: dict[str, ScanUnit] = {}
    for scan in extract_lineage(
        bundle.optimized_logical_plan,
        udf_snapshot=bundle.artifacts.udf_snapshot,
    ).scans:
        location = dataset_resolver.location(scan.dataset_name)
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
    policy: MaterializationPolicy,
    view_id: str | None = None,
    dataset_resolver: ManifestDatasetResolver | None = None,
    execution_context: SemanticExecutionContext | None = None,
) -> PlanProduct:
    """Execute a registered view and return a PlanProduct wrapper.

    Args:
        view_name: Registered view name to execute.
        session_runtime: Session runtime to execute with.
        policy: Materialization policy.
        view_id: Optional explicit view identifier.
        dataset_resolver: Optional manifest-based dataset resolver.
        execution_context: Optional semantic execution context. When provided,
            the resolver is extracted from the context rather than creating a
            separate execution context.

    Returns:
        PlanProduct: Materialized plan product.

    Raises:
        ValueError: If the target view is not registered.
    """
    configure_otel(
        service_name="codeanatomy",
        options=OtelBootstrapOptions(resource_overrides={"codeanatomy.view_name": view_name}),
    )
    profile = session_runtime.profile
    if dataset_resolver is None and execution_context is not None:
        dataset_resolver = execution_context.dataset_resolver
    if dataset_resolver is None:
        msg = (
            "dataset_resolver is required for execute_plan_product; "
            "provide execution_context.dataset_resolver or an explicit resolver."
        )
        raise ValueError(msg)
    if is_semantic_view(view_name):
        ensure_semantic_views_registered(session_runtime.ctx, view_names=[view_name])
    if not session_runtime.ctx.table_exist(view_name):
        msg = f"View {view_name!r} is not registered for materialization."
        raise ValueError(msg)
    from datafusion_engine.schema.registry import validate_nested_types

    validate_nested_types(session_runtime.ctx, view_name)
    bundle = _plan_view_bundle(
        view_name,
        session_runtime=session_runtime,
        runtime_profile=profile,
    )
    scan_units, scan_keys = _plan_view_scan_units(
        bundle,
        runtime_profile=profile,
        dataset_resolver=dataset_resolver,
    )
    if scan_units:
        from datafusion_engine.dataset.resolution import apply_scan_unit_overrides

        apply_scan_unit_overrides(
            session_runtime.ctx,
            scan_units=scan_units,
            runtime_profile=profile,
            dataset_resolver=dataset_resolver,
        )
    semantic_cache_policy: SemanticCachePolicy | None = (
        profile.view_registry.entries[view_name].cache_policy
        if profile.view_registry is not None and view_name in profile.view_registry.entries
        else profile.data_sources.semantic_output.cache_overrides.get(view_name)
    )
    prefer_reader = _resolve_prefer_reader(policy=policy)
    cache_decision = resolve_materialization_cache_decision(
        policy=policy,
        prefer_reader=prefer_reader,
        params=None,
        semantic_cache_policy=semantic_cache_policy,
    )
    stream: RecordBatchReaderLike | None = None
    table: TableLike | None = None
    execution = execute_plan_artifact_helper(
        session_runtime.ctx,
        bundle,
        options=PlanExecutionOptions(
            runtime_profile=profile,
            view_name=view_name,
            scan=PlanScanOverrides(
                scan_units=scan_units,
                scan_keys=scan_keys,
                apply_scan_overrides=False,
            ),
        ),
    )
    df = execution.execution_result.require_dataframe()
    if prefer_reader:
        stream = cast("RecordBatchReaderLike", StreamingExecutionResult(df).to_arrow_stream())
        schema = stream.schema
        result = ExecutionResult.from_reader(stream)
    else:
        table = cast("TableLike", df.to_arrow_table())
        schema = table.schema
        result = ExecutionResult.from_table(table)
    EngineEventRecorder(profile).record_plan_execution(
        plan_id=view_id or view_name,
        result=result,
        view_artifact=None,
    )
    return PlanProduct(
        plan_id=view_id or view_name,
        schema=schema,
        determinism_tier=policy.determinism_tier,
        writer_strategy=policy.writer_strategy,
        view_artifact=None,
        cache_decision=cache_decision,
        stream=stream,
        table=table,
        execution_result=result,
    )


def _resolve_reader(
    data: TableLike | RecordBatchReaderLike | Iterable[pa.RecordBatch],
) -> tuple[RecordBatchReaderLike | None, int | None]:
    reader: RecordBatchReaderLike | None = None
    rows: int | None = None
    if isinstance(data, TableLike):
        reader = coerce_to_recordbatch_reader(data)
        rows = int(data.num_rows)
    elif isinstance(data, pa.RecordBatch):
        record_batch = cast("pa.RecordBatch", data)
        reader = coerce_to_recordbatch_reader(record_batch)
        rows = int(record_batch.num_rows)
    elif isinstance(data, RecordBatchReader):
        reader = cast("RecordBatchReaderLike", data)
    elif isinstance(data, Sequence):
        batches = [batch for batch in data if isinstance(batch, pa.RecordBatch)]
        if batches:
            reader = coerce_to_recordbatch_reader(batches)
            rows = sum(batch.num_rows for batch in batches)
        else:
            rows = 0
    else:
        iterator = iter(cast("Iterable[pa.RecordBatch]", data))
        try:
            first = next(iterator)
        except StopIteration:
            rows = 0
        else:

            def _iter_batches() -> Iterable[pa.RecordBatch]:
                yield first
                yield from iterator

            reader = pa.RecordBatchReader.from_batches(first.schema, _iter_batches())
    return reader, rows


def write_extract_outputs(
    name: str,
    data: TableLike | RecordBatchReaderLike | Iterable[pa.RecordBatch],
    *,
    runtime_profile: DataFusionRuntimeProfile,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> None:
    """Write extract outputs using DataFusion-native paths when configured.

    Emits diagnostics for extract output quality and write behavior.

    Raises:
        ValueError: If *dataset_resolver* is ``None``.
    """
    if dataset_resolver is None:
        msg = "dataset_resolver is required for write_extract_outputs; the dataset_catalog_from_profile fallback has been removed"
        raise ValueError(msg)
    record_schema_snapshots_for_profile(runtime_profile)
    recorder = EngineEventRecorder(runtime_profile)
    location = dataset_resolver.location(name)
    if location is None:
        recorder.record_extract_quality_events(
            [
                ExtractQualityEvent(
                    dataset=name,
                    stage="write",
                    status="missing_location",
                    rows=None,
                    location_path=None,
                    location_format=None,
                    issue="No extract dataset location configured.",
                )
            ]
        )
        return
    reader, rows = _resolve_reader(data)
    if reader is None:
        recorder.record_extract_quality_events(
            [
                ExtractQualityEvent(
                    dataset=name,
                    stage="write",
                    status="empty_output",
                    rows=rows,
                    location_path=str(location.path),
                    location_format=location.format,
                    issue="Extract output yielded no rows.",
                )
            ]
        )
        return
    if location.format.lower() != "delta":
        recorder.record_extract_quality_events(
            [
                ExtractQualityEvent(
                    dataset=name,
                    stage="write",
                    status="unsupported_format",
                    rows=rows,
                    location_path=str(location.path),
                    location_format=location.format,
                    issue="Delta-only extract writes are enforced.",
                )
            ]
        )
        return
    session_runtime = runtime_profile.session_runtime()
    df = datafusion_from_arrow(
        session_runtime.ctx,
        name=f"__extract_output_{uuid7_hex()}",
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
        dataset_resolver=dataset_resolver,
    )
    try:
        write_result = pipeline.write(
            WriteRequest(
                source=df,
                destination=str(location.path),
                format=WriteFormat.DELTA,
                mode=WriteMode.APPEND,
                format_options={"commit_metadata": commit_metadata},
            )
        )
    except (RuntimeError, ValueError, TypeError, OSError) as exc:
        recorder.record_extract_quality_events(
            [
                ExtractQualityEvent(
                    dataset=name,
                    stage="write",
                    status="write_failed",
                    rows=rows,
                    location_path=str(location.path),
                    location_format=location.format,
                    issue=str(exc),
                )
            ]
        )
        return
    recorder.record_extract_write(
        ExtractWriteEvent(
            dataset=name,
            mode="append",
            path=str(location.path),
            file_format="delta",
            rows=rows,
            copy_sql=None,
            copy_options=None,
            delta_version=write_result.delta_result.version
            if write_result.delta_result is not None
            else None,
        )
    )
    if rows == 0:
        recorder.record_extract_quality_events(
            [
                ExtractQualityEvent(
                    dataset=name,
                    stage="write",
                    status="zero_rows",
                    rows=rows,
                    location_path=str(location.path),
                    location_format=location.format,
                    issue="Extract output contained zero rows.",
                )
            ]
        )
    recorder.record_extract_quality_events(
        [
            ExtractQualityEvent(
                dataset=name,
                stage="write",
                status="written",
                rows=rows,
                location_path=str(location.path),
                location_format=location.format,
            )
        ]
    )
    recorder.record_diskcache_stats()


__all__ = [
    "MaterializationCacheDecision",
    "build_view_product",
    "resolve_materialization_cache_decision",
    "resolve_prefer_reader",
    "write_extract_outputs",
]
