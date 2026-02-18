"""Delta-specific write helpers for WritePipeline."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, cast

from datafusion_engine.delta.service import DeltaFeatureMutationRequest
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.io.write_core import (
    DeltaWriteOutcome,
    DeltaWriteSpec,
    WriteMode,
    WriteRequest,
    _DeltaCommitContext,
    _DeltaWriteSpecInputs,
    _require_runtime_profile,
    _stats_decision_from_policy,
)
from datafusion_engine.io.write_delta import (
    _apply_delta_check_constraints,
    _apply_explicit_delta_features,
    _apply_policy_commit_metadata,
    _delta_commit_metadata,
    _delta_feature_gate_override,
    _delta_idempotent_options,
    _delta_maintenance_policy_override,
    _delta_mode,
    _delta_schema_mode,
    _replace_where_predicate,
    _resolve_delta_schema_policy,
    _validate_delta_protocol_support,
)
from datafusion_engine.schema.contracts import delta_constraints_for_location
from storage.deltalake import (
    DeltaWriteResult,
    canonical_table_uri,
    idempotent_commit_properties,
    snapshot_key_for_table,
)
from storage.deltalake.delta_runtime_ops import commit_metadata_from_properties
from utils.hashing import hash_sha256_hex

if TYPE_CHECKING:
    from datafusion_engine.delta.observability import DeltaOperationReport
    from datafusion_engine.io.write_pipeline import WritePipeline
    from datafusion_engine.obs.datafusion_runs import DataFusionRun
    from datafusion_engine.session.streaming import StreamingExecutionResult
    from storage.deltalake.delta_write import IdempotentWriteOptions


class _DeltaCommitFinalizeContextLike(Protocol):
    @property
    def spec(self) -> DeltaWriteSpec: ...

    @property
    def delta_version(self) -> int: ...

    @property
    def duration_ms(self) -> float | None: ...

    @property
    def row_count(self) -> int | None: ...

    @property
    def status(self) -> str: ...

    @property
    def error(self) -> str | None: ...


class _DeltaBootstrapPipeline(Protocol):
    def write_delta_bootstrap(
        self,
        result: StreamingExecutionResult,
        *,
        spec: DeltaWriteSpec,
    ) -> DeltaWriteResult: ...


class DeltaWriteHandler:
    """Adapter wrapper for delta-write bootstrap delegation."""

    def __init__(self, pipeline: _DeltaBootstrapPipeline) -> None:
        """Initialize handler with a pipeline implementing bootstrap writes."""
        self._pipeline = pipeline

    def write_bootstrap(
        self,
        result: StreamingExecutionResult,
        *,
        spec: DeltaWriteSpec,
    ) -> DeltaWriteResult:
        """Delegate bootstrap writing to the injected pipeline.

        Returns:
            DeltaWriteResult: Result produced by the pipeline bootstrap write.
        """
        return self._pipeline.write_delta_bootstrap(result, spec=spec)


def prepare_commit_metadata(
    pipeline: WritePipeline,
    *,
    commit_key: str,
    commit_metadata: dict[str, str],
    method_label: str,
    mode: Literal["append", "overwrite"],
    options: Mapping[str, object],
) -> tuple[dict[str, str], IdempotentWriteOptions | None, DataFusionRun | None]:
    """Finalize commit metadata with idempotent and runtime commit context.

    Returns:
        tuple[dict[str, str], IdempotentWriteOptions | None, DataFusionRun | None]: Metadata, idempotent
            options, and optional runtime commit reservation.
    """
    metadata = dict(commit_metadata)
    commit_run: DataFusionRun | None = None
    idempotent = _delta_idempotent_options(options)
    if idempotent is None:
        reserved = reserve_runtime_commit(
            pipeline,
            commit_key=commit_key,
            commit_metadata=metadata,
            method_label=method_label,
            mode=mode,
        )
        if reserved is not None:
            idempotent, commit_run = reserved
    if idempotent is not None:
        metadata["commit_app_id"] = idempotent.app_id
        metadata["commit_version"] = str(idempotent.version)
    if commit_run is not None:
        metadata["commit_run_id"] = commit_run.run_id
    return metadata, idempotent, commit_run


def delta_write_spec(
    pipeline: WritePipeline,
    request: WriteRequest,
    *,
    method_label: str,
    inputs: _DeltaWriteSpecInputs,
) -> DeltaWriteSpec:
    """Build a deterministic Delta write specification for a request.

    Returns:
        DeltaWriteSpec: Deterministic Delta write specification for execution.
    """
    options = request.format_options or {}
    mode = _delta_mode(request.mode)
    schema_policy = _resolve_delta_schema_policy(
        options,
        dataset_location=inputs.dataset_location,
    )
    maintenance_policy = _delta_maintenance_policy_override(options)
    if maintenance_policy is None:
        maintenance_policy = (
            inputs.dataset_location.delta_maintenance_policy
            if inputs.dataset_location is not None
            else None
        )
    from datafusion_engine.io.write_core import _delta_policy_context

    policy_ctx = _delta_policy_context(
        options=options,
        dataset_location=inputs.dataset_location,
        request_partition_by=request.partition_by,
        schema_columns=inputs.schema_columns,
        lineage_columns=inputs.lineage_columns,
        plan_bundle=inputs.plan_bundle,
    )
    if policy_ctx.adaptive_file_size_decision is not None:
        record_policy = getattr(pipeline, "_record_adaptive_write_policy", None)
        if callable(record_policy):
            record_policy(policy_ctx.adaptive_file_size_decision)
    feature_gate = _delta_feature_gate_override(options)
    if feature_gate is None and inputs.dataset_location is not None:
        feature_gate = inputs.dataset_location.delta_feature_gate
    stats_decision = _stats_decision_from_policy(
        dataset_name=inputs.dataset_name or request.destination,
        policy_ctx=policy_ctx,
        lineage_columns=inputs.lineage_columns,
    )
    extra_constraints = delta_constraints_for_location(
        inputs.dataset_location,
        extra_checks=request.constraints,
    )
    commit_metadata = _delta_commit_metadata(
        request,
        options,
        context=_DeltaCommitContext(
            method_label=method_label,
            mode=mode,
            dataset_name=inputs.dataset_name,
            dataset_location=inputs.dataset_location,
        ),
    )
    commit_metadata = _apply_policy_commit_metadata(
        commit_metadata,
        policy_ctx=policy_ctx,
        extra_constraints=extra_constraints,
    )
    commit_key = inputs.dataset_name or request.destination
    commit_metadata, idempotent, commit_run = prepare_commit_metadata(
        pipeline,
        commit_key=commit_key,
        commit_metadata=commit_metadata,
        method_label=method_label,
        mode=mode,
        options=options,
    )
    commit_properties = idempotent_commit_properties(
        operation="write_pipeline",
        mode=mode,
        idempotent=idempotent,
        extra_metadata=commit_metadata,
    )
    commit_metadata = commit_metadata_from_properties(commit_properties)
    return DeltaWriteSpec(
        table_uri=request.destination,
        mode=mode,
        method_label=method_label,
        commit_properties=commit_properties,
        commit_metadata=commit_metadata,
        commit_key=commit_key,
        dataset_location=inputs.dataset_location,
        write_policy=policy_ctx.write_policy,
        schema_policy=schema_policy,
        maintenance_policy=maintenance_policy,
        partition_by=policy_ctx.partition_by,
        zorder_by=policy_ctx.zorder_by,
        enable_features=policy_ctx.enable_features,
        feature_gate=feature_gate,
        table_properties=policy_ctx.table_properties,
        target_file_size=policy_ctx.target_file_size,
        schema_mode=_delta_schema_mode(
            options,
            schema_policy=schema_policy,
        ),
        writer_properties=policy_ctx.writer_properties,
        stats_decision=stats_decision,
        commit_app_id=idempotent.app_id if idempotent is not None else None,
        commit_version=idempotent.version if idempotent is not None else None,
        commit_run=commit_run,
        storage_options=policy_ctx.storage_options,
        log_storage_options=policy_ctx.log_storage_options,
        replace_predicate=_replace_where_predicate(options),
        extra_constraints=extra_constraints,
    )


def reserve_runtime_commit(
    pipeline: WritePipeline,
    *,
    commit_key: str,
    commit_metadata: Mapping[str, str],
    method_label: str,
    mode: Literal["append", "overwrite"],
) -> tuple[IdempotentWriteOptions, DataFusionRun] | None:
    """Reserve idempotent Delta commit options from runtime profile.

    Returns:
        tuple[IdempotentWriteOptions, DataFusionRun] | None: Reserved idempotent options and run handle.
    """
    if pipeline.runtime_profile is None:
        return None
    commit_options, commit_run = pipeline.runtime_profile.delta_ops.reserve_delta_commit(
        key=commit_key,
        metadata={
            "destination": commit_key,
            "method": method_label,
            "mode": mode,
            "format": "delta",
        },
        commit_metadata=commit_metadata,
    )
    return commit_options, commit_run


def finalize_delta_commit(
    pipeline: WritePipeline,
    context: _DeltaCommitFinalizeContextLike,
) -> None:
    """Finalize reserved Delta commit and persist write artifact."""
    if pipeline.runtime_profile is None:
        return
    spec = context.spec
    if spec.commit_run is not None:
        metadata: dict[str, object] = {
            "destination": spec.table_uri,
            "method": spec.method_label,
            "mode": spec.mode,
            "delta_version": context.delta_version,
        }
        if spec.commit_app_id is not None:
            metadata["commit_app_id"] = spec.commit_app_id
        if spec.commit_version is not None:
            metadata["commit_version"] = spec.commit_version
        pipeline.runtime_profile.delta_ops.finalize_delta_commit(
            key=spec.commit_key,
            run=spec.commit_run,
            metadata=metadata,
        )
    persist_write_artifact(pipeline, context)


def persist_write_artifact(
    pipeline: WritePipeline,
    context: _DeltaCommitFinalizeContextLike,
) -> None:
    """Persist write metadata to the plan artifact store."""
    if pipeline.runtime_profile is None:
        return
    from datafusion_engine.plan.artifact_store_persistence import (
        WriteArtifactRequest,
        persist_write_artifact,
    )

    spec = context.spec
    commit_run_id = spec.commit_run.run_id if spec.commit_run is not None else None
    persist_write_artifact(
        pipeline.runtime_profile,
        request=WriteArtifactRequest(
            destination=spec.commit_key,
            write_format="delta",
            mode=spec.mode,
            method=spec.method_label,
            table_uri=spec.table_uri,
            delta_version=context.delta_version,
            commit_app_id=spec.commit_app_id,
            commit_version=spec.commit_version,
            commit_run_id=commit_run_id,
            delta_write_policy=spec.write_policy,
            delta_schema_policy=spec.schema_policy,
            partition_by=spec.partition_by,
            table_properties=dict(spec.table_properties),
            commit_metadata=dict(spec.commit_metadata),
            stats_decision=spec.stats_decision,
            duration_ms=context.duration_ms,
            row_count=context.row_count,
            status=context.status,
            error=context.error,
        ),
    )
    if spec.stats_decision is None:
        return
    from datafusion_engine.lineage.diagnostics import record_artifact
    from serde_artifact_specs import DELTA_STATS_DECISION_SPEC
    from serde_artifacts import DeltaStatsDecisionEnvelope
    from serde_msgspec import convert, to_builtins

    envelope = DeltaStatsDecisionEnvelope(payload=spec.stats_decision)
    validated = convert(
        to_builtins(envelope, str_keys=True),
        target_type=DeltaStatsDecisionEnvelope,
        strict=True,
    )
    record_artifact(
        pipeline.runtime_profile,
        DELTA_STATS_DECISION_SPEC,
        cast("dict[str, object]", to_builtins(validated, str_keys=True)),
    )


def record_delta_mutation(
    pipeline: WritePipeline,
    *,
    spec: DeltaWriteSpec,
    delta_result: DeltaWriteResult,
    operation: str,
    constraint_status: str,
) -> None:
    """Record Delta mutation observability artifact."""
    if pipeline.runtime_profile is None:
        return
    operation_name = spec.commit_metadata.get("operation")
    from datafusion_engine.io.write_pipeline import _is_delta_observability_operation

    if _is_delta_observability_operation(operation_name):
        return
    from datafusion_engine.delta.observability import (
        DeltaMutationArtifact,
        DeltaOperationReport,
        record_delta_mutation,
    )

    operation_report = DeltaOperationReport.from_payload(
        delta_result.report,
        operation=operation,
        commit_metadata=spec.commit_metadata,
    )

    commit_run_id = spec.commit_run.run_id if spec.commit_run is not None else None
    record_delta_mutation(
        pipeline.runtime_profile,
        artifact=DeltaMutationArtifact(
            table_uri=spec.table_uri,
            operation=operation,
            report=operation_report.to_payload(),
            dataset_name=spec.commit_key,
            mode=spec.mode,
            commit_metadata=spec.commit_metadata,
            commit_app_id=spec.commit_app_id,
            commit_version=spec.commit_version,
            commit_run_id=commit_run_id,
            constraint_status=constraint_status,
            constraint_violations=(),
        ),
        ctx=pipeline.ctx,
    )


def run_post_write_maintenance(
    pipeline: WritePipeline,
    *,
    spec: DeltaWriteSpec,
    delta_version: int,
    initial_version: int | None,
    write_report: DeltaOperationReport | None,
) -> None:
    """Resolve and run Delta maintenance policy after writes."""
    if pipeline.runtime_profile is None:
        return
    from datafusion_engine.delta.maintenance import (
        DeltaMaintenancePlanInput,
        WriteOutcomeMetrics,
        build_write_outcome_metrics,
        maintenance_decision_artifact_payload,
        resolve_maintenance_from_execution,
        run_delta_maintenance,
    )
    from datafusion_engine.lineage.diagnostics import record_artifact
    from serde_artifact_specs import DELTA_MAINTENANCE_DECISION_SPEC

    metrics: WriteOutcomeMetrics | None = None
    if write_report is not None:
        metrics = build_write_outcome_metrics(
            write_report.to_payload(),
            initial_version=initial_version,
        )
        if metrics.final_version is None:
            metrics = WriteOutcomeMetrics(
                files_created=metrics.files_created,
                total_file_count=metrics.total_file_count,
                version_delta=metrics.version_delta,
                final_version=delta_version,
            )
    elif delta_version >= 0:
        metrics = WriteOutcomeMetrics(final_version=delta_version)

    plan_input = DeltaMaintenancePlanInput(
        dataset_location=spec.dataset_location,
        table_uri=spec.table_uri,
        dataset_name=spec.commit_key,
        storage_options=spec.storage_options,
        log_storage_options=spec.log_storage_options,
        delta_version=delta_version,
        delta_timestamp=None,
        feature_gate=spec.feature_gate,
        policy=spec.maintenance_policy,
    )
    decision = resolve_maintenance_from_execution(
        plan_input,
        metrics=metrics,
    )
    record_artifact(
        pipeline.runtime_profile,
        DELTA_MAINTENANCE_DECISION_SPEC,
        maintenance_decision_artifact_payload(
            decision,
            dataset_name=spec.commit_key,
        ),
    )
    plan = decision.plan
    if plan is None:
        return
    run_delta_maintenance(pipeline.ctx, plan=plan, runtime_profile=pipeline.runtime_profile)


def write_delta(
    pipeline: WritePipeline,
    result: StreamingExecutionResult,
    *,
    request: WriteRequest,
    spec: DeltaWriteSpec,
) -> DeltaWriteOutcome:
    """Write a Delta table using a deterministic write specification.

    Raises:
        DataFusionEngineError: If a committed write cannot resolve a Delta version.
        ValueError: If write mode is `ERROR` and destination already exists.

    Returns:
        DeltaWriteOutcome: Finalized Delta write outcome with feature metadata.
    """
    from datafusion_engine.delta.observability import DeltaOperationReport

    runtime_profile = _require_runtime_profile(
        pipeline.runtime_profile,
        operation="delta writes",
    )
    local_path = Path(spec.table_uri)
    delta_service = runtime_profile.delta_ops.delta_service()
    existing_version = delta_service.table_version(
        path=spec.table_uri,
        storage_options=spec.storage_options,
        log_storage_options=spec.log_storage_options,
    )
    if request.mode == WriteMode.ERROR and (local_path.exists() or existing_version is not None):
        msg = f"Delta destination already exists: {spec.table_uri}"
        raise ValueError(msg)
    _validate_delta_protocol_support(
        runtime_profile=pipeline.runtime_profile,
        delta_service=delta_service,
        table_uri=spec.table_uri,
        storage_options=spec.storage_options,
        log_storage_options=spec.log_storage_options,
        gate=spec.feature_gate,
        table_exists=existing_version is not None,
    )
    delta_result = write_delta_bootstrap(pipeline, result, spec=spec)
    feature_request = DeltaFeatureMutationRequest(
        path=spec.table_uri,
        storage_options=spec.storage_options,
        log_storage_options=spec.log_storage_options,
        commit_metadata=spec.commit_metadata,
        dataset_name=spec.commit_key,
        gate=spec.feature_gate,
    )
    feature_options = delta_service.features.feature_mutation_options(feature_request)
    enabled_features = delta_service.features.enable_features(
        feature_options,
        features=spec.table_properties,
    )
    _apply_explicit_delta_features(spec=spec, delta_service=delta_service)
    constraint_status = _apply_delta_check_constraints(spec=spec, delta_service=delta_service)
    record_delta_mutation(
        pipeline,
        spec=spec,
        delta_result=delta_result,
        operation="write",
        constraint_status=constraint_status,
    )
    if not enabled_features:
        enabled_features = dict(spec.table_properties)
    final_version = delta_service.table_version(
        path=spec.table_uri,
        storage_options=spec.storage_options,
        log_storage_options=spec.log_storage_options,
    )
    if final_version is None:
        if pipeline.runtime_profile is not None:
            from datafusion_engine.lineage.diagnostics import record_artifact
            from serde_artifact_specs import DELTA_WRITE_VERSION_MISSING_SPEC

            record_artifact(
                pipeline.runtime_profile,
                DELTA_WRITE_VERSION_MISSING_SPEC,
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "table_uri": spec.table_uri,
                    "mode": spec.mode,
                },
            )
        msg = (
            "Committed Delta write did not resolve a table version; "
            f"table_uri={spec.table_uri} mode={spec.mode}"
        )
        raise DataFusionEngineError(msg, kind=ErrorKind.DELTA)
    if pipeline.runtime_profile is not None:
        from datafusion_engine.delta.observability import (
            DeltaFeatureStateArtifact,
            record_delta_feature_state,
        )

        commit_run_id = spec.commit_run.run_id if spec.commit_run is not None else None
        record_delta_feature_state(
            pipeline.runtime_profile,
            artifact=DeltaFeatureStateArtifact(
                table_uri=spec.table_uri,
                enabled_features=enabled_features,
                dataset_name=spec.commit_key,
                delta_version=final_version,
                commit_metadata=spec.commit_metadata,
                commit_app_id=spec.commit_app_id,
                commit_version=spec.commit_version,
                commit_run_id=commit_run_id,
            ),
        )
    finalize_delta_commit(
        pipeline,
        DeltaCommitFinalizeContext(
            spec=spec,
            delta_version=final_version,
        ),
    )
    operation_report = DeltaOperationReport.from_payload(
        delta_result.report,
        operation="write",
        commit_metadata=spec.commit_metadata,
    )
    run_post_write_maintenance(
        pipeline,
        spec=spec,
        delta_version=final_version,
        initial_version=existing_version,
        write_report=operation_report,
    )
    canonical_uri = canonical_table_uri(spec.table_uri)
    return DeltaWriteOutcome(
        delta_result=DeltaWriteResult(
            path=canonical_uri,
            version=final_version,
            report=delta_result.report,
            snapshot_key=snapshot_key_for_table(spec.table_uri, final_version),
        ),
        enabled_features=enabled_features,
        commit_app_id=spec.commit_app_id,
        commit_version=spec.commit_version,
    )


def write_delta_bootstrap(
    pipeline: WritePipeline,
    result: StreamingExecutionResult,
    *,
    spec: DeltaWriteSpec,
) -> DeltaWriteResult:
    """Write initial Delta payload and return bootstrap result.

    Returns:
        DeltaWriteResult: Bootstrap write result before final version resolution.
    """
    from datafusion_engine.delta.control_plane_core import DeltaCommitOptions
    from datafusion_engine.delta.transactions import write_transaction
    from datafusion_engine.delta.write_ipc_payload import (
        DeltaWriteRequestOptions,
        build_delta_write_request,
    )
    from datafusion_engine.lineage.diagnostics import record_artifact
    from serde_artifact_specs import DELTA_WRITE_BOOTSTRAP_SPEC
    from storage.deltalake.delta_runtime_ops import commit_metadata_from_properties
    from utils.storage_options import merged_storage_options

    table = result.df.to_arrow_table()
    storage = merged_storage_options(spec.storage_options, spec.log_storage_options)
    partition_by = list(spec.partition_by) if spec.partition_by else None
    storage_options = dict(storage) if storage else None
    commit_options = DeltaCommitOptions(
        metadata=commit_metadata_from_properties(spec.commit_properties),
        app_transaction=None,
    )
    request = build_delta_write_request(
        table_uri=spec.table_uri,
        table=table,
        options=DeltaWriteRequestOptions(
            mode=spec.mode,
            schema_mode=spec.schema_mode,
            storage_options=storage_options,
            partition_columns=partition_by,
            target_file_size=spec.target_file_size,
            extra_constraints=spec.extra_constraints,
            commit_options=commit_options,
        ),
    )
    report = write_transaction(pipeline.ctx, request=request)
    if pipeline.runtime_profile is not None:
        row_count = int(table.num_rows)
        record_artifact(
            pipeline.runtime_profile,
            DELTA_WRITE_BOOTSTRAP_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table_uri": spec.table_uri,
                "mode": spec.mode,
                "row_count": row_count,
            },
        )
    return DeltaWriteResult(
        path=canonical_table_uri(spec.table_uri),
        version=None,
        report=report,
    )


def delta_insert_table_name(spec: DeltaWriteSpec) -> str:
    """Return deterministic temporary insert table name for Delta writes."""
    base = spec.commit_key or "delta_write"
    normalized = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in base)
    digest = hash_sha256_hex(spec.table_uri.encode("utf-8"))[:8]
    return f"{normalized}_{digest}"


@dataclass(frozen=True, slots=True)
class DeltaCommitFinalizeContext:
    """Commit-finalization context for one Delta write operation."""

    spec: DeltaWriteSpec
    delta_version: int
    duration_ms: float | None = None
    row_count: int | None = None
    status: str = "success"
    error: str | None = None


def register_delta_insert_target(
    pipeline: WritePipeline, spec: DeltaWriteSpec, *, table_name: str
) -> None:
    """Register table provider for a Delta insert target."""
    from datafusion_engine.dataset.resolution import (
        DatasetResolutionRequest,
        resolve_dataset_provider,
    )
    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.tables.metadata import TableProviderCapsule

    location = spec.dataset_location
    if location is None:
        overrides = None
        if spec.feature_gate is not None:
            from datafusion_engine.dataset.registry import DatasetLocation, DatasetLocationOverrides
            from schema_spec.dataset_spec import DeltaPolicyBundle

            overrides = DatasetLocationOverrides(
                delta=DeltaPolicyBundle(feature_gate=spec.feature_gate)
            )
        else:
            from datafusion_engine.dataset.registry import DatasetLocation

        location = DatasetLocation(
            path=spec.table_uri,
            format="delta",
            storage_options=dict(spec.storage_options or {}),
            delta_log_storage_options=dict(spec.log_storage_options or {}),
            overrides=overrides,
        )
    if pipeline.runtime_profile is not None:
        from datafusion_engine.delta.store_policy import apply_delta_store_policy

        location = apply_delta_store_policy(
            location, policy=pipeline.runtime_profile.policies.delta_store_policy
        )
    resolution = resolve_dataset_provider(
        DatasetResolutionRequest(
            ctx=pipeline.ctx,
            location=location,
            runtime_profile=pipeline.runtime_profile,
            name=table_name,
        )
    )
    adapter = DataFusionIOAdapter(ctx=pipeline.ctx, profile=pipeline.runtime_profile)
    adapter.register_table(
        table_name,
        TableProviderCapsule(resolution.provider),
        overwrite=True,
    )


__all__ = [
    "DeltaCommitFinalizeContext",
    "DeltaWriteHandler",
    "delta_insert_table_name",
    "delta_write_spec",
    "finalize_delta_commit",
    "prepare_commit_metadata",
    "record_delta_mutation",
    "register_delta_insert_target",
    "reserve_runtime_commit",
    "run_post_write_maintenance",
    "write_delta",
    "write_delta_bootstrap",
]
