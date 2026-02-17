"""Delta observability tables and recording helpers."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec
import pyarrow as pa
from opentelemetry import trace

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.delta.obs_schemas import (
    delta_maintenance_schema,
    delta_mutation_schema,
    delta_scan_plan_schema,
    delta_snapshot_schema,
)
from datafusion_engine.delta.obs_table_manager import (
    DELTA_MAINTENANCE_TABLE_NAME,
    DELTA_MUTATION_TABLE_NAME,
    DELTA_SCAN_PLAN_TABLE_NAME,
    DELTA_SNAPSHOT_TABLE_NAME,
    ensure_delta_observability_tables,
    ensure_observability_schema,
    ensure_observability_table,
    is_observability_target,
    is_schema_mismatch_error,
    observability_commit_metadata,
)
from datafusion_engine.delta.payload import (
    msgpack_or_none,
    msgpack_payload,
    string_list,
    string_map,
)
from datafusion_engine.errors import DataFusionEngineError
from datafusion_engine.io.ingest import datafusion_from_arrow
from datafusion_engine.io.write_core import (
    WriteFormat,
    WriteMode,
    WritePipeline,
    WriteRequest,
)
from obs.otel import get_run_id
from serde_msgspec import StructBaseStrict
from utils.value_coercion import coerce_int

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.delta.protocol import DeltaProtocolSnapshot
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

_DELTA_TRACING_LOCK = threading.Lock()
_DELTA_TRACING_READY = threading.Event()
_LOGGER = logging.getLogger(__name__)
_OBSERVABILITY_APPEND_ERRORS = (
    DataFusionEngineError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
    KeyError,
    ImportError,
    AttributeError,
)


def ensure_delta_tracing() -> tuple[bool, str | None]:
    """Initialize delta-rs tracing once per process.

    Returns:
    -------
    tuple[bool, str | None]
        (installed, error) result.
    """
    if _DELTA_TRACING_READY.is_set():
        return True, None
    with _DELTA_TRACING_LOCK:
        if _DELTA_TRACING_READY.is_set():
            return True, None
        try:
            from deltalake import init_tracing
        except ImportError as exc:  # pragma: no cover - optional dependency
            return False, str(exc)
        try:
            init_tracing()
        except (RuntimeError, TypeError, ValueError) as exc:  # pragma: no cover - env-dependent
            _LOGGER.debug("Delta tracing init failed: %s", exc)
            return False, str(exc)
        _DELTA_TRACING_READY.set()
        return True, None


class DeltaSnapshotArtifact(StructBaseStrict, frozen=True):
    """Snapshot artifact payload for Delta tables."""

    table_uri: str
    snapshot: Mapping[str, object]
    dataset_name: str | None = None
    schema_identity_hash: str | None = None
    ddl_fingerprint: str | None = None


class DeltaMutationArtifact(StructBaseStrict, frozen=True):
    """Mutation artifact payload for Delta tables."""

    table_uri: str
    operation: str
    report: Mapping[str, object]
    dataset_name: str | None = None
    mode: str | None = None
    commit_metadata: Mapping[str, str] | None = None
    commit_app_id: str | None = None
    commit_version: int | None = None
    commit_run_id: str | None = None
    constraint_status: str | None = None
    constraint_violations: Sequence[str] = ()


class DeltaOperationReport(StructBaseStrict, frozen=True):
    """Structured Delta operation report payload."""

    rows_affected: int | None = None
    version: int | None = None
    operation: str | None = None
    duration_ms: float | None = None
    commit_metadata: Mapping[str, str] = msgspec.field(default_factory=dict)
    payload: Mapping[str, object] = msgspec.field(default_factory=dict)

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, object] | None,
        *,
        operation: str | None = None,
        commit_metadata: Mapping[str, str] | None = None,
    ) -> DeltaOperationReport:
        """Build a normalized operation report from an arbitrary payload.

        Returns:
        -------
        DeltaOperationReport
            Structured operation report with normalized counters and metadata.
        """
        resolved = dict(payload or {})
        rows_affected = coerce_int(
            resolved.get("rows_affected")
            or resolved.get("rows_written")
            or resolved.get("num_output_rows")
        )
        version = coerce_int(resolved.get("version") or resolved.get("delta_version"))
        duration_value = resolved.get("duration_ms")
        duration_ms = float(duration_value) if isinstance(duration_value, (int, float)) else None
        return cls(
            rows_affected=rows_affected,
            version=version,
            operation=operation,
            duration_ms=duration_ms,
            commit_metadata=dict(commit_metadata or {}),
            payload=resolved,
        )

    def to_payload(self) -> dict[str, object]:
        """Serialize the report to a JSON-friendly payload.

        Returns:
        -------
        dict[str, object]
            Report payload with canonical top-level operation fields.
        """
        payload = dict(self.payload)
        payload.setdefault("rows_affected", self.rows_affected)
        payload.setdefault("version", self.version)
        payload.setdefault("operation", self.operation)
        payload.setdefault("duration_ms", self.duration_ms)
        if self.commit_metadata:
            payload.setdefault("commit_metadata", dict(self.commit_metadata))
        return payload


class DeltaScanPlanArtifact(StructBaseStrict, frozen=True):
    """Scan planning artifact payload for Delta tables."""

    dataset_name: str
    table_uri: str
    delta_version: int | None
    snapshot_timestamp: int | None
    total_files: int
    candidate_files: int
    pruned_files: int
    pushed_filters: Sequence[str]
    projected_columns: Sequence[str]
    delta_protocol: DeltaProtocolSnapshot | None


class DeltaMaintenanceArtifact(StructBaseStrict, frozen=True):
    """Maintenance artifact payload for Delta tables (optimize/vacuum/checkpoint)."""

    table_uri: str
    operation: str
    report: Mapping[str, object]
    dataset_name: str | None = None
    retention_hours: int | None = None
    dry_run: bool | None = None
    commit_metadata: Mapping[str, str] | None = None


class DeltaFeatureStateArtifact(StructBaseStrict, frozen=True):
    """Feature state artifact payload for Delta tables."""

    table_uri: str
    enabled_features: Mapping[str, str]
    dataset_name: str | None = None
    delta_version: int | None = None
    commit_metadata: Mapping[str, str] | None = None
    commit_app_id: str | None = None
    commit_version: int | None = None
    commit_run_id: str | None = None


class _AppendObservabilityRequest(StructBaseStrict, frozen=True):
    """Inputs required to append a Delta observability row."""

    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile | None
    location: DatasetLocation
    schema: pa.Schema
    payload: Mapping[str, object]
    operation: str
    commit_metadata: Mapping[str, str] | None


def _trace_span_ids() -> tuple[str | None, str | None]:
    span = trace.get_current_span()
    if span is None:
        return None, None
    context = span.get_span_context()
    if context is None or not context.is_valid:
        return None, None
    return f"{context.trace_id:032x}", f"{context.span_id:016x}"


def record_delta_snapshot(
    profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaSnapshotArtifact,
    ctx: SessionContext | None = None,
) -> int | None:
    """Persist a Delta snapshot artifact row when enabled.

    Returns:
    -------
    int | None
        Delta table version for the write, or ``None`` when disabled.
    """
    if profile is None:
        return None
    active_ctx = ctx or profile.session_context()
    location = ensure_observability_table(
        active_ctx,
        profile,
        name=DELTA_SNAPSHOT_TABLE_NAME,
        schema=delta_snapshot_schema(),
    )
    if location is None:
        return None
    snapshot = artifact.snapshot
    run_id = get_run_id()
    trace_id, span_id = _trace_span_ids()
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "run_id": run_id,
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "trace_id": trace_id,
        "span_id": span_id,
        "delta_version": coerce_int(snapshot.get("version")),
        "snapshot_timestamp": coerce_int(snapshot.get("snapshot_timestamp")),
        "min_reader_version": coerce_int(snapshot.get("min_reader_version")),
        "min_writer_version": coerce_int(snapshot.get("min_writer_version")),
        "reader_features": string_list(snapshot.get("reader_features") or ()),
        "writer_features": string_list(snapshot.get("writer_features") or ()),
        "table_properties": string_map(snapshot.get("table_properties") or {}),
        "schema_msgpack": msgpack_payload(_schema_payload(snapshot.get("schema_json"))),
        "schema_identity_hash": artifact.schema_identity_hash,
        "ddl_fingerprint": artifact.ddl_fingerprint,
        "partition_columns": string_list(snapshot.get("partition_columns") or ()),
    }
    return _append_observability_row(
        _AppendObservabilityRequest(
            ctx=active_ctx,
            runtime_profile=profile,
            location=location,
            schema=delta_snapshot_schema(),
            payload=payload,
            operation="delta_snapshot_artifact",
            commit_metadata={"table_uri": artifact.table_uri},
        )
    )


def record_delta_mutation(
    profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaMutationArtifact,
    ctx: SessionContext | None = None,
) -> int | None:
    """Persist a Delta mutation artifact row when enabled.

    Returns:
    -------
    int | None
        Delta table version for the write, or ``None`` when disabled.
    """
    if profile is None:
        return None
    if is_observability_target(
        dataset_name=artifact.dataset_name,
        table_uri=artifact.table_uri,
    ):
        return None
    active_ctx = ctx or profile.session_context()
    location = ensure_observability_table(
        active_ctx,
        profile,
        name=DELTA_MUTATION_TABLE_NAME,
        schema=delta_mutation_schema(),
    )
    if location is None:
        return None
    report = artifact.report
    snapshot_payload = _snapshot_payload(report)
    table_properties = _snapshot_table_properties(snapshot_payload)
    run_id = get_run_id()
    trace_id, span_id = _trace_span_ids()
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "run_id": run_id,
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "trace_id": trace_id,
        "span_id": span_id,
        "operation": artifact.operation,
        "mode": artifact.mode,
        "delta_version": coerce_int(report.get("version")),
        "min_reader_version": coerce_int(snapshot_payload.get("min_reader_version")),
        "min_writer_version": coerce_int(snapshot_payload.get("min_writer_version")),
        "reader_features": string_list(snapshot_payload.get("reader_features") or ()),
        "writer_features": string_list(snapshot_payload.get("writer_features") or ()),
        "table_properties": table_properties,
        "constraint_status": artifact.constraint_status,
        "constraint_violations": string_list(artifact.constraint_violations),
        "commit_app_id": artifact.commit_app_id,
        "commit_version": artifact.commit_version,
        "commit_run_id": artifact.commit_run_id,
        "commit_metadata": string_map(dict(artifact.commit_metadata or {})),
        "metrics_msgpack": msgpack_payload(report.get("metrics") or {}),
    }
    return _append_observability_row(
        _AppendObservabilityRequest(
            ctx=active_ctx,
            runtime_profile=profile,
            location=location,
            schema=delta_mutation_schema(),
            payload=payload,
            operation=f"delta_mutation_{artifact.operation}",
            commit_metadata=artifact.commit_metadata,
        )
    )


def record_delta_feature_state(
    profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaFeatureStateArtifact,
) -> None:
    """Record Delta feature state adoption in diagnostics."""
    if profile is None:
        return
    run_id = get_run_id()
    trace_id, span_id = _trace_span_ids()
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "run_id": run_id,
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "trace_id": trace_id,
        "span_id": span_id,
        "delta_version": artifact.delta_version,
        "enabled_features": string_map(dict(artifact.enabled_features)),
        "commit_metadata": string_map(dict(artifact.commit_metadata or {})),
        "commit_app_id": artifact.commit_app_id,
        "commit_version": artifact.commit_version,
        "commit_run_id": artifact.commit_run_id,
    }
    from serde_artifact_specs import DATAFUSION_DELTA_FEATURES_SPEC

    profile.record_artifact(DATAFUSION_DELTA_FEATURES_SPEC, payload)


def record_delta_scan_plan(
    profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaScanPlanArtifact,
) -> int | None:
    """Persist a Delta scan-plan artifact row when enabled.

    Returns:
    -------
    int | None
        Delta table version for the write, or ``None`` when disabled.
    """
    if profile is None:
        return None
    ctx = profile.session_context()
    location = ensure_observability_table(
        ctx,
        profile,
        name=DELTA_SCAN_PLAN_TABLE_NAME,
        schema=delta_scan_plan_schema(),
    )
    if location is None:
        return None
    run_id = get_run_id()
    trace_id, span_id = _trace_span_ids()
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "run_id": run_id,
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "trace_id": trace_id,
        "span_id": span_id,
        "delta_version": artifact.delta_version,
        "snapshot_timestamp": artifact.snapshot_timestamp,
        "total_files": artifact.total_files,
        "candidate_files": artifact.candidate_files,
        "pruned_files": artifact.pruned_files,
        "pushed_filters": string_list(artifact.pushed_filters),
        "projected_columns": string_list(artifact.projected_columns),
        "delta_protocol_msgpack": msgpack_or_none(artifact.delta_protocol),
    }
    return _append_observability_row(
        _AppendObservabilityRequest(
            ctx=ctx,
            runtime_profile=profile,
            location=location,
            schema=delta_scan_plan_schema(),
            payload=payload,
            operation="delta_scan_plan",
            commit_metadata={"dataset_name": artifact.dataset_name},
        )
    )


def record_delta_maintenance(
    profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaMaintenanceArtifact,
) -> int | None:
    """Persist a Delta maintenance artifact row when enabled.

    Returns:
    -------
    int | None
        Delta table version for the write, or ``None`` when disabled.
    """
    if profile is None:
        return None
    ctx = profile.session_context()
    location = ensure_observability_table(
        ctx,
        profile,
        name=DELTA_MAINTENANCE_TABLE_NAME,
        schema=delta_maintenance_schema(),
    )
    if location is None:
        return None
    report = artifact.report
    snapshot_payload = _snapshot_payload(report)
    table_properties = _snapshot_table_properties(snapshot_payload)
    log_retention = table_properties.get("delta.logRetentionDuration")
    checkpoint_interval = table_properties.get("delta.checkpointInterval")
    checkpoint_retention = table_properties.get("delta.checkpointRetentionDuration")
    checkpoint_protection = table_properties.get("delta.checkpointProtection")
    run_id = get_run_id()
    trace_id, span_id = _trace_span_ids()
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "run_id": run_id,
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "trace_id": trace_id,
        "span_id": span_id,
        "operation": artifact.operation,
        "delta_version": coerce_int(report.get("version")),
        "min_reader_version": coerce_int(snapshot_payload.get("min_reader_version")),
        "min_writer_version": coerce_int(snapshot_payload.get("min_writer_version")),
        "reader_features": string_list(snapshot_payload.get("reader_features") or ()),
        "writer_features": string_list(snapshot_payload.get("writer_features") or ()),
        "table_properties": table_properties,
        "log_retention_duration": log_retention,
        "checkpoint_interval": checkpoint_interval,
        "checkpoint_retention_duration": checkpoint_retention,
        "checkpoint_protection": checkpoint_protection,
        "retention_hours": artifact.retention_hours,
        "dry_run": artifact.dry_run,
        "metrics_msgpack": msgpack_payload(report.get("metrics") or {}),
        "commit_metadata": string_map(dict(artifact.commit_metadata or {})),
    }
    return _append_observability_row(
        _AppendObservabilityRequest(
            ctx=ctx,
            runtime_profile=profile,
            location=location,
            schema=delta_maintenance_schema(),
            payload=payload,
            operation=f"delta_maintenance_{artifact.operation}",
            commit_metadata=artifact.commit_metadata,
        )
    )


def _append_observability_row(request: _AppendObservabilityRequest) -> int | None:
    table = pa.Table.from_pylist([dict(request.payload)], schema=request.schema)
    pipeline = WritePipeline(ctx=request.ctx, runtime_profile=request.runtime_profile)
    commit_metadata_input = (
        dict(request.commit_metadata) if request.commit_metadata is not None else None
    )
    commit_metadata = observability_commit_metadata(
        request.operation,
        commit_metadata_input,
    )
    try:
        append_name = f"{request.location.path}_append_{time.time_ns()}"
        df = datafusion_from_arrow(request.ctx, name=append_name, value=table)
        result = pipeline.write(
            WriteRequest(
                source=df,
                destination=str(request.location.path),
                format=WriteFormat.DELTA,
                mode=WriteMode.APPEND,
                format_options={"commit_metadata": commit_metadata},
            )
        )
    except _OBSERVABILITY_APPEND_ERRORS as exc:  # pragma: no cover - defensive fail-open path
        if (
            request.runtime_profile is not None
            and is_schema_mismatch_error(exc)
            and ensure_observability_schema(
                request.ctx,
                request.runtime_profile,
                table_path=Path(str(request.location.path)),
                schema=request.schema,
                operation=f"{request.operation}_schema_repair",
            )
        ):
            retry_name = f"{request.location.path}_append_retry_{time.time_ns()}"
            retry_df = datafusion_from_arrow(request.ctx, name=retry_name, value=table)
            try:
                result = pipeline.write(
                    WriteRequest(
                        source=retry_df,
                        destination=str(request.location.path),
                        format=WriteFormat.DELTA,
                        mode=WriteMode.APPEND,
                        format_options={"commit_metadata": commit_metadata},
                    )
                )
            except (
                _OBSERVABILITY_APPEND_ERRORS
            ) as retry_exc:  # pragma: no cover - defensive fail-open path
                _record_append_failure(request, retry_exc)
                return None
        else:
            _record_append_failure(request, exc)
            return None
    if result.delta_result is None:
        return None
    return result.delta_result.version


def _record_append_failure(request: _AppendObservabilityRequest, exc: Exception) -> None:
    if request.runtime_profile is None:
        return
    from serde_artifact_specs import DELTA_OBSERVABILITY_APPEND_FAILED_SPEC

    request.runtime_profile.record_artifact(
        DELTA_OBSERVABILITY_APPEND_FAILED_SPEC,
        {
            "event_time_unix_ms": int(time.time() * 1000),
            "table": request.location.path,
            "operation": request.operation,
            "error": str(exc),
        },
    )


def _snapshot_payload(report: Mapping[str, object]) -> dict[str, object]:
    snapshot = report.get("snapshot") if isinstance(report, Mapping) else None
    if isinstance(snapshot, Mapping):
        return {str(key): value for key, value in dict(snapshot).items()}
    metrics = report.get("metrics") if isinstance(report, Mapping) else None
    if isinstance(metrics, Mapping):
        nested = metrics.get("snapshot")
        if isinstance(nested, Mapping):
            return {str(key): value for key, value in dict(nested).items()}
    return {}


def _snapshot_table_properties(snapshot_payload: Mapping[str, object]) -> dict[str, str]:
    properties = snapshot_payload.get("table_properties")
    if not isinstance(properties, Mapping):
        return {}
    return {str(key): str(value) for key, value in dict(properties).items()}


def _schema_payload(value: object) -> object:
    empty: dict[str, object] = {}
    if value is None:
        return empty
    if isinstance(value, str):
        if not value:
            return empty
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {"raw": value}
    return value


__all__ = [
    "DELTA_MAINTENANCE_TABLE_NAME",
    "DELTA_MUTATION_TABLE_NAME",
    "DELTA_SCAN_PLAN_TABLE_NAME",
    "DELTA_SNAPSHOT_TABLE_NAME",
    "DeltaFeatureStateArtifact",
    "DeltaMaintenanceArtifact",
    "DeltaMutationArtifact",
    "DeltaOperationReport",
    "DeltaScanPlanArtifact",
    "DeltaSnapshotArtifact",
    "ensure_delta_observability_tables",
    "record_delta_feature_state",
    "record_delta_maintenance",
    "record_delta_mutation",
    "record_delta_scan_plan",
    "record_delta_snapshot",
]
