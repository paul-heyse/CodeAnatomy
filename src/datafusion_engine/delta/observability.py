"""Delta observability tables and recording helpers."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
from opentelemetry import trace

from datafusion_engine.arrow.field_builders import (
    binary_field,
    bool_field,
    int32_field,
    int64_field,
    list_field,
    string_field,
)
from datafusion_engine.dataset.registration import (
    DatasetRegistrationOptions,
    register_dataset_df,
)
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.delta.payload import (
    msgpack_or_none,
    msgpack_payload,
    string_list,
    string_map,
)
from datafusion_engine.io.ingest import datafusion_from_arrow
from datafusion_engine.io.write import (
    WriteFormat,
    WriteMode,
    WritePipeline,
    WriteRequest,
)
from obs.otel.run_context import get_run_id
from serde_msgspec import StructBaseStrict
from utils.value_coercion import coerce_int

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.delta.protocol import DeltaProtocolSnapshot
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


DELTA_SNAPSHOT_TABLE_NAME = "datafusion_delta_snapshots_v2"
DELTA_MUTATION_TABLE_NAME = "datafusion_delta_mutations_v2"
DELTA_SCAN_PLAN_TABLE_NAME = "datafusion_delta_scan_plans_v2"
DELTA_MAINTENANCE_TABLE_NAME = "datafusion_delta_maintenance_v2"

try:
    _DEFAULT_OBSERVABILITY_ROOT = Path(__file__).resolve().parents[2] / ".artifacts"
except IndexError:
    _DEFAULT_OBSERVABILITY_ROOT = Path.cwd() / ".artifacts"

_DELTA_TRACING_LOCK = threading.Lock()
_DELTA_TRACING_READY = threading.Event()
_LOGGER = logging.getLogger(__name__)


def ensure_delta_tracing() -> tuple[bool, str | None]:
    """Initialize delta-rs tracing once per process.

    Returns
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
) -> int | None:
    """Persist a Delta snapshot artifact row when enabled.

    Returns
    -------
    int | None
        Delta table version for the write, or ``None`` when disabled.
    """
    if profile is None:
        return None
    ctx = profile.session_context()
    location = _ensure_observability_table(
        ctx,
        profile,
        name=DELTA_SNAPSHOT_TABLE_NAME,
        schema=_delta_snapshot_schema(),
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
            ctx=ctx,
            runtime_profile=profile,
            location=location,
            schema=_delta_snapshot_schema(),
            payload=payload,
            operation="delta_snapshot_artifact",
            commit_metadata={"table_uri": artifact.table_uri},
        )
    )


def record_delta_mutation(
    profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaMutationArtifact,
) -> int | None:
    """Persist a Delta mutation artifact row when enabled.

    Returns
    -------
    int | None
        Delta table version for the write, or ``None`` when disabled.
    """
    if profile is None:
        return None
    ctx = profile.session_context()
    location = _ensure_observability_table(
        ctx,
        profile,
        name=DELTA_MUTATION_TABLE_NAME,
        schema=_delta_mutation_schema(),
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
            ctx=ctx,
            runtime_profile=profile,
            location=location,
            schema=_delta_mutation_schema(),
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
    profile.record_artifact("datafusion_delta_features_v1", payload)


def record_delta_scan_plan(
    profile: DataFusionRuntimeProfile | None,
    *,
    artifact: DeltaScanPlanArtifact,
) -> int | None:
    """Persist a Delta scan-plan artifact row when enabled.

    Returns
    -------
    int | None
        Delta table version for the write, or ``None`` when disabled.
    """
    if profile is None:
        return None
    ctx = profile.session_context()
    location = _ensure_observability_table(
        ctx,
        profile,
        name=DELTA_SCAN_PLAN_TABLE_NAME,
        schema=_delta_scan_plan_schema(),
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
            schema=_delta_scan_plan_schema(),
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

    Returns
    -------
    int | None
        Delta table version for the write, or ``None`` when disabled.
    """
    if profile is None:
        return None
    ctx = profile.session_context()
    location = _ensure_observability_table(
        ctx,
        profile,
        name=DELTA_MAINTENANCE_TABLE_NAME,
        schema=_delta_maintenance_schema(),
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
            schema=_delta_maintenance_schema(),
            payload=payload,
            operation=f"delta_maintenance_{artifact.operation}",
            commit_metadata=artifact.commit_metadata,
        )
    )


def _ensure_observability_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    name: str,
    schema: pa.Schema,
) -> DatasetLocation | None:
    _ = schema
    table_path = _observability_root(profile) / name
    delta_log_path = table_path / "_delta_log"
    has_delta_log = delta_log_path.exists() and any(delta_log_path.glob("*.json"))
    if not has_delta_log:
        profile.record_artifact(
            "delta_observability_missing_v1",
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": name,
                "path": str(table_path),
                "error": "Delta log missing; observability table bootstrap skipped.",
            },
        )
        return None
    location = DatasetLocation(path=str(table_path), format="delta")
    try:
        register_dataset_df(
            ctx,
            name=name,
            location=location,
            options=DatasetRegistrationOptions(runtime_profile=profile),
        )
    except (RuntimeError, ValueError, TypeError, OSError, KeyError) as exc:
        profile.record_artifact(
            "delta_observability_register_failed_v1",
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": name,
                "path": str(table_path),
                "error": str(exc),
            },
        )
        return None
    return location


def _bootstrap_observability_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    table_path: Path,
    schema: pa.Schema,
    operation: str,
) -> None:
    _ = (ctx, profile)
    table_path.parent.mkdir(parents=True, exist_ok=True)
    bootstrap_row = _bootstrap_observability_row(schema)
    empty = pa.Table.from_pylist([bootstrap_row], schema=schema)
    from deltalake import CommitProperties
    from deltalake.writer import write_deltalake

    commit_properties = CommitProperties(
        custom_metadata={"operation": operation, "table": table_path.name},
    )
    write_deltalake(
        str(table_path),
        empty,
        mode="overwrite",
        schema_mode="overwrite",
        commit_properties=commit_properties,
    )


def _bootstrap_observability_row(schema: pa.Schema) -> dict[str, object]:
    row: dict[str, object] = {}
    for field in schema:
        if field.nullable:
            row[field.name] = None
            continue
        dtype = field.type
        if pa.types.is_string(dtype):
            row[field.name] = "__bootstrap__"
        elif pa.types.is_integer(dtype):
            row[field.name] = 0
        elif pa.types.is_binary(dtype):
            row[field.name] = b""
        elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
            row[field.name] = []
        elif pa.types.is_map(dtype):
            row[field.name] = {}
        else:
            row[field.name] = None
    return row


def _append_observability_row(request: _AppendObservabilityRequest) -> int | None:
    table = pa.Table.from_pylist([dict(request.payload)], schema=request.schema)
    df = datafusion_from_arrow(request.ctx, name=f"{request.location.path}_append", value=table)
    pipeline = WritePipeline(ctx=request.ctx, runtime_profile=request.runtime_profile)
    result = pipeline.write(
        WriteRequest(
            source=df,
            destination=str(request.location.path),
            format=WriteFormat.DELTA,
            mode=WriteMode.APPEND,
            format_options={
                "commit_metadata": {
                    "operation": request.operation,
                    **(request.commit_metadata or {}),
                }
            },
        )
    )
    if result.delta_result is None:
        return None
    return result.delta_result.version


def _observability_root(profile: DataFusionRuntimeProfile) -> Path:
    root_value = profile.policies.plan_artifacts_root
    root = Path(root_value) if root_value else _DEFAULT_OBSERVABILITY_ROOT
    return root / "delta_observability"


def _delta_snapshot_schema() -> pa.Schema:
    return pa.schema(
        [
            int64_field("event_time_unix_ms", nullable=False),
            string_field("run_id"),
            string_field("dataset_name"),
            string_field("table_uri", nullable=False),
            string_field("trace_id"),
            string_field("span_id"),
            int64_field("delta_version"),
            int64_field("snapshot_timestamp"),
            int32_field("min_reader_version"),
            int32_field("min_writer_version"),
            list_field("reader_features", pa.string()),
            list_field("writer_features", pa.string()),
            pa.field("table_properties", pa.map_(pa.string(), pa.string())),
            binary_field("schema_msgpack", nullable=False),
            string_field("schema_identity_hash"),
            string_field("ddl_fingerprint"),
            list_field("partition_columns", pa.string()),
        ]
    )


def _delta_mutation_schema() -> pa.Schema:
    return pa.schema(
        [
            int64_field("event_time_unix_ms", nullable=False),
            string_field("run_id"),
            string_field("dataset_name"),
            string_field("table_uri", nullable=False),
            string_field("trace_id"),
            string_field("span_id"),
            string_field("operation", nullable=False),
            string_field("mode"),
            int64_field("delta_version"),
            int32_field("min_reader_version"),
            int32_field("min_writer_version"),
            list_field("reader_features", pa.string()),
            list_field("writer_features", pa.string()),
            pa.field("table_properties", pa.map_(pa.string(), pa.string())),
            string_field("constraint_status"),
            list_field("constraint_violations", pa.string()),
            string_field("commit_app_id"),
            int64_field("commit_version"),
            string_field("commit_run_id"),
            pa.field("commit_metadata", pa.map_(pa.string(), pa.string())),
            binary_field("metrics_msgpack", nullable=False),
        ]
    )


def _delta_scan_plan_schema() -> pa.Schema:
    return pa.schema(
        [
            int64_field("event_time_unix_ms", nullable=False),
            string_field("run_id"),
            string_field("dataset_name", nullable=False),
            string_field("table_uri", nullable=False),
            string_field("trace_id"),
            string_field("span_id"),
            int64_field("delta_version"),
            int64_field("snapshot_timestamp"),
            int64_field("total_files", nullable=False),
            int64_field("candidate_files", nullable=False),
            int64_field("pruned_files", nullable=False),
            list_field("pushed_filters", pa.string()),
            list_field("projected_columns", pa.string()),
            binary_field("delta_protocol_msgpack"),
        ]
    )


def _delta_maintenance_schema() -> pa.Schema:
    return pa.schema(
        [
            int64_field("event_time_unix_ms", nullable=False),
            string_field("run_id"),
            string_field("dataset_name"),
            string_field("table_uri", nullable=False),
            string_field("trace_id"),
            string_field("span_id"),
            string_field("operation", nullable=False),
            int64_field("delta_version"),
            int32_field("min_reader_version"),
            int32_field("min_writer_version"),
            list_field("reader_features", pa.string()),
            list_field("writer_features", pa.string()),
            pa.field("table_properties", pa.map_(pa.string(), pa.string())),
            string_field("log_retention_duration"),
            string_field("checkpoint_interval"),
            string_field("checkpoint_retention_duration"),
            string_field("checkpoint_protection"),
            int64_field("retention_hours"),
            bool_field("dry_run"),
            binary_field("metrics_msgpack", nullable=False),
            pa.field("commit_metadata", pa.map_(pa.string(), pa.string())),
        ]
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
    "DeltaScanPlanArtifact",
    "DeltaSnapshotArtifact",
    "record_delta_feature_state",
    "record_delta_maintenance",
    "record_delta_mutation",
    "record_delta_scan_plan",
    "record_delta_snapshot",
]
