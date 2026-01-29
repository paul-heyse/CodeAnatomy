"""Delta observability tables and recording helpers."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.dataset_registry import DatasetLocation
from datafusion_engine.registry_bridge import register_dataset_df
from serde_msgspec import dumps_msgpack, to_builtins
from storage.deltalake import DeltaWriteOptions, idempotent_commit_properties, write_delta_table

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.delta_protocol import DeltaFeatureGate, DeltaProtocolSnapshot
    from datafusion_engine.runtime import DataFusionRuntimeProfile


DELTA_SNAPSHOT_TABLE_NAME = "datafusion_delta_snapshots_v2"
DELTA_MUTATION_TABLE_NAME = "datafusion_delta_mutations_v2"
DELTA_SCAN_PLAN_TABLE_NAME = "datafusion_delta_scan_plans_v2"
DELTA_MAINTENANCE_TABLE_NAME = "datafusion_delta_maintenance_v2"

try:
    _DEFAULT_OBSERVABILITY_ROOT = Path(__file__).resolve().parents[2] / ".artifacts"
except IndexError:
    _DEFAULT_OBSERVABILITY_ROOT = Path.cwd() / ".artifacts"


@dataclass(frozen=True)
class DeltaSnapshotArtifact:
    """Snapshot artifact payload for Delta tables."""

    table_uri: str
    snapshot: Mapping[str, object]
    dataset_name: str | None = None
    storage_options_hash: str | None = None


@dataclass(frozen=True)
class DeltaMutationArtifact:
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
    storage_options_hash: str | None = None


@dataclass(frozen=True)
class DeltaScanPlanArtifact:
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
    delta_feature_gate: DeltaFeatureGate | None
    storage_options_hash: str | None = None


@dataclass(frozen=True)
class DeltaMaintenanceArtifact:
    """Maintenance artifact payload for Delta tables."""

    table_uri: str
    operation: str
    report: Mapping[str, object]
    dataset_name: str | None = None
    retention_hours: int | None = None
    dry_run: bool | None = None
    storage_options_hash: str | None = None
    commit_metadata: Mapping[str, str] | None = None


@dataclass(frozen=True)
class _AppendObservabilityRequest:
    """Inputs required to append a Delta observability row."""

    ctx: SessionContext
    location: DatasetLocation
    schema: pa.Schema
    payload: Mapping[str, object]
    operation: str
    commit_metadata: Mapping[str, str] | None


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
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "delta_version": _coerce_int(snapshot.get("version")),
        "snapshot_timestamp": _coerce_int(snapshot.get("snapshot_timestamp")),
        "min_reader_version": _coerce_int(snapshot.get("min_reader_version")),
        "min_writer_version": _coerce_int(snapshot.get("min_writer_version")),
        "reader_features": _string_list(snapshot.get("reader_features") or ()),
        "writer_features": _string_list(snapshot.get("writer_features") or ()),
        "table_properties": _string_map(snapshot.get("table_properties") or {}),
        "schema_msgpack": _msgpack_payload(_schema_payload(snapshot.get("schema_json"))),
        "partition_columns": _string_list(snapshot.get("partition_columns") or ()),
        "storage_options_hash": artifact.storage_options_hash,
    }
    return _append_observability_row(
        _AppendObservabilityRequest(
            ctx=ctx,
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
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "operation": artifact.operation,
        "mode": artifact.mode,
        "delta_version": _coerce_int(report.get("version")),
        "min_reader_version": _coerce_int(snapshot_payload.get("min_reader_version")),
        "min_writer_version": _coerce_int(snapshot_payload.get("min_writer_version")),
        "reader_features": _string_list(snapshot_payload.get("reader_features") or ()),
        "writer_features": _string_list(snapshot_payload.get("writer_features") or ()),
        "table_properties": table_properties,
        "constraint_status": artifact.constraint_status,
        "constraint_violations": _string_list(artifact.constraint_violations),
        "commit_app_id": artifact.commit_app_id,
        "commit_version": artifact.commit_version,
        "commit_run_id": artifact.commit_run_id,
        "commit_metadata": _string_map(dict(artifact.commit_metadata or {})),
        "metrics_msgpack": _msgpack_payload(report.get("metrics") or {}),
        "storage_options_hash": artifact.storage_options_hash,
    }
    return _append_observability_row(
        _AppendObservabilityRequest(
            ctx=ctx,
            location=location,
            schema=_delta_mutation_schema(),
            payload=payload,
            operation=f"delta_mutation_{artifact.operation}",
            commit_metadata=artifact.commit_metadata,
        )
    )


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
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "delta_version": artifact.delta_version,
        "snapshot_timestamp": artifact.snapshot_timestamp,
        "total_files": artifact.total_files,
        "candidate_files": artifact.candidate_files,
        "pruned_files": artifact.pruned_files,
        "pushed_filters": _string_list(artifact.pushed_filters),
        "projected_columns": _string_list(artifact.projected_columns),
        "delta_protocol_msgpack": _msgpack_or_none(artifact.delta_protocol),
        "delta_feature_gate_msgpack": _msgpack_or_none(artifact.delta_feature_gate),
        "storage_options_hash": artifact.storage_options_hash,
    }
    return _append_observability_row(
        _AppendObservabilityRequest(
            ctx=ctx,
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
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "operation": artifact.operation,
        "delta_version": _coerce_int(report.get("version")),
        "min_reader_version": _coerce_int(snapshot_payload.get("min_reader_version")),
        "min_writer_version": _coerce_int(snapshot_payload.get("min_writer_version")),
        "reader_features": _string_list(snapshot_payload.get("reader_features") or ()),
        "writer_features": _string_list(snapshot_payload.get("writer_features") or ()),
        "table_properties": table_properties,
        "log_retention_duration": log_retention,
        "checkpoint_interval": checkpoint_interval,
        "checkpoint_retention_duration": checkpoint_retention,
        "checkpoint_protection": checkpoint_protection,
        "retention_hours": artifact.retention_hours,
        "dry_run": artifact.dry_run,
        "metrics_msgpack": _msgpack_payload(report.get("metrics") or {}),
        "commit_metadata": _string_map(dict(artifact.commit_metadata or {})),
        "storage_options_hash": artifact.storage_options_hash,
    }
    return _append_observability_row(
        _AppendObservabilityRequest(
            ctx=ctx,
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
    table_path = _observability_root(profile) / name
    if not table_path.exists():
        _bootstrap_observability_table(
            ctx,
            profile,
            table_path=table_path,
            schema=schema,
            operation="delta_observability_bootstrap",
        )
    location = DatasetLocation(path=str(table_path), format="delta")
    register_dataset_df(ctx, name=name, location=location, runtime_profile=profile)
    return location


def _bootstrap_observability_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    table_path: Path,
    schema: pa.Schema,
    operation: str,
) -> None:
    table_path.parent.mkdir(parents=True, exist_ok=True)
    empty = pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in schema],
        schema=schema,
    )
    options = DeltaWriteOptions(
        mode="overwrite",
        schema_mode="overwrite",
        commit_properties=idempotent_commit_properties(
            operation=operation,
            mode="overwrite",
            extra_metadata={"table": table_path.name},
        ),
        commit_metadata={"operation": operation, "table": table_path.name},
    )
    _ = write_delta_table(empty, str(table_path), options=options, ctx=ctx)
    _ = profile


def _append_observability_row(request: _AppendObservabilityRequest) -> int | None:
    table = pa.Table.from_pylist([dict(request.payload)], schema=request.schema)
    options = DeltaWriteOptions(
        mode="append",
        schema_mode="merge",
        commit_properties=idempotent_commit_properties(
            operation=request.operation,
            mode="append",
            extra_metadata={"operation": request.operation},
        ),
        commit_metadata={"operation": request.operation, **(request.commit_metadata or {})},
    )
    result = write_delta_table(table, str(request.location.path), options=options, ctx=request.ctx)
    return result.version


def _observability_root(profile: DataFusionRuntimeProfile) -> Path:
    root_value = profile.plan_artifacts_root
    root = Path(root_value) if root_value else _DEFAULT_OBSERVABILITY_ROOT
    return root / "delta_observability"


def _delta_snapshot_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("dataset_name", pa.string()),
            pa.field("table_uri", pa.string(), nullable=False),
            pa.field("delta_version", pa.int64()),
            pa.field("snapshot_timestamp", pa.int64()),
            pa.field("min_reader_version", pa.int32()),
            pa.field("min_writer_version", pa.int32()),
            pa.field("reader_features", pa.list_(pa.string())),
            pa.field("writer_features", pa.list_(pa.string())),
            pa.field("table_properties", pa.map_(pa.string(), pa.string())),
            pa.field("schema_msgpack", pa.binary(), nullable=False),
            pa.field("partition_columns", pa.list_(pa.string())),
            pa.field("storage_options_hash", pa.string()),
        ]
    )


def _delta_mutation_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("dataset_name", pa.string()),
            pa.field("table_uri", pa.string(), nullable=False),
            pa.field("operation", pa.string(), nullable=False),
            pa.field("mode", pa.string()),
            pa.field("delta_version", pa.int64()),
            pa.field("min_reader_version", pa.int32()),
            pa.field("min_writer_version", pa.int32()),
            pa.field("reader_features", pa.list_(pa.string())),
            pa.field("writer_features", pa.list_(pa.string())),
            pa.field("table_properties", pa.map_(pa.string(), pa.string())),
            pa.field("constraint_status", pa.string()),
            pa.field("constraint_violations", pa.list_(pa.string())),
            pa.field("commit_app_id", pa.string()),
            pa.field("commit_version", pa.int64()),
            pa.field("commit_run_id", pa.string()),
            pa.field("commit_metadata", pa.map_(pa.string(), pa.string())),
            pa.field("metrics_msgpack", pa.binary(), nullable=False),
            pa.field("storage_options_hash", pa.string()),
        ]
    )


def _delta_scan_plan_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("dataset_name", pa.string(), nullable=False),
            pa.field("table_uri", pa.string(), nullable=False),
            pa.field("delta_version", pa.int64()),
            pa.field("snapshot_timestamp", pa.int64()),
            pa.field("total_files", pa.int64(), nullable=False),
            pa.field("candidate_files", pa.int64(), nullable=False),
            pa.field("pruned_files", pa.int64(), nullable=False),
            pa.field("pushed_filters", pa.list_(pa.string())),
            pa.field("projected_columns", pa.list_(pa.string())),
            pa.field("delta_protocol_msgpack", pa.binary(), nullable=True),
            pa.field("delta_feature_gate_msgpack", pa.binary(), nullable=True),
            pa.field("storage_options_hash", pa.string()),
        ]
    )


def _delta_maintenance_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("dataset_name", pa.string()),
            pa.field("table_uri", pa.string(), nullable=False),
            pa.field("operation", pa.string(), nullable=False),
            pa.field("delta_version", pa.int64()),
            pa.field("min_reader_version", pa.int32()),
            pa.field("min_writer_version", pa.int32()),
            pa.field("reader_features", pa.list_(pa.string())),
            pa.field("writer_features", pa.list_(pa.string())),
            pa.field("table_properties", pa.map_(pa.string(), pa.string())),
            pa.field("log_retention_duration", pa.string()),
            pa.field("checkpoint_interval", pa.string()),
            pa.field("checkpoint_retention_duration", pa.string()),
            pa.field("checkpoint_protection", pa.string()),
            pa.field("retention_hours", pa.int64()),
            pa.field("dry_run", pa.bool_()),
            pa.field("metrics_msgpack", pa.binary(), nullable=False),
            pa.field("commit_metadata", pa.map_(pa.string(), pa.string())),
            pa.field("storage_options_hash", pa.string()),
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


def _msgpack_payload(value: object) -> bytes:
    return dumps_msgpack(to_builtins(value, str_keys=True))


def _msgpack_or_none(value: object | None) -> bytes | None:
    if value is None:
        return None
    return _msgpack_payload(value)


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, memoryview)):
        return [str(item) for item in value]
    return [str(value)]


def _string_map(value: object) -> dict[str, str]:
    if isinstance(value, Mapping):
        return {str(key): str(item) for key, item in value.items()}
    return {}


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


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return None
    return None


__all__ = [
    "DELTA_MAINTENANCE_TABLE_NAME",
    "DELTA_MUTATION_TABLE_NAME",
    "DELTA_SCAN_PLAN_TABLE_NAME",
    "DELTA_SNAPSHOT_TABLE_NAME",
    "DeltaMaintenanceArtifact",
    "DeltaMutationArtifact",
    "DeltaScanPlanArtifact",
    "DeltaSnapshotArtifact",
    "record_delta_maintenance",
    "record_delta_mutation",
    "record_delta_scan_plan",
    "record_delta_snapshot",
]
