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
from storage.deltalake import DeltaWriteOptions, idempotent_commit_properties, write_delta_table

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.delta_protocol import DeltaFeatureGate
    from datafusion_engine.runtime import DataFusionRuntimeProfile


DELTA_SNAPSHOT_TABLE_NAME = "datafusion_delta_snapshots_v1"
DELTA_MUTATION_TABLE_NAME = "datafusion_delta_mutations_v1"
DELTA_SCAN_PLAN_TABLE_NAME = "datafusion_delta_scan_plans_v1"
DELTA_MAINTENANCE_TABLE_NAME = "datafusion_delta_maintenance_v1"

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
    delta_protocol: Mapping[str, object] | None
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
        "reader_features_json": _json_text(snapshot.get("reader_features") or ()),
        "writer_features_json": _json_text(snapshot.get("writer_features") or ()),
        "table_properties_json": _json_text(snapshot.get("table_properties") or {}),
        "schema_json": str(snapshot.get("schema_json") or ""),
        "partition_columns_json": _json_text(snapshot.get("partition_columns") or ()),
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
    snapshot = report.get("snapshot") if isinstance(report, Mapping) else None
    if isinstance(snapshot, Mapping):
        snapshot_payload = {str(key): value for key, value in dict(snapshot).items()}
    else:
        snapshot_payload: dict[str, object] = {}
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "operation": artifact.operation,
        "mode": artifact.mode,
        "delta_version": _coerce_int(report.get("version")),
        "min_reader_version": _coerce_int(snapshot_payload.get("min_reader_version")),
        "min_writer_version": _coerce_int(snapshot_payload.get("min_writer_version")),
        "reader_features_json": _json_text(snapshot_payload.get("reader_features") or ()),
        "writer_features_json": _json_text(snapshot_payload.get("writer_features") or ()),
        "constraint_status": artifact.constraint_status,
        "constraint_violations_json": _json_text(list(artifact.constraint_violations)),
        "commit_app_id": artifact.commit_app_id,
        "commit_version": artifact.commit_version,
        "commit_run_id": artifact.commit_run_id,
        "commit_metadata_json": _json_text(dict(artifact.commit_metadata or {})),
        "metrics_json": _json_text(report.get("metrics") or {}),
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
        "pushed_filters_json": _json_text(list(artifact.pushed_filters)),
        "projected_columns_json": _json_text(list(artifact.projected_columns)),
        "delta_protocol_json": _json_text(artifact.delta_protocol or {}),
        "delta_feature_gate_json": _json_text(_gate_payload(artifact.delta_feature_gate)),
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
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset_name": artifact.dataset_name,
        "table_uri": artifact.table_uri,
        "operation": artifact.operation,
        "delta_version": _coerce_int(report.get("version")),
        "retention_hours": artifact.retention_hours,
        "dry_run": artifact.dry_run,
        "metrics_json": _json_text(report.get("metrics") or {}),
        "commit_metadata_json": _json_text(dict(artifact.commit_metadata or {})),
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
            pa.field("reader_features_json", pa.string()),
            pa.field("writer_features_json", pa.string()),
            pa.field("table_properties_json", pa.string()),
            pa.field("schema_json", pa.string()),
            pa.field("partition_columns_json", pa.string()),
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
            pa.field("reader_features_json", pa.string()),
            pa.field("writer_features_json", pa.string()),
            pa.field("constraint_status", pa.string()),
            pa.field("constraint_violations_json", pa.string()),
            pa.field("commit_app_id", pa.string()),
            pa.field("commit_version", pa.int64()),
            pa.field("commit_run_id", pa.string()),
            pa.field("commit_metadata_json", pa.string()),
            pa.field("metrics_json", pa.string()),
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
            pa.field("pushed_filters_json", pa.string()),
            pa.field("projected_columns_json", pa.string()),
            pa.field("delta_protocol_json", pa.string()),
            pa.field("delta_feature_gate_json", pa.string()),
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
            pa.field("retention_hours", pa.int64()),
            pa.field("dry_run", pa.bool_()),
            pa.field("metrics_json", pa.string()),
            pa.field("commit_metadata_json", pa.string()),
            pa.field("storage_options_hash", pa.string()),
        ]
    )


def _json_text(value: object) -> str:
    return json.dumps(_coerce_json_value(value), sort_keys=True)


def _coerce_json_value(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _coerce_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_coerce_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_coerce_json_value(item) for item in value]
    if value is None or isinstance(value, (bool, float, int, str)):
        return value
    return str(value)


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


def _gate_payload(gate: DeltaFeatureGate | None) -> dict[str, object] | None:
    if gate is None:
        return None
    return {
        "min_reader_version": gate.min_reader_version,
        "min_writer_version": gate.min_writer_version,
        "required_reader_features": list(gate.required_reader_features),
        "required_writer_features": list(gate.required_writer_features),
    }


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
