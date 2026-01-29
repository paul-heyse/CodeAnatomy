"""Engine-level helpers for Delta maintenance operations."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from datafusion_engine.arrow_interop import RecordBatchReaderLike
from storage.deltalake import (
    DeltaVacuumOptions,
    StorageOptions,
    delta_history_snapshot,
    delta_protocol_snapshot,
    delta_table_version,
    vacuum_delta,
)
from utils.hashing import hash_storage_options
from utils.storage_options import normalize_storage_options

_DELTA_MIN_RETENTION_HOURS = 168

if TYPE_CHECKING:
    from datafusion_engine.delta_protocol import DeltaProtocolSnapshot
    from datafusion_engine.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class DeltaHistorySnapshot:
    """Delta history snapshot with metadata."""

    path: str
    version: int | None
    history: Mapping[str, object] | None
    protocol: DeltaProtocolSnapshot | None


@dataclass(frozen=True)
class DeltaVacuumResult:
    """Result of a Delta vacuum operation."""

    path: str
    removed_files: Sequence[str]
    dry_run: bool
    retention_hours: int | None


@dataclass(frozen=True)
class DeltaHistoryRequest:
    """Inputs for Delta history snapshots."""

    path: str
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    limit: int = 1
    runtime_profile: DataFusionRuntimeProfile | None = None
    dataset: str | None = None


@dataclass(frozen=True)
class DeltaVacuumRequest:
    """Inputs for Delta vacuum operations."""

    path: str
    options: DeltaVacuumOptions | None = None
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    dataset: str | None = None


@dataclass(frozen=True)
class DeltaQueryRequest:
    """Inputs for Delta SQL execution via DataFusion."""

    path: str
    sql: str
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    table_name: str = "t"
    runtime_profile: DataFusionRuntimeProfile | None = None


def delta_history(request: DeltaHistoryRequest) -> DeltaHistorySnapshot:
    """Return Delta history/protocol snapshots and record diagnostics.

    Returns
    -------
    DeltaHistorySnapshot
        History and protocol snapshots for the Delta table.
    """
    history = delta_history_snapshot(
        request.path,
        storage_options=request.storage_options,
        log_storage_options=request.log_storage_options,
        limit=request.limit,
    )
    protocol = delta_protocol_snapshot(
        request.path,
        storage_options=request.storage_options,
        log_storage_options=request.log_storage_options,
    )
    version = delta_table_version(
        request.path,
        storage_options=request.storage_options,
        log_storage_options=request.log_storage_options,
    )
    snapshot = DeltaHistorySnapshot(
        path=request.path,
        version=version,
        history=history,
        protocol=protocol,
    )
    storage_options, log_storage_options = normalize_storage_options(
        request.storage_options,
        request.log_storage_options,
    )
    storage_hash = hash_storage_options(storage_options, log_storage_options)
    _record_delta_snapshot_table(
        request.runtime_profile,
        table_uri=request.path,
        snapshot=history or {},
        dataset_name=request.dataset,
        storage_hash=storage_hash,
    )
    _record_maintenance(
        request.runtime_profile,
        payload={
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": request.dataset,
            "path": request.path,
            "operation": "history",
            "version": version,
            "history": history,
            "protocol": protocol,
        },
    )
    return snapshot


def delta_vacuum(request: DeltaVacuumRequest) -> DeltaVacuumResult:
    """Run Delta vacuum and record diagnostics.

    Returns
    -------
    DeltaVacuumResult
        Vacuum result payload.

    Raises
    ------
    ValueError
        Raised when the retention guardrail is violated.
    """
    resolved = request.options or DeltaVacuumOptions()
    if resolved.enforce_retention_duration and (
        resolved.retention_hours is None or resolved.retention_hours < _DELTA_MIN_RETENTION_HOURS
    ):
        msg = (
            "Delta vacuum retention_hours must be at least "
            f"{_DELTA_MIN_RETENTION_HOURS} when enforcement is enabled."
        )
        raise ValueError(msg)
    removed = vacuum_delta(
        request.path,
        options=resolved,
        storage_options=request.storage_options,
        log_storage_options=request.log_storage_options,
    )
    result = DeltaVacuumResult(
        path=request.path,
        removed_files=tuple(removed),
        dry_run=resolved.dry_run,
        retention_hours=resolved.retention_hours,
    )
    storage_options, log_storage_options = normalize_storage_options(
        request.storage_options,
        request.log_storage_options,
    )
    storage_hash = hash_storage_options(storage_options, log_storage_options)
    _record_delta_maintenance_table(
        _DeltaMaintenanceRecordRequest(
            profile=request.runtime_profile,
            table_uri=request.path,
            operation="vacuum",
            report={"metrics": {"removed_files": list(removed)}},
            dataset_name=request.dataset,
            retention_hours=resolved.retention_hours,
            dry_run=resolved.dry_run,
            storage_hash=storage_hash,
        )
    )
    _record_maintenance(
        request.runtime_profile,
        payload={
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": request.dataset,
            "path": request.path,
            "operation": "vacuum",
            "dry_run": resolved.dry_run,
            "retention_hours": resolved.retention_hours,
            "removed_files": list(removed),
        },
    )
    return result


def _record_maintenance(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    payload: Mapping[str, object],
) -> None:
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    from datafusion_engine.diagnostics import record_artifact

    record_artifact(runtime_profile, "delta_maintenance_v1", payload)


def delta_query(request: DeltaQueryRequest) -> RecordBatchReaderLike:
    """Execute SQL against a Delta table using DataFusion.

    Returns
    -------
    RecordBatchReaderLike
        Streaming reader over the query results.

    Raises
    ------
    ValueError
        Raised when SQL execution is disabled in the runtime profile.
    """
    profile = request.runtime_profile
    if profile is None:
        from datafusion_engine.runtime import DataFusionRuntimeProfile

        profile = DataFusionRuntimeProfile()
    sql_policy = profile.sql_policy
    if sql_policy is None or not sql_policy.allow_statements:
        msg = "Delta SQL execution is disabled by the runtime SQL policy."
        raise ValueError(msg)
    storage = dict(request.storage_options or {})
    if request.log_storage_options:
        storage.update({str(key): str(value) for key, value in request.log_storage_options.items()})
    if profile.enable_delta_querybuilder:
        from storage.deltalake import query_delta_sql

        reader = query_delta_sql(
            request.sql,
            {request.table_name: request.path},
            storage_options=storage or None,
        )
        _record_delta_query(
            profile,
            payload={
                "event_time_unix_ms": int(time.time() * 1000),
                "path": request.path,
                "sql": request.sql,
                "table_name": request.table_name,
                "engine": "delta_querybuilder",
            },
        )
        return reader
    from datafusion_engine.dataset_registry import DatasetLocation
    from datafusion_engine.registry_bridge import register_dataset_df

    ctx = profile.session_context()
    location = DatasetLocation(
        path=request.path,
        format="delta",
        storage_options=storage,
        delta_log_storage_options=request.log_storage_options or {},
    )
    register_dataset_df(
        ctx,
        name=request.table_name,
        location=location,
        runtime_profile=profile,
    )
    df = ctx.sql_with_options(request.sql, profile.sql_options())
    to_reader = getattr(df, "to_arrow_reader", None)
    reader = cast(
        "RecordBatchReaderLike",
        to_reader() if callable(to_reader) else df.to_arrow_table().to_reader(),
    )
    _record_delta_query(
        profile,
        payload={
            "event_time_unix_ms": int(time.time() * 1000),
            "path": request.path,
            "sql": request.sql,
            "table_name": request.table_name,
            "engine": "datafusion",
        },
    )
    return reader


def _record_delta_query(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    payload: Mapping[str, object],
) -> None:
    if runtime_profile.diagnostics_sink is None:
        return
    from datafusion_engine.diagnostics import record_artifact

    record_artifact(runtime_profile, "delta_query_v1", payload)


def _record_delta_snapshot_table(
    profile: DataFusionRuntimeProfile | None,
    *,
    table_uri: str,
    snapshot: Mapping[str, object],
    dataset_name: str | None,
    storage_hash: str | None,
) -> None:
    if profile is None or not snapshot:
        return
    from datafusion_engine.delta_observability import (
        DeltaSnapshotArtifact,
        record_delta_snapshot,
    )

    record_delta_snapshot(
        profile,
        artifact=DeltaSnapshotArtifact(
            table_uri=table_uri,
            snapshot=snapshot,
            dataset_name=dataset_name,
            storage_options_hash=storage_hash,
        ),
    )


@dataclass(frozen=True)
class _DeltaMaintenanceRecordRequest:
    """Inputs required to record a Delta maintenance artifact."""

    profile: DataFusionRuntimeProfile | None
    table_uri: str
    operation: str
    report: Mapping[str, object]
    dataset_name: str | None
    retention_hours: int | None
    dry_run: bool | None
    storage_hash: str | None


def _record_delta_maintenance_table(request: _DeltaMaintenanceRecordRequest) -> None:
    if request.profile is None:
        return
    from datafusion_engine.delta_observability import (
        DeltaMaintenanceArtifact,
        record_delta_maintenance,
    )

    record_delta_maintenance(
        request.profile,
        artifact=DeltaMaintenanceArtifact(
            table_uri=request.table_uri,
            operation=request.operation,
            report=request.report,
            dataset_name=request.dataset_name,
            retention_hours=request.retention_hours,
            dry_run=request.dry_run,
            storage_options_hash=request.storage_hash,
        ),
    )


__all__ = [
    "DeltaHistoryRequest",
    "DeltaHistorySnapshot",
    "DeltaQueryRequest",
    "DeltaVacuumRequest",
    "DeltaVacuumResult",
    "delta_history",
    "delta_query",
    "delta_vacuum",
]
