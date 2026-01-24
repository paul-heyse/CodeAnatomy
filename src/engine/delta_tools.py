"""Engine-level helpers for Delta maintenance operations."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from arrowdsl.core.interop import RecordBatchReaderLike
from storage.deltalake import (
    DeltaVacuumOptions,
    StorageOptions,
    delta_history_snapshot,
    delta_protocol_snapshot,
    delta_table_version,
    query_builder_available,
    query_delta_via_querybuilder,
    vacuum_delta,
)

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class DeltaHistorySnapshot:
    """Delta history snapshot with metadata."""

    path: str
    version: int | None
    history: Mapping[str, object] | None
    protocol: Mapping[str, object] | None


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
    """Inputs for Delta QueryBuilder SQL execution."""

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
    """
    resolved = request.options or DeltaVacuumOptions()
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
    runtime_profile.diagnostics_sink.record_artifact("delta_maintenance_v1", payload)


def delta_query(request: DeltaQueryRequest) -> RecordBatchReaderLike:
    """Execute SQL against a Delta table using QueryBuilder.

    Returns
    -------
    RecordBatchReaderLike
        Streaming reader over the query results.

    Raises
    ------
    ValueError
        Raised when QueryBuilder execution is disabled or unavailable.
    """
    if (
        request.runtime_profile is not None
        and not request.runtime_profile.enable_delta_querybuilder
    ):
        msg = "Delta QueryBuilder execution is disabled in the runtime profile."
        raise ValueError(msg)
    if not query_builder_available():
        msg = "deltalake.QueryBuilder is unavailable."
        raise ValueError(msg)
    storage = request.log_storage_options or request.storage_options
    reader = query_delta_via_querybuilder(
        request.path,
        request.sql,
        table_name=request.table_name,
        storage_options=storage,
    )
    _record_querybuilder(
        request.runtime_profile,
        payload={
            "event_time_unix_ms": int(time.time() * 1000),
            "path": request.path,
            "sql": request.sql,
            "table_name": request.table_name,
        },
    )
    return reader


def _record_querybuilder(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    payload: Mapping[str, object],
) -> None:
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    runtime_profile.diagnostics_sink.record_artifact("delta_querybuilder_v1", payload)


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
