"""Engine-level helpers for Delta maintenance operations."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from storage.deltalake import (
    DeltaVacuumOptions,
    StorageOptions,
    delta_history_snapshot,
    delta_protocol_snapshot,
    delta_table_version,
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


def delta_history(
    *,
    path: str,
    storage_options: StorageOptions | None = None,
    limit: int = 1,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    dataset: str | None = None,
) -> DeltaHistorySnapshot:
    """Return Delta history/protocol snapshots and record diagnostics.

    Returns
    -------
    DeltaHistorySnapshot
        History and protocol snapshots for the Delta table.
    """
    history = delta_history_snapshot(
        path,
        storage_options=storage_options,
        limit=limit,
    )
    protocol = delta_protocol_snapshot(path, storage_options=storage_options)
    version = delta_table_version(path, storage_options=storage_options)
    snapshot = DeltaHistorySnapshot(
        path=path,
        version=version,
        history=history,
        protocol=protocol,
    )
    _record_maintenance(
        runtime_profile,
        payload={
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": dataset,
            "path": path,
            "operation": "history",
            "version": version,
            "history": history,
            "protocol": protocol,
        },
    )
    return snapshot


def delta_vacuum(
    *,
    path: str,
    options: DeltaVacuumOptions | None = None,
    storage_options: StorageOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    dataset: str | None = None,
) -> DeltaVacuumResult:
    """Run Delta vacuum and record diagnostics.

    Returns
    -------
    DeltaVacuumResult
        Vacuum result payload.
    """
    resolved = options or DeltaVacuumOptions()
    removed = vacuum_delta(
        path,
        options=resolved,
        storage_options=storage_options,
    )
    result = DeltaVacuumResult(
        path=path,
        removed_files=tuple(removed),
        dry_run=resolved.dry_run,
        retention_hours=resolved.retention_hours,
    )
    _record_maintenance(
        runtime_profile,
        payload={
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": dataset,
            "path": path,
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


__all__ = ["DeltaHistorySnapshot", "DeltaVacuumResult", "delta_history", "delta_vacuum"]
