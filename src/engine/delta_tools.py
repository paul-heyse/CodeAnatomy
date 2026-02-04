"""Engine-level helpers for Delta maintenance operations."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from datafusion_engine.arrow.interop import RecordBatchReaderLike
from datafusion_engine.delta.service import delta_service_for_profile
from engine.diagnostics import EngineEventRecorder
from storage.deltalake import DeltaVacuumOptions, StorageOptions

_DELTA_MIN_RETENTION_HOURS = 168

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.delta.protocol import DeltaProtocolSnapshot
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


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
    """Inputs for Delta DataFusion query execution."""

    path: str
    builder: Callable[[SessionContext, str], DataFrame] | None = None
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    table_name: str = "t"
    runtime_profile: DataFusionRuntimeProfile | None = None
    query_label: str | None = None


def delta_history(request: DeltaHistoryRequest) -> DeltaHistorySnapshot:
    """Return Delta history/protocol snapshots and record diagnostics.

    Returns
    -------
    DeltaHistorySnapshot
        History and protocol snapshots for the Delta table.
    """
    service = delta_service_for_profile(request.runtime_profile)
    history = service.history_snapshot(
        path=request.path,
        storage_options=request.storage_options,
        log_storage_options=request.log_storage_options,
        limit=request.limit,
    )
    protocol = service.protocol_snapshot(
        path=request.path,
        storage_options=request.storage_options,
        log_storage_options=request.log_storage_options,
    )
    version = service.table_version(
        path=request.path,
        storage_options=request.storage_options,
        log_storage_options=request.log_storage_options,
    )
    snapshot = DeltaHistorySnapshot(
        path=request.path,
        version=version,
        history=history,
        protocol=protocol,
    )
    _record_delta_snapshot_table(
        request.runtime_profile,
        table_uri=request.path,
        snapshot=history or {},
        dataset_name=request.dataset,
    )
    EngineEventRecorder(request.runtime_profile).record_delta_maintenance(
        dataset=request.dataset,
        path=request.path,
        operation="history",
        extra={
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
    service = delta_service_for_profile(request.runtime_profile)
    removed = service.vacuum(
        path=request.path,
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
    _record_delta_maintenance_table(
        _DeltaMaintenanceRecordRequest(
            profile=request.runtime_profile,
            table_uri=request.path,
            operation="vacuum",
            report={"metrics": {"removed_files": list(removed)}},
            dataset_name=request.dataset,
            retention_hours=resolved.retention_hours,
            dry_run=resolved.dry_run,
        )
    )
    EngineEventRecorder(request.runtime_profile).record_delta_maintenance(
        dataset=request.dataset,
        path=request.path,
        operation="vacuum",
        extra={
            "dry_run": resolved.dry_run,
            "retention_hours": resolved.retention_hours,
            "removed_files": list(removed),
        },
    )
    return result


def delta_query(request: DeltaQueryRequest) -> RecordBatchReaderLike:
    """Execute a DataFusion query against a Delta table.

    Returns
    -------
    RecordBatchReaderLike
        Streaming reader over the query results.
    """
    profile = request.runtime_profile
    if profile is None:
        from datafusion_engine.session.runtime import DataFusionRuntimeProfile

        profile = DataFusionRuntimeProfile()
    storage = dict(request.storage_options or {})
    if request.log_storage_options:
        storage.update({str(key): str(value) for key, value in request.log_storage_options.items()})
    from datafusion_engine.dataset.registration import (
        DatasetRegistrationOptions,
        register_dataset_df,
    )
    from datafusion_engine.dataset.registry import DatasetLocation

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
        options=DatasetRegistrationOptions(runtime_profile=profile),
    )
    builder = request.builder
    df = builder(ctx, request.table_name) if builder is not None else ctx.table(request.table_name)
    to_reader = getattr(df, "to_arrow_reader", None)
    reader = cast(
        "RecordBatchReaderLike",
        to_reader() if callable(to_reader) else df.to_arrow_table().to_reader(),
    )
    EngineEventRecorder(profile).record_delta_query(
        path=request.path,
        sql=request.query_label,
        table_name=request.table_name,
        engine="datafusion",
    )
    return reader


def _record_delta_snapshot_table(
    profile: DataFusionRuntimeProfile | None,
    *,
    table_uri: str,
    snapshot: Mapping[str, object],
    dataset_name: str | None,
) -> None:
    if profile is None or not snapshot:
        return
    from datafusion_engine.delta.observability import (
        DeltaSnapshotArtifact,
        record_delta_snapshot,
    )

    record_delta_snapshot(
        profile,
        artifact=DeltaSnapshotArtifact(
            table_uri=table_uri,
            snapshot=snapshot,
            dataset_name=dataset_name,
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


def _record_delta_maintenance_table(request: _DeltaMaintenanceRecordRequest) -> None:
    if request.profile is None:
        return
    from datafusion_engine.delta.observability import (
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
