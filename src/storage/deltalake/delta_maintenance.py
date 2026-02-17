"""Delta Lake maintenance operations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from deltalake import DeltaTable

from obs.otel import SCOPE_STORAGE, stage_span
from storage.deltalake.delta_read import DeltaVacuumOptions, StorageOptions
from storage.deltalake.delta_runtime_ops import (
    _DeltaMaintenanceRecord,
    record_delta_maintenance,
    storage_span_attributes,
)
from storage.deltalake.delta_write import build_commit_properties

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def vacuum_delta(
    path: str,
    *,
    options: DeltaVacuumOptions | None = None,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> list[str]:
    """Run Delta vacuum maintenance.

    Returns:
    -------
    list[str]
        Removed/eligible file paths reported by vacuum.
    """
    from utils.storage_options import merged_storage_options

    options = options or DeltaVacuumOptions()
    attrs = storage_span_attributes(
        operation="vacuum",
        table_path=path,
        extra={
            "codeanatomy.retention_hours": options.retention_hours,
            "codeanatomy.dry_run": options.dry_run,
            "codeanatomy.enforce_retention_duration": options.enforce_retention_duration,
        },
    )
    with stage_span(
        "storage.vacuum",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ) as span:
        storage = merged_storage_options(storage_options, log_storage_options)
        report: Mapping[str, object] | None = None
        fallback_error: Exception | None = None
        if runtime_profile is not None:
            try:
                from datafusion_engine.delta.control_plane_core import (
                    DeltaCommitOptions,
                    DeltaVacuumRequest,
                    delta_vacuum,
                )

                report = delta_vacuum(
                    runtime_profile.delta_ops.delta_runtime_ctx(),
                    request=DeltaVacuumRequest(
                        table_uri=path,
                        storage_options=storage or None,
                        version=None,
                        timestamp=None,
                        retention_hours=options.retention_hours,
                        dry_run=options.dry_run,
                        enforce_retention_duration=options.enforce_retention_duration,
                        require_vacuum_protocol_check=options.require_vacuum_protocol_check,
                        commit_options=DeltaCommitOptions(
                            metadata=dict(options.commit_metadata or {})
                        ),
                    ),
                )
            except (ImportError, RuntimeError, TypeError, ValueError) as exc:
                fallback_error = exc
        else:
            fallback_error = RuntimeError("runtime_profile not provided")

        if report is None:
            table = DeltaTable(path, storage_options=dict(storage) if storage else None)
            commit_properties = build_commit_properties(
                commit_metadata=options.commit_metadata or None
            )
            files = table.vacuum(
                retention_hours=options.retention_hours,
                dry_run=options.dry_run,
                enforce_retention_duration=options.enforce_retention_duration,
                commit_properties=commit_properties,
                full=options.full,
                keep_versions=list(options.keep_versions)
                if options.keep_versions is not None
                else None,
            )
            span.set_attribute("codeanatomy.vacuum_fallback", value=True)
            if fallback_error is not None:
                span.set_attribute("codeanatomy.vacuum_fallback_error", str(fallback_error))
            if isinstance(files, Sequence) and not isinstance(files, (str, bytes, bytearray)):
                return [str(item) for item in files]
            return []
        metrics = report.get("metrics")
        if isinstance(metrics, Mapping):
            for key in ("files", "removed_files", "deleted_files"):
                value = metrics.get(key)
                if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                    span.set_attribute("codeanatomy.files_removed", len(value))
                    return [str(item) for item in value]
        return []


def create_delta_checkpoint(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    dataset_name: str | None = None,
) -> Mapping[str, object]:
    """Create Delta checkpoints for log compaction.

    Returns:
    -------
    Mapping[str, object]
        Checkpoint maintenance report payload.
    """
    from utils.storage_options import merged_storage_options

    attrs = storage_span_attributes(
        operation="checkpoint",
        table_path=path,
        dataset_name=dataset_name,
    )
    with stage_span(
        "storage.checkpoint",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ):
        storage = merged_storage_options(storage_options, log_storage_options)
        ctx = runtime_profile.delta_ops.delta_runtime_ctx() if runtime_profile is not None else None
        report: Mapping[str, object] | None = None
        fallback_error: Exception | None = None
        if ctx is not None:
            try:
                from datafusion_engine.delta.control_plane_core import (
                    DeltaCheckpointRequest,
                    delta_create_checkpoint,
                )

                report = delta_create_checkpoint(
                    ctx,
                    request=DeltaCheckpointRequest(
                        table_uri=path,
                        storage_options=storage or None,
                        version=None,
                        timestamp=None,
                    ),
                )
            except (ImportError, RuntimeError, TypeError, ValueError) as exc:
                fallback_error = exc
        else:
            fallback_error = RuntimeError("runtime_profile not provided")

        if report is None:
            table = DeltaTable(path, storage_options=dict(storage) if storage else None)
            table.create_checkpoint()
            report = {
                "checkpoint": True,
                "version": table.version(),
                "fallback_error": str(fallback_error) if fallback_error is not None else None,
            }
            record_delta_maintenance(
                _DeltaMaintenanceRecord(
                    runtime_profile=runtime_profile,
                    report=report,
                    operation="create_checkpoint",
                    path=path,
                    storage_options=storage_options,
                    log_storage_options=log_storage_options,
                    dataset_name=dataset_name,
                    commit_metadata=None,
                    retention_hours=None,
                    dry_run=None,
                )
            )
            return report
        record_delta_maintenance(
            _DeltaMaintenanceRecord(
                runtime_profile=runtime_profile,
                report=report,
                operation="create_checkpoint",
                path=path,
                storage_options=storage_options,
                log_storage_options=log_storage_options,
                dataset_name=dataset_name,
                commit_metadata=None,
                retention_hours=None,
                dry_run=None,
            )
        )
        return report


def cleanup_delta_log(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    dataset_name: str | None = None,
) -> Mapping[str, object]:
    """Clean up stale Delta log files.

    Returns:
    -------
    Mapping[str, object]
        Cleanup maintenance report payload.

    Raises:
        ValueError: If ``runtime_profile`` is not provided.
        RuntimeError: If Rust control-plane cleanup fails.
    """
    from utils.storage_options import merged_storage_options

    attrs = storage_span_attributes(
        operation="cleanup_metadata",
        table_path=path,
        dataset_name=dataset_name,
    )
    with stage_span(
        "storage.cleanup_metadata",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ):
        storage = merged_storage_options(storage_options, log_storage_options)
        if runtime_profile is None:
            msg = "cleanup_delta_log requires an explicit runtime_profile."
            raise ValueError(msg)
        ctx = runtime_profile.delta_ops.delta_runtime_ctx()
        try:
            from datafusion_engine.delta.control_plane_core import (
                DeltaCheckpointRequest,
                delta_cleanup_metadata,
            )

            report = delta_cleanup_metadata(
                ctx,
                request=DeltaCheckpointRequest(
                    table_uri=path,
                    storage_options=storage or None,
                    version=None,
                    timestamp=None,
                ),
            )
        except (ImportError, RuntimeError, TypeError, ValueError) as exc:
            msg = f"Delta metadata cleanup failed via Rust control plane: {exc}"
            raise RuntimeError(msg) from exc
        record_delta_maintenance(
            _DeltaMaintenanceRecord(
                runtime_profile=runtime_profile,
                report=report,
                operation="cleanup_metadata",
                path=path,
                storage_options=storage_options,
                log_storage_options=log_storage_options,
                dataset_name=dataset_name,
                commit_metadata=None,
                retention_hours=None,
                dry_run=None,
            )
        )
        return report


__all__ = [
    "cleanup_delta_log",
    "create_delta_checkpoint",
    "vacuum_delta",
]
