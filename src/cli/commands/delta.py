"""Delta maintenance CLI commands."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal

import msgspec
from cyclopts import Parameter
from deltalake import DeltaTable

from cli.groups import restore_target_group
from cli.kv_parser import parse_kv_pairs
from datafusion_engine.delta.control_plane import (
    DeltaProviderRequest,
    DeltaRestoreRequest,
    delta_provider_from_session,
    delta_restore,
)
from datafusion_engine.delta.service import delta_service_for_profile
from datafusion_engine.errors import DataFusionEngineError
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.io.ingest import datafusion_from_arrow
from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.tables.metadata import TableProviderCapsule
from serde_msgspec import JSON_ENCODER_SORTED, StructBaseCompat, json_default
from storage.deltalake import DeltaVacuumOptions
from utils.uuid_factory import uuid7_hex


class VacuumReport(StructBaseCompat, frozen=True):
    """Results from a Delta vacuum operation."""

    retention_hours: int | None
    dry_run: bool
    enforce_retention_duration: bool
    full: bool
    keep_versions: list[int] | None
    file_count: int
    files: list[str]


class MaintenanceReport(StructBaseCompat, frozen=True):
    """Report payload for Delta maintenance operations."""

    path: str
    vacuum: VacuumReport | None
    checkpoint: bool
    cleanup_log: bool


class RestoreReport(StructBaseCompat, frozen=True):
    """Report payload for Delta restore operations."""

    path: str
    version: int | None
    timestamp: str | None
    result: object | None


class CloneReport(StructBaseCompat, frozen=True):
    """Report payload for Delta snapshot clones."""

    path: str
    target: str
    version: int | None
    timestamp: str | None
    rows: int
    schema_identity_hash: str


@dataclass(frozen=True)
class DeltaCloneOptions:
    """Options for cloning a Delta snapshot."""

    version: int | None = None
    timestamp: str | None = None
    storage_options: dict[str, str] | None = None
    mode: str = "overwrite"
    schema_mode: str | None = "overwrite"


@dataclass(frozen=True)
class VacuumOptions:
    """CLI options for Delta vacuum."""

    storage_option: Annotated[
        tuple[str, ...],
        Parameter(
            name="--storage-option",
            help="Storage option key=value (repeatable).",
            env_var="CODEANATOMY_DELTA_STORAGE_OPTIONS",
        ),
    ] = ()
    retention_hours: Annotated[
        int | None,
        Parameter(
            name="--retention-hours",
            help="Retention window in hours for vacuum.",
        ),
    ] = None
    apply: Annotated[
        bool,
        Parameter(
            name="--apply",
            help="Apply vacuum deletions (not a dry-run).",
        ),
    ] = False
    no_enforce_retention: Annotated[
        bool,
        Parameter(
            name="--no-enforce-retention",
            help="Disable minimum retention duration checks during vacuum.",
        ),
    ] = False
    full: Annotated[
        bool,
        Parameter(
            name="--full",
            help="Run a full vacuum (including active files where supported).",
        ),
    ] = False
    keep_versions: Annotated[
        str | None,
        Parameter(
            name="--keep-versions",
            help="Comma-separated Delta versions to retain during vacuum.",
        ),
    ] = None
    commit_metadata: Annotated[
        tuple[str, ...],
        Parameter(
            name="--commit-metadata",
            help="Commit metadata key=value entries for vacuum (repeatable).",
        ),
    ] = ()
    report_path: Annotated[
        str | None,
        Parameter(
            name="--report-path",
            help="Optional path for JSON report (default: stdout).",
        ),
    ] = None
    artifact_path: Annotated[
        str | None,
        Parameter(
            name="--artifact-path",
            help="Optional path for a run-bundle artifact payload.",
        ),
    ] = None


@dataclass(frozen=True)
class ExportOptions:
    """CLI options for Delta snapshot export."""

    version: Annotated[
        int | None,
        Parameter(
            name="--version",
            help="Delta version to clone.",
            group=restore_target_group,
        ),
    ] = None
    timestamp: Annotated[
        str | None,
        Parameter(
            name="--timestamp",
            help="Timestamp to clone (as accepted by Delta Lake).",
            group=restore_target_group,
        ),
    ] = None
    storage_option: Annotated[tuple[str, ...], Parameter(name="--storage-option")] = ()
    mode: Annotated[
        Literal["overwrite", "append", "error"],
        Parameter(name="--mode", help="Delta write mode."),
    ] = "overwrite"
    schema_mode: Annotated[
        Literal["overwrite", "merge"] | None,
        Parameter(name="--schema-mode", help="Delta schema mode."),
    ] = "overwrite"
    report_path: Annotated[str | None, Parameter(name="--report-path")] = None


@dataclass(frozen=True)
class RestoreOptions:
    """CLI options for Delta restore."""

    version: Annotated[
        int | None,
        Parameter(
            name="--version",
            help="Delta table version to restore.",
            group=restore_target_group,
        ),
    ] = None
    timestamp: Annotated[
        str | None,
        Parameter(
            name="--timestamp",
            help="Timestamp to restore (as accepted by Delta Lake).",
            group=restore_target_group,
        ),
    ] = None
    storage_option: Annotated[tuple[str, ...], Parameter(name="--storage-option")] = ()
    allow_unsafe: Annotated[
        bool,
        Parameter(
            name="--allow-unsafe",
            help="Allow unsafe restore operations when supported.",
        ),
    ] = False
    report_path: Annotated[str | None, Parameter(name="--report-path")] = None


_DEFAULT_VACUUM_OPTIONS = VacuumOptions()
_DEFAULT_EXPORT_OPTIONS = ExportOptions()
_DEFAULT_RESTORE_OPTIONS = RestoreOptions()


def vacuum_command(
    path: Annotated[
        str,
        Parameter(
            name="--path",
            help="Delta table path.",
            required=True,
        ),
    ],
    options: Annotated[VacuumOptions, Parameter(name="*")] = _DEFAULT_VACUUM_OPTIONS,
) -> int:
    """Run Delta vacuum to remove expired files.

    Returns:
    -------
    int
        Exit status code.
    """
    storage_options: dict[str, str] = (
        parse_kv_pairs(options.storage_option) if options.storage_option else {}
    )
    commit_payload: dict[str, str] = (
        parse_kv_pairs(options.commit_metadata) if options.commit_metadata else {}
    )
    keep_versions_list = _parse_int_list(options.keep_versions)

    vacuum_options = DeltaVacuumOptions(
        retention_hours=options.retention_hours,
        dry_run=not options.apply,
        enforce_retention_duration=not options.no_enforce_retention,
        full=bool(options.full),
        keep_versions=keep_versions_list,
        commit_metadata=commit_payload or None,
    )
    service = delta_service_for_profile(None)
    files = service.vacuum(
        path=path,
        options=vacuum_options,
        storage_options=storage_options or None,
    )
    vacuum_report = VacuumReport(
        retention_hours=options.retention_hours,
        dry_run=not options.apply,
        enforce_retention_duration=not options.no_enforce_retention,
        full=bool(options.full),
        keep_versions=keep_versions_list,
        file_count=len(files),
        files=list(files),
    )
    report = MaintenanceReport(
        path=path,
        vacuum=vacuum_report,
        checkpoint=False,
        cleanup_log=False,
    )
    _write_report(report, options.report_path)
    _write_artifact(report, options.artifact_path)
    return 0


def checkpoint_command(
    path: Annotated[str, Parameter(name="--path", required=True)],
    storage_option: Annotated[tuple[str, ...], Parameter(name="--storage-option")] = (),
    report_path: Annotated[str | None, Parameter(name="--report-path")] = None,
) -> int:
    """Create a Delta checkpoint.

    Returns:
    -------
    int
        Exit status code.
    """
    storage_options: dict[str, str] = parse_kv_pairs(storage_option) if storage_option else {}
    service = delta_service_for_profile(None)
    service.create_checkpoint(
        path=path,
        storage_options=storage_options or None,
    )
    report = MaintenanceReport(
        path=path,
        vacuum=None,
        checkpoint=True,
        cleanup_log=False,
    )
    _write_report(report, report_path)
    return 0


def cleanup_log_command(
    path: Annotated[str, Parameter(name="--path", required=True)],
    storage_option: Annotated[tuple[str, ...], Parameter(name="--storage-option")] = (),
    report_path: Annotated[str | None, Parameter(name="--report-path")] = None,
) -> int:
    """Cleanup expired Delta log files.

    Returns:
    -------
    int
        Exit status code.
    """
    storage_options: dict[str, str] = parse_kv_pairs(storage_option) if storage_option else {}
    service = delta_service_for_profile(None)
    service.cleanup_log(
        path=path,
        storage_options=storage_options or None,
    )
    report = MaintenanceReport(
        path=path,
        vacuum=None,
        checkpoint=False,
        cleanup_log=True,
    )
    _write_report(report, report_path)
    return 0


def export_command(
    path: Annotated[str, Parameter(name="--path", required=True, help="Delta table path.")],
    target: Annotated[
        str,
        Parameter(name="--target", required=True, help="Target Delta table path."),
    ],
    options: Annotated[ExportOptions, Parameter(name="*")] = _DEFAULT_EXPORT_OPTIONS,
) -> int:
    """Clone a Delta snapshot into Delta storage.

    Returns:
    -------
    int
        Exit status code.
    """
    storage_options: dict[str, str] = (
        parse_kv_pairs(options.storage_option) if options.storage_option else {}
    )
    clone_options = DeltaCloneOptions(
        version=options.version,
        timestamp=options.timestamp,
        storage_options=storage_options or None,
        mode=options.mode,
        schema_mode=options.schema_mode,
    )
    report = clone_delta_snapshot(path, target, options=clone_options)
    payload = _encode_pretty_json(_report_payload(report))
    _write_text_payload(payload, options.report_path)
    return 0


def restore_command(
    path: Annotated[str, Parameter(name="--path", required=True, help="Delta table path.")],
    options: Annotated[RestoreOptions, Parameter(name="*")] = _DEFAULT_RESTORE_OPTIONS,
) -> int:
    """Restore a Delta table to a prior version or timestamp.

    Returns:
    -------
    int
        Exit status code.
    """
    storage_options: dict[str, str] = (
        parse_kv_pairs(options.storage_option) if options.storage_option else {}
    )
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    result = delta_restore(
        ctx,
        request=DeltaRestoreRequest(
            table_uri=path,
            storage_options=storage_options or None,
            version=None,
            timestamp=None,
            restore_version=options.version,
            restore_timestamp=options.timestamp,
            allow_unsafe_restore=options.allow_unsafe,
        ),
    )
    report = RestoreReport(
        path=path,
        version=options.version,
        timestamp=options.timestamp,
        result=result,
    )
    payload = _encode_pretty_json(_report_payload(report))
    _write_text_payload(payload, options.report_path)
    return 0


def clone_delta_snapshot(
    path: str,
    target: str,
    *,
    options: DeltaCloneOptions | None = None,
) -> CloneReport:
    """Clone a Delta snapshot into Delta storage and return a report.

    Args:
        path: Source Delta table path.
        target: Destination path for the cloned snapshot.
        options: Optional clone settings.

    Returns:
        CloneReport: Result.

    Raises:
        ValueError: If both version and timestamp are provided.
    """
    resolved = options or DeltaCloneOptions()
    if resolved.version is not None and resolved.timestamp is not None:
        msg = "Specify only one of version or timestamp."
        raise ValueError(msg)
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    try:
        bundle = delta_provider_from_session(
            ctx,
            request=DeltaProviderRequest(
                table_uri=path,
                storage_options=resolved.storage_options or None,
                version=resolved.version,
                timestamp=resolved.timestamp,
                delta_scan=None,
                gate=None,
            ),
        )
        adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
        table_name = f"__delta_snapshot_{uuid7_hex()}"
        adapter.register_table(
            table_name,
            TableProviderCapsule(bundle.provider),
            overwrite=True,
        )
        try:
            arrow_table = ctx.table(table_name).to_arrow_table()
        finally:
            adapter.deregister_table(table_name)
    except (DataFusionEngineError, RuntimeError, TypeError, ValueError):
        table = DeltaTable(
            path,
            version=resolved.version,
            storage_options=resolved.storage_options or None,
        )
        if resolved.timestamp is not None:
            table.load_as_version(resolved.timestamp)
        arrow_table = table.to_pyarrow_table()
    target_path = Path(target)
    if target_path.parent:
        target_path.parent.mkdir(parents=True, exist_ok=True)
    commit_metadata: dict[str, str] = {"clone_source": path}
    if resolved.version is not None:
        commit_metadata["source_version"] = str(resolved.version)
    if resolved.timestamp is not None:
        commit_metadata["source_timestamp"] = resolved.timestamp
    format_options: dict[str, object] = {"commit_metadata": commit_metadata}
    if resolved.schema_mode is not None:
        format_options["schema_mode"] = resolved.schema_mode
    if resolved.storage_options is not None:
        format_options["storage_options"] = dict(resolved.storage_options)
    df = datafusion_from_arrow(ctx, name=f"__delta_clone_{uuid7_hex()}", value=arrow_table)
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    pipeline.write(
        WriteRequest(
            source=df,
            destination=str(target_path),
            format=WriteFormat.DELTA,
            mode=_resolve_write_mode(resolved.mode or "overwrite"),
            format_options=format_options,
        )
    )
    return CloneReport(
        path=path,
        target=str(target_path),
        version=resolved.version,
        timestamp=resolved.timestamp,
        rows=int(arrow_table.num_rows),
        schema_identity_hash=schema_identity_hash(arrow_table.schema),
    )


def _resolve_write_mode(mode: str) -> WriteMode:
    normalized = mode.strip().lower()
    if normalized == "overwrite":
        return WriteMode.OVERWRITE
    if normalized == "append":
        return WriteMode.APPEND
    if normalized == "error":
        return WriteMode.ERROR
    msg = f"Unsupported write mode: {mode!r}."
    raise ValueError(msg)


def _parse_int_list(value: str | None) -> list[int] | None:
    if value is None:
        return None
    items = [item.strip() for item in value.split(",") if item.strip()]
    if not items:
        return None
    out: list[int] = []
    for item in items:
        try:
            out.append(int(item))
        except ValueError as exc:
            msg = f"Invalid integer in keep_versions: {item!r}."
            raise ValueError(msg) from exc
    return out


def _write_report(report: MaintenanceReport, report_path: str | None) -> None:
    payload = _encode_pretty_json(_report_payload(report))
    _write_text_payload(payload, report_path)


def _write_artifact(report: MaintenanceReport, artifact_path: str | None) -> None:
    if artifact_path is None:
        return
    payload = {"reports": [_report_payload(report)]}
    encoded = _encode_pretty_json(payload)
    _write_text_payload(encoded, artifact_path)


def _report_payload(report: object) -> object:
    def _enc_hook(value: object) -> object:
        try:
            return json_default(value)
        except TypeError:
            return str(value)

    return msgspec.to_builtins(report, str_keys=True, enc_hook=_enc_hook)


def _encode_pretty_json(payload: object) -> str:
    raw = JSON_ENCODER_SORTED.encode(payload)
    formatted = msgspec.json.format(raw, indent=2)
    return formatted.decode("utf-8")


def _write_text_payload(payload: str, output_path: str | None) -> None:
    if output_path is None:
        sys.stdout.write(payload + "\n")
        return
    Path(output_path).write_text(payload + "\n", encoding="utf-8")


__all__ = [
    "checkpoint_command",
    "cleanup_log_command",
    "export_command",
    "restore_command",
    "vacuum_command",
]
