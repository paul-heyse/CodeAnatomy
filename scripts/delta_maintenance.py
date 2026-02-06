#!/usr/bin/env python3
"""Run Delta Lake maintenance tasks (vacuum/checkpoint/cleanup).

Deprecated: use `codeanatomy delta` subcommands instead.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from datafusion_engine.delta.service import delta_service_for_profile
from storage.deltalake import DeltaVacuumOptions

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True)
class VacuumReport:
    """Results from a Delta vacuum operation."""

    retention_hours: int | None
    dry_run: bool
    enforce_retention_duration: bool
    full: bool
    keep_versions: list[int] | None
    file_count: int
    files: list[str]


@dataclass(frozen=True)
class MaintenanceReport:
    """Report payload for Delta maintenance operations."""

    path: str
    vacuum: VacuumReport | None
    checkpoint: bool
    cleanup_log: bool


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Delta Lake maintenance tasks.")
    parser.add_argument("--path", required=True, help="Delta table path.")
    parser.add_argument(
        "--storage-option",
        action="append",
        default=[],
        help="Storage option key=value (repeatable).",
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Run Delta vacuum (default dry-run).",
    )
    parser.add_argument(
        "--checkpoint",
        action="store_true",
        help="Create a Delta checkpoint.",
    )
    parser.add_argument(
        "--cleanup-log",
        action="store_true",
        help="Cleanup expired Delta log files.",
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=None,
        help="Retention window in hours for vacuum.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply vacuum deletions (not a dry-run).",
    )
    parser.add_argument(
        "--no-enforce-retention",
        action="store_true",
        help="Disable minimum retention duration checks during vacuum.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run a full vacuum (including active files where supported).",
    )
    parser.add_argument(
        "--keep-versions",
        default=None,
        help="Comma-separated Delta versions to retain during vacuum.",
    )
    parser.add_argument(
        "--commit-metadata",
        action="append",
        default=[],
        help="Commit metadata key=value entries for vacuum.",
    )
    parser.add_argument(
        "--report-path",
        default=None,
        help="Optional path for JSON report (default: stdout).",
    )
    parser.add_argument(
        "--artifact-path",
        default=None,
        help="Optional path for a run-bundle artifact payload.",
    )
    return parser


def _parse_kv_pairs(values: Sequence[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for item in values:
        key, sep, value = item.partition("=")
        if not sep or not key:
            msg = f"Expected key=value, got {item!r}."
            raise ValueError(msg)
        parsed[key] = value
    return parsed


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
    payload = json.dumps(asdict(report), indent=2, sort_keys=True)
    if report_path is None:
        sys.stdout.write(payload + "\n")
        return
    Path(report_path).write_text(payload + "\n", encoding="utf-8")


def _write_artifact(report: MaintenanceReport, artifact_path: str | None) -> None:
    if artifact_path is None:
        return
    payload = {"reports": [asdict(report)]}
    encoded = json.dumps(payload, indent=2, sort_keys=True)
    Path(artifact_path).write_text(encoded + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    """Run Delta maintenance tasks with CLI-provided options.

    Returns:
    -------
    int
        Exit status code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not (args.vacuum or args.checkpoint or args.cleanup_log):
        parser.error("Specify at least one operation: --vacuum, --checkpoint, --cleanup-log.")

    try:
        storage_options = _parse_kv_pairs(args.storage_option)
        commit_metadata = _parse_kv_pairs(args.commit_metadata)
        keep_versions = _parse_int_list(args.keep_versions)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    storage_opts = storage_options or None
    service = delta_service_for_profile(None)
    vacuum_report: VacuumReport | None = None
    if args.vacuum:
        options = DeltaVacuumOptions(
            retention_hours=args.retention_hours,
            dry_run=not args.apply,
            enforce_retention_duration=not args.no_enforce_retention,
            full=bool(args.full),
            keep_versions=keep_versions,
            commit_metadata=commit_metadata or None,
        )
        files = service.vacuum(
            path=args.path,
            options=options,
            storage_options=storage_opts,
        )
        vacuum_report = VacuumReport(
            retention_hours=args.retention_hours,
            dry_run=not args.apply,
            enforce_retention_duration=not args.no_enforce_retention,
            full=bool(args.full),
            keep_versions=keep_versions,
            file_count=len(files),
            files=list(files),
        )

    checkpoint = False
    if args.checkpoint:
        service.create_checkpoint(path=args.path, storage_options=storage_opts)
        checkpoint = True

    cleanup_log = False
    if args.cleanup_log:
        service.cleanup_log(path=args.path, storage_options=storage_opts)
        cleanup_log = True

    report = MaintenanceReport(
        path=args.path,
        vacuum=vacuum_report,
        checkpoint=checkpoint,
        cleanup_log=cleanup_log,
    )
    _write_report(report, args.report_path)
    _write_artifact(report, args.artifact_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
