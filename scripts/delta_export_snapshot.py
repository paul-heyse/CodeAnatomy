#!/usr/bin/env python3
"""Export a Delta snapshot to a Parquet file."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

import pyarrow.parquet as pq

from arrowdsl.io.delta import open_delta_table
from arrowdsl.schema.serialization import schema_fingerprint


@dataclass(frozen=True)
class ExportReport:
    """Report payload for Delta snapshot exports."""

    path: str
    target: str
    version: int | None
    timestamp: str | None
    rows: int
    schema_fingerprint: str


@dataclass(frozen=True)
class DeltaExportOptions:
    """Options for exporting a Delta snapshot."""

    version: int | None = None
    timestamp: str | None = None
    storage_options: dict[str, str] | None = None
    compression: str | None = None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export a Delta snapshot to Parquet.")
    parser.add_argument("--path", required=True, help="Delta table path.")
    parser.add_argument("--target", required=True, help="Target Parquet file path.")
    parser.add_argument(
        "--version",
        type=int,
        default=None,
        help="Delta version to export.",
    )
    parser.add_argument(
        "--timestamp",
        default=None,
        help="Timestamp to export (as accepted by Delta Lake).",
    )
    parser.add_argument(
        "--storage-option",
        action="append",
        default=[],
        help="Storage option key=value (repeatable).",
    )
    parser.add_argument(
        "--compression",
        default=None,
        help="Optional Parquet compression codec (default: engine default).",
    )
    parser.add_argument(
        "--report-path",
        default=None,
        help="Optional path for JSON report (default: stdout).",
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


def _write_report(report: ExportReport, report_path: str | None) -> None:
    payload = json.dumps(asdict(report), indent=2, sort_keys=True)
    if report_path is None:
        sys.stdout.write(payload + "\n")
        return
    Path(report_path).write_text(payload + "\n", encoding="utf-8")


def export_delta_snapshot(
    path: str,
    target: str,
    *,
    options: DeltaExportOptions | None = None,
) -> ExportReport:
    """Export a Delta snapshot to Parquet and return a report.

    Returns
    -------
    ExportReport
        Snapshot export report payload.

    Raises
    ------
    ValueError
        Raised when both version and timestamp are supplied.
    """
    resolved = options or DeltaExportOptions()
    if resolved.version is not None and resolved.timestamp is not None:
        msg = "Specify only one of version or timestamp."
        raise ValueError(msg)
    table = open_delta_table(
        path,
        storage_options=resolved.storage_options,
        version=resolved.version,
        timestamp=resolved.timestamp,
    )
    arrow_table = table.to_pyarrow_table()
    target_path = Path(target)
    if target_path.parent:
        target_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(arrow_table, target_path, compression=resolved.compression)
    return ExportReport(
        path=path,
        target=str(target_path),
        version=resolved.version,
        timestamp=resolved.timestamp,
        rows=int(arrow_table.num_rows),
        schema_fingerprint=schema_fingerprint(arrow_table.schema),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run Delta snapshot export with CLI-provided options.

    Returns
    -------
    int
        Exit status code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        storage_options = _parse_kv_pairs(args.storage_option)
    except ValueError as exc:
        parser.error(str(exc))
        return 2
    options = DeltaExportOptions(
        version=args.version,
        timestamp=args.timestamp,
        storage_options=storage_options or None,
        compression=args.compression,
    )
    report = export_delta_snapshot(args.path, args.target, options=options)
    _write_report(report, args.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
