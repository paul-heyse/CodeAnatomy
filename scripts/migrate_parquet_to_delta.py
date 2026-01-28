#!/usr/bin/env python3
"""Migrate a Parquet dataset directory into a Delta table directory."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import pyarrow.dataset as ds

from arrow_utils.schema.abi import schema_fingerprint
from storage.deltalake import (
    DeltaWriteOptions,
    delta_table_version,
    read_table_delta,
    write_table_delta,
)


@dataclass(frozen=True)
class MigrationVerification:
    """Verification results for a migration."""

    row_count_match: bool
    schema_fingerprint_match: bool


@dataclass(frozen=True)
class MigrationReport:
    """Report payload for a parquet-to-delta migration."""

    source: str
    target: str
    rows: int
    schema_fingerprint: str
    delta_version: int | None = None
    verification: MigrationVerification | None = None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate a Parquet dataset directory into a Delta table directory."
    )
    parser.add_argument("--source", required=True, help="Source Parquet dataset directory.")
    parser.add_argument("--target", required=True, help="Target Delta table directory.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the target Delta table if it exists.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute summary only without writing Delta output.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify row counts and schema fingerprints after writing.",
    )
    parser.add_argument(
        "--report-path",
        default=None,
        help="Optional path to write the JSON report (default: stdout).",
    )
    return parser


def _dataset_summary(source: str) -> tuple[int, str]:
    dataset = ds.dataset(source, format="parquet")
    table = dataset.to_table()
    return int(table.num_rows), schema_fingerprint(table.schema)


def _write_report(report: MigrationReport, report_path: str | None) -> None:
    payload = json.dumps(asdict(report), indent=2, sort_keys=True)
    if report_path is None:
        sys.stdout.write(payload + "\n")
        return
    Path(report_path).write_text(payload + "\n", encoding="utf-8")


def _verify_delta(
    target: str, expected_rows: int, expected_fingerprint: str
) -> MigrationVerification:
    table = read_table_delta(target)
    row_count_match = int(table.num_rows) == expected_rows
    schema_match = schema_fingerprint(table.schema) == expected_fingerprint
    return MigrationVerification(
        row_count_match=row_count_match,
        schema_fingerprint_match=schema_match,
    )


def migrate_parquet_to_delta(
    *,
    source: str,
    target: str,
    overwrite: bool,
    dry_run: bool,
    verify: bool,
) -> MigrationReport:
    """Migrate a Parquet dataset directory into a Delta table.

    Returns
    -------
    MigrationReport
        Report of the migration outcome.
    """
    rows, fingerprint = _dataset_summary(source)
    report = MigrationReport(
        source=source, target=target, rows=rows, schema_fingerprint=fingerprint
    )
    if dry_run:
        return report
    mode = "overwrite" if overwrite else "error"
    write_table_delta(
        ds.dataset(source, format="parquet").to_table(),
        target,
        options=DeltaWriteOptions(mode=mode, schema_mode="overwrite" if overwrite else None),
    )
    return MigrationReport(
        source=source,
        target=target,
        rows=rows,
        schema_fingerprint=fingerprint,
        delta_version=delta_table_version(target),
        verification=_verify_delta(target, rows, fingerprint) if verify else None,
    )


def main() -> int:
    """Run the Parquet-to-Delta migration CLI.

    Returns
    -------
    int
        Exit code (0 on success, 1 on verification failure).
    """
    parser = _build_parser()
    args = parser.parse_args()
    report = migrate_parquet_to_delta(
        source=str(Path(args.source)),
        target=str(Path(args.target)),
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
        verify=bool(args.verify),
    )
    _write_report(report, args.report_path)
    if report.verification is None:
        return 0
    if report.verification.row_count_match and report.verification.schema_fingerprint_match:
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
