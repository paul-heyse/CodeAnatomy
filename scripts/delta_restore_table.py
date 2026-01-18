#!/usr/bin/env python3
"""Restore a Delta table to a prior version or timestamp."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from deltalake import DeltaTable


@dataclass(frozen=True)
class RestoreReport:
    """Report payload for Delta restore operations."""

    path: str
    version: int | None
    timestamp: str | None
    result: object | None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Restore a Delta table to a prior snapshot.")
    parser.add_argument("--path", required=True, help="Delta table path.")
    parser.add_argument(
        "--version",
        type=int,
        default=None,
        help="Delta table version to restore.",
    )
    parser.add_argument(
        "--timestamp",
        default=None,
        help="Timestamp to restore (as accepted by Delta Lake).",
    )
    parser.add_argument(
        "--storage-option",
        action="append",
        default=[],
        help="Storage option key=value (repeatable).",
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


def _write_report(report: RestoreReport, report_path: str | None) -> None:
    payload = json.dumps(asdict(report), indent=2, sort_keys=True, default=str)
    if report_path is None:
        sys.stdout.write(payload + "\n")
        return
    Path(report_path).write_text(payload + "\n", encoding="utf-8")


def restore_delta_table(
    *,
    path: str,
    version: int | None,
    timestamp: str | None,
    storage_options: dict[str, str] | None,
) -> RestoreReport:
    """Restore a Delta table and return a report payload.

    Returns
    -------
    RestoreReport
        Restore report payload.

    Raises
    ------
    ValueError
        Raised when neither or both of version/timestamp are provided.
    """
    if version is None and timestamp is None:
        msg = "Either --version or --timestamp must be specified."
        raise ValueError(msg)
    if version is not None and timestamp is not None:
        msg = "Specify only one of --version or --timestamp."
        raise ValueError(msg)
    table = DeltaTable(path, storage_options=storage_options)
    result = table.restore(version=version, timestamp=timestamp)
    return RestoreReport(path=path, version=version, timestamp=timestamp, result=result)


def main(argv: Sequence[str] | None = None) -> int:
    """Run Delta restore with CLI-provided options.

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
    try:
        report = restore_delta_table(
            path=args.path,
            version=args.version,
            timestamp=args.timestamp,
            storage_options=storage_options or None,
        )
    except ValueError as exc:
        parser.error(str(exc))
        return 2
    _write_report(report, args.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
