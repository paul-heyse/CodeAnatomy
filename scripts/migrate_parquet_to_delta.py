#!/usr/bin/env python3
"""Migrate Parquet data to Delta Lake using the DataFusion write pipeline."""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq

from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.write_pipeline import WriteFormat, WriteMode, WritePipeline, WriteRequest
from storage.deltalake import delta_table_version


@dataclass(frozen=True)
class MigrationReport:
    """Report payload for Parquet-to-Delta migrations."""

    source: str
    target: str
    mode: str
    delta_version: int | None


@dataclass(frozen=True)
class MigrationOptions:
    """Options for Parquet-to-Delta migrations."""

    mode: WriteMode
    schema_mode: str | None
    partition_by: tuple[str, ...]
    storage_options: Mapping[str, str] | None
    log_storage_options: Mapping[str, str] | None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Migrate Parquet data to a Delta table.")
    parser.add_argument("--source", required=True, help="Source Parquet file or directory.")
    parser.add_argument("--target", required=True, help="Target Delta table path.")
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=("overwrite", "append", "error"),
        help="Write mode for the Delta table (default: overwrite).",
    )
    parser.add_argument(
        "--schema-mode",
        default=None,
        choices=("merge", "overwrite"),
        help="Delta schema mode (merge/overwrite).",
    )
    parser.add_argument(
        "--partition-by",
        default=None,
        help="Comma-separated partition columns for Delta writes.",
    )
    parser.add_argument(
        "--storage-option",
        action="append",
        default=[],
        help="Storage option key=value (repeatable).",
    )
    parser.add_argument(
        "--log-storage-option",
        action="append",
        default=[],
        help="Log storage option key=value (repeatable).",
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


def _parse_partition_by(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    parts = [part.strip() for part in value.split(",") if part.strip()]
    return tuple(parts)


def _write_report(report: MigrationReport, report_path: str | None) -> None:
    payload = json.dumps(asdict(report), indent=2, sort_keys=True)
    if report_path is None:
        sys.stdout.write(payload + "\n")
        return
    Path(report_path).write_text(payload + "\n", encoding="utf-8")


def _mode_from_string(value: str) -> WriteMode:
    if value == "append":
        return WriteMode.APPEND
    if value == "overwrite":
        return WriteMode.OVERWRITE
    if value == "error":
        return WriteMode.ERROR
    msg = f"Unsupported write mode: {value!r}."
    raise ValueError(msg)


def _format_options(
    *,
    schema_mode: str | None,
    storage_options: Mapping[str, str] | None,
    log_storage_options: Mapping[str, str] | None,
) -> dict[str, object]:
    options: dict[str, object] = {}
    if schema_mode is not None:
        options["schema_mode"] = schema_mode
    if storage_options:
        options["storage_options"] = dict(storage_options)
    if log_storage_options:
        options["log_storage_options"] = dict(log_storage_options)
    return options


def migrate_parquet_to_delta(
    source: str,
    target: str,
    *,
    options: MigrationOptions,
) -> MigrationReport:
    """Migrate Parquet data to Delta using DataFusion.

    Parameters
    ----------
    source
        Source Parquet file or directory.
    target
        Destination Delta table path.
    options
        Migration options controlling schema, partitioning, and storage.

    Returns
    -------
    MigrationReport
        Migration report payload with resolved Delta version.
    """
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    temp_name: str | None = None

    try:
        read_parquet = getattr(ctx, "read_parquet", None)
        if callable(read_parquet):
            df = read_parquet(source)
        else:
            table = pq.read_table(source)
            temp_name = f"__parquet_{uuid4().hex}"
            adapter.register_record_batches(temp_name, [list(table.to_batches())])
            df = ctx.table(temp_name)

        options_payload = _format_options(
            schema_mode=options.schema_mode,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
        )
        request = WriteRequest(
            source=df,
            destination=target,
            format=WriteFormat.DELTA,
            mode=options.mode,
            partition_by=options.partition_by,
            format_options=options_payload or None,
        )
        pipeline.write(request)
        version = delta_table_version(
            target,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
        )
        return MigrationReport(
            source=source,
            target=target,
            mode=options.mode.name.lower(),
            delta_version=version,
        )
    finally:
        if temp_name is not None:
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table(temp_name)


def main(argv: Sequence[str] | None = None) -> int:
    """Run Parquet-to-Delta migration with CLI-provided options.

    Returns
    -------
    int
        Exit status code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        storage_options = _parse_kv_pairs(args.storage_option)
        log_storage_options = _parse_kv_pairs(args.log_storage_option)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    options = MigrationOptions(
        mode=_mode_from_string(args.mode),
        schema_mode=args.schema_mode,
        partition_by=_parse_partition_by(args.partition_by),
        storage_options=storage_options or None,
        log_storage_options=log_storage_options or None,
    )
    report = migrate_parquet_to_delta(args.source, args.target, options=options)
    _write_report(report, args.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
