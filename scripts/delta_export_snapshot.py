#!/usr/bin/env python3
"""Clone a Delta snapshot into a Delta table."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from datafusion_engine.arrow_schema.abi import schema_fingerprint
from datafusion_engine.delta_control_plane import DeltaProviderRequest, delta_provider_from_session
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.table_provider_capsule import TableProviderCapsule
from storage.deltalake import DeltaWriteOptions, write_delta_table
from utils.uuid_factory import uuid7_hex


@dataclass(frozen=True)
class CloneReport:
    """Report payload for Delta snapshot clones."""

    path: str
    target: str
    version: int | None
    timestamp: str | None
    rows: int
    schema_fingerprint: str


@dataclass(frozen=True)
class DeltaCloneOptions:
    """Options for cloning a Delta snapshot."""

    version: int | None = None
    timestamp: str | None = None
    storage_options: dict[str, str] | None = None
    mode: str = "overwrite"
    schema_mode: str | None = "overwrite"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clone a Delta snapshot into Delta storage.")
    parser.add_argument("--path", required=True, help="Delta table path.")
    parser.add_argument("--target", required=True, help="Target Delta table path.")
    parser.add_argument(
        "--version",
        type=int,
        default=None,
        help="Delta version to clone.",
    )
    parser.add_argument(
        "--timestamp",
        default=None,
        help="Timestamp to clone (as accepted by Delta Lake).",
    )
    parser.add_argument(
        "--storage-option",
        action="append",
        default=[],
        help="Storage option key=value (repeatable).",
    )
    parser.add_argument(
        "--mode",
        default=None,
        help="Delta write mode (default: overwrite).",
    )
    parser.add_argument(
        "--schema-mode",
        default=None,
        help="Delta schema mode (default: overwrite).",
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


def _write_report(report: CloneReport, report_path: str | None) -> None:
    payload = json.dumps(asdict(report), indent=2, sort_keys=True)
    if report_path is None:
        sys.stdout.write(payload + "\n")
        return
    Path(report_path).write_text(payload + "\n", encoding="utf-8")


def clone_delta_snapshot(
    path: str,
    target: str,
    *,
    options: DeltaCloneOptions | None = None,
) -> CloneReport:
    """Clone a Delta snapshot into Delta storage and return a report.

    Returns
    -------
    CloneReport
        Snapshot clone report payload.

    Raises
    ------
    ValueError
        Raised when both version and timestamp are supplied.
    """
    resolved = options or DeltaCloneOptions()
    if resolved.version is not None and resolved.timestamp is not None:
        msg = "Specify only one of version or timestamp."
        raise ValueError(msg)
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
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
    adapter.register_delta_table_provider(
        table_name,
        TableProviderCapsule(bundle.provider),
        overwrite=True,
    )
    try:
        arrow_table = ctx.table(table_name).to_arrow_table()
    finally:
        adapter.deregister_table(table_name)
    target_path = Path(target)
    if target_path.parent:
        target_path.parent.mkdir(parents=True, exist_ok=True)
    commit_metadata: dict[str, str] = {"clone_source": path}
    if resolved.version is not None:
        commit_metadata["source_version"] = str(resolved.version)
    if resolved.timestamp is not None:
        commit_metadata["source_timestamp"] = resolved.timestamp
    write_delta_table(
        arrow_table,
        str(target_path),
        options=DeltaWriteOptions(
            mode=resolved.mode or "overwrite",
            schema_mode=resolved.schema_mode or "overwrite",
            commit_metadata=commit_metadata,
            storage_options=resolved.storage_options,
        ),
    )
    return CloneReport(
        path=path,
        target=str(target_path),
        version=resolved.version,
        timestamp=resolved.timestamp,
        rows=int(arrow_table.num_rows),
        schema_fingerprint=schema_fingerprint(arrow_table.schema),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run Delta snapshot clone with CLI-provided options.

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
    options = DeltaCloneOptions(
        version=args.version,
        timestamp=args.timestamp,
        storage_options=storage_options or None,
        mode=args.mode or "overwrite",
        schema_mode=args.schema_mode or "overwrite",
    )
    report = clone_delta_snapshot(args.path, args.target, options=options)
    _write_report(report, args.report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
