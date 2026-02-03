"""Delta-backed cache inventory helpers."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.arrow.field_builders import (
    int64_field,
    list_field,
    string_field,
)
from datafusion_engine.dataset.registration import (
    DatasetRegistrationOptions,
    register_dataset_df,
)
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.io.ingest import datafusion_from_arrow
from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from obs.otel.run_context import get_run_id

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


CACHE_INVENTORY_TABLE_NAME = "datafusion_view_cache_inventory_v1"

try:
    _DEFAULT_CACHE_ROOT = Path(__file__).resolve().parents[2] / ".artifacts"
except IndexError:
    _DEFAULT_CACHE_ROOT = Path.cwd() / ".artifacts"


@dataclass(frozen=True)
class CacheInventoryEntry:
    """Inventory entry for cached views."""

    view_name: str
    cache_policy: str
    cache_path: str
    plan_fingerprint: str | None
    plan_identity_hash: str | None
    schema_identity_hash: str | None
    snapshot_version: int | None
    snapshot_timestamp: str | None
    run_id: str | None = None
    result: str | None = None
    row_count: int | None = None
    file_count: int | None = None
    partition_by: tuple[str, ...] = ()
    event_time_unix_ms: int | None = None

    def to_row(self) -> dict[str, object]:
        """Return a JSON-ready row payload.

        Returns
        -------
        dict[str, object]
            JSON-ready representation of the inventory entry.
        """
        run_id = self.run_id or get_run_id()
        return {
            "event_time_unix_ms": self.event_time_unix_ms or int(time.time() * 1000),
            "run_id": run_id,
            "view_name": self.view_name,
            "cache_policy": self.cache_policy,
            "cache_path": self.cache_path,
            "result": self.result,
            "plan_fingerprint": self.plan_fingerprint,
            "plan_identity_hash": self.plan_identity_hash,
            "schema_identity_hash": self.schema_identity_hash,
            "snapshot_version": self.snapshot_version,
            "snapshot_timestamp": self.snapshot_timestamp,
            "row_count": self.row_count,
            "file_count": self.file_count,
            "partition_by": list(self.partition_by),
        }


def ensure_cache_inventory_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
) -> DatasetLocation | None:
    """Ensure the cache inventory table exists and is registered.

    Returns
    -------
    DatasetLocation | None
        Dataset location for the cache inventory table when available.
    """
    table_path = _cache_inventory_root(profile) / CACHE_INVENTORY_TABLE_NAME
    delta_log_path = table_path / "_delta_log"
    has_delta_log = delta_log_path.exists() and any(delta_log_path.glob("*.json"))
    if not has_delta_log:
        profile.record_artifact(
            "cache_inventory_missing_v1",
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": CACHE_INVENTORY_TABLE_NAME,
                "path": str(table_path),
                "error": "Delta log missing; cache inventory bootstrap skipped.",
            },
        )
        return None
    location = DatasetLocation(path=str(table_path), format="delta")
    try:
        register_dataset_df(
            ctx,
            name=CACHE_INVENTORY_TABLE_NAME,
            location=location,
            options=DatasetRegistrationOptions(runtime_profile=profile),
        )
    except (RuntimeError, ValueError, TypeError, OSError, KeyError) as exc:
        profile.record_artifact(
            "cache_inventory_register_failed_v1",
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": CACHE_INVENTORY_TABLE_NAME,
                "path": str(table_path),
                "error": str(exc),
            },
        )
        return None
    return location


def record_cache_inventory_entry(
    profile: DataFusionRuntimeProfile | None,
    *,
    entry: CacheInventoryEntry,
    ctx: SessionContext | None = None,
) -> int | None:
    """Append a cache inventory entry when enabled.

    Parameters
    ----------
    profile
        Runtime profile controlling cache inventory persistence.
    entry
        Inventory entry to append.
    ctx
        Optional session context to reuse for inventory writes. If not provided,
        the runtime profile session context is used.

    Returns
    -------
    int | None
        Delta table version when recorded, if available.
    """
    if profile is None:
        return None
    active_ctx = ctx or profile.session_context()
    location = ensure_cache_inventory_table(active_ctx, profile)
    if location is None:
        return None
    table = pa.Table.from_pylist([entry.to_row()], schema=_cache_inventory_schema())
    df = datafusion_from_arrow(
        active_ctx,
        name=f"{CACHE_INVENTORY_TABLE_NAME}_append",
        value=table,
    )
    pipeline = WritePipeline(ctx=active_ctx, runtime_profile=profile)
    from datafusion_engine.cache.commit_metadata import (
        CacheCommitMetadataRequest,
        cache_commit_metadata,
    )

    commit_metadata = cache_commit_metadata(
        CacheCommitMetadataRequest(
            operation="cache_inventory_append",
            cache_policy="cache_inventory",
            cache_scope="ledger",
            cache_key=entry.view_name,
            result=entry.result,
        )
    )
    result = pipeline.write(
        WriteRequest(
            source=df,
            destination=str(location.path),
            format=WriteFormat.DELTA,
            mode=WriteMode.APPEND,
            format_options={"commit_metadata": commit_metadata},
        )
    )
    if result.delta_result is None:
        return None
    return result.delta_result.version


def _bootstrap_cache_inventory_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    table_path: Path,
    schema: pa.Schema,
) -> None:
    _ = (ctx, profile)
    table_path.parent.mkdir(parents=True, exist_ok=True)
    bootstrap_row = {
        "event_time_unix_ms": 0,
        "view_name": "__bootstrap__",
        "cache_policy": "bootstrap",
        "cache_path": "",
    }
    table = pa.Table.from_pylist([bootstrap_row], schema=schema)
    from deltalake.writer import write_deltalake

    write_deltalake(
        str(table_path),
        table,
        mode="overwrite",
        schema_mode="overwrite",
    )


def _cache_inventory_root(profile: DataFusionRuntimeProfile) -> Path:
    root_value = profile.plan_artifacts_root
    if root_value:
        return Path(root_value).expanduser()
    return _DEFAULT_CACHE_ROOT


def _cache_inventory_schema() -> pa.Schema:
    return pa.schema(
        [
            int64_field("event_time_unix_ms"),
            string_field("run_id", nullable=True),
            string_field("view_name"),
            string_field("cache_policy"),
            string_field("cache_path"),
            string_field("result", nullable=True),
            string_field("plan_fingerprint", nullable=True),
            string_field("plan_identity_hash", nullable=True),
            string_field("schema_identity_hash", nullable=True),
            int64_field("snapshot_version", nullable=True),
            string_field("snapshot_timestamp", nullable=True),
            int64_field("row_count", nullable=True),
            int64_field("file_count", nullable=True),
            list_field("partition_by", string_field("item"), nullable=True),
        ]
    )


def delta_report_file_count(report: Mapping[str, object] | None) -> int | None:
    """Extract a file count from a Delta write report payload.

    Returns
    -------
    int | None
        File count when present in the payload.
    """
    if report is None:
        return None
    payloads: list[Mapping[str, object]] = []
    payloads.append(report)
    metrics = report.get("metrics") if isinstance(report, Mapping) else None
    if isinstance(metrics, Mapping):
        payloads.append(metrics)
    for payload in payloads:
        for key in (
            "numFiles",
            "num_files",
            "files",
            "added_files",
            "files_added",
            "numAddFiles",
            "numAddedFiles",
        ):
            if key not in payload:
                continue
            value = _coerce_int(payload.get(key))
            if value is not None:
                return value
    return None


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return None
    return None


def cache_inventory_schema() -> pa.Schema:
    """Return the cache inventory table schema.

    Returns
    -------
    pa.Schema
        Arrow schema for the cache inventory table.
    """
    return _cache_inventory_schema()


__all__ = [
    "CACHE_INVENTORY_TABLE_NAME",
    "CacheInventoryEntry",
    "cache_inventory_schema",
    "delta_report_file_count",
    "ensure_cache_inventory_table",
    "record_cache_inventory_entry",
]
