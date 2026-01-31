"""Delta-backed cache inventory helpers."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.arrow.field_builders import (
    int64_field,
    list_field,
    string_field,
)
from datafusion_engine.dataset.registration import register_dataset_df
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.io.ingest import datafusion_from_arrow
from datafusion_engine.io.write import (
    WriteFormat,
    WriteMode,
    WritePipeline,
    WriteRequest,
)

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
    partition_by: tuple[str, ...] = ()
    event_time_unix_ms: int | None = None

    def to_row(self) -> dict[str, object]:
        """Return a JSON-ready row payload.

        Returns
        -------
        dict[str, object]
            JSON-ready representation of the inventory entry.
        """
        return {
            "event_time_unix_ms": self.event_time_unix_ms or int(time.time() * 1000),
            "view_name": self.view_name,
            "cache_policy": self.cache_policy,
            "cache_path": self.cache_path,
            "plan_fingerprint": self.plan_fingerprint,
            "plan_identity_hash": self.plan_identity_hash,
            "schema_identity_hash": self.schema_identity_hash,
            "snapshot_version": self.snapshot_version,
            "snapshot_timestamp": self.snapshot_timestamp,
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
    if not table_path.exists():
        _bootstrap_cache_inventory_table(
            ctx,
            profile,
            table_path=table_path,
            schema=_cache_inventory_schema(),
        )
    location = DatasetLocation(path=str(table_path), format="delta")
    register_dataset_df(
        ctx, name=CACHE_INVENTORY_TABLE_NAME, location=location, runtime_profile=profile
    )
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
    result = pipeline.write(
        WriteRequest(
            source=df,
            destination=str(location.path),
            format=WriteFormat.DELTA,
            mode=WriteMode.APPEND,
            format_options={
                "commit_metadata": {
                    "operation": "view_cache_inventory_append",
                    "view_name": entry.view_name,
                }
            },
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
    table_path.parent.mkdir(parents=True, exist_ok=True)
    empty = pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in schema],
        schema=schema,
    )
    df = datafusion_from_arrow(ctx, name=f"{table_path.name}_bootstrap", value=empty)
    pipeline = WritePipeline(ctx=ctx, runtime_profile=profile)
    pipeline.write(
        WriteRequest(
            source=df,
            destination=str(table_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
            format_options={
                "commit_metadata": {
                    "operation": "view_cache_inventory_bootstrap",
                    "table": table_path.name,
                }
            },
        )
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
            string_field("view_name"),
            string_field("cache_policy"),
            string_field("cache_path"),
            string_field("plan_fingerprint", nullable=True),
            string_field("plan_identity_hash", nullable=True),
            string_field("schema_identity_hash", nullable=True),
            int64_field("snapshot_version", nullable=True),
            string_field("snapshot_timestamp", nullable=True),
            list_field("partition_by", string_field("item"), nullable=True),
        ]
    )


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
    "ensure_cache_inventory_table",
    "record_cache_inventory_entry",
]
