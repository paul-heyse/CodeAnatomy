"""Delta-backed cache ledger tables for run summaries and snapshot registry."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.arrow.field_builders import int64_field, string_field
from datafusion_engine.arrow.interop import empty_table_for_schema
from datafusion_engine.cache._ports import _WriterPort
from datafusion_engine.cache.commit_metadata import (
    CacheCommitMetadataRequest,
    cache_commit_metadata,
)
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.io.ingest import datafusion_from_arrow
from datafusion_engine.io.write_core import (
    WriteFormat,
    WriteMode,
    WriteRequest,
    build_write_pipeline,
)
from datafusion_engine.session.facade import DataFusionExecutionFacade
from obs.otel import get_run_id
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


CACHE_RUN_SUMMARY_TABLE_NAME = "datafusion_cache_run_summary_v1"
CACHE_SNAPSHOT_REGISTRY_TABLE_NAME = "datafusion_cache_snapshot_registry_v1"


def _append_source_name(table_name: str) -> str:
    return f"{table_name}_append_{uuid7_hex()}"


def _default_write_pipeline(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
) -> _WriterPort:
    return build_write_pipeline(ctx=ctx, runtime_profile=profile)


@dataclass(frozen=True)
class CacheRunSummary:
    """Run-level summary payload for cache operations."""

    run_id: str
    start_time_unix_ms: int
    end_time_unix_ms: int | None
    cache_root: str
    total_writes: int
    total_reads: int
    error_count: int
    event_time_unix_ms: int | None = None

    def to_row(self) -> dict[str, object]:
        """Return a JSON-ready summary payload.

        Returns:
        -------
        dict[str, object]
            JSON-ready representation of the summary.
        """
        return {
            "event_time_unix_ms": self.event_time_unix_ms or int(time.time() * 1000),
            "run_id": self.run_id,
            "start_time_unix_ms": self.start_time_unix_ms,
            "end_time_unix_ms": self.end_time_unix_ms,
            "cache_root": self.cache_root,
            "total_writes": self.total_writes,
            "total_reads": self.total_reads,
            "error_count": self.error_count,
        }


@dataclass(frozen=True)
class CacheSnapshotRegistryEntry:
    """Registry payload for metadata cache snapshots."""

    snapshot_name: str
    cache_table: str
    cache_path: str | None
    snapshot_version: int | None
    error: str | None
    run_id: str | None = None
    event_time_unix_ms: int | None = None

    def to_row(self) -> dict[str, object]:
        """Return a JSON-ready snapshot registry row.

        Returns:
        -------
        dict[str, object]
            JSON-ready representation of the registry entry.
        """
        resolved_run_id = self.run_id or get_run_id()
        return {
            "event_time_unix_ms": self.event_time_unix_ms or int(time.time() * 1000),
            "run_id": resolved_run_id,
            "snapshot_name": self.snapshot_name,
            "cache_table": self.cache_table,
            "cache_path": self.cache_path,
            "snapshot_version": self.snapshot_version,
            "error": self.error,
        }


def ensure_cache_run_summary_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    write_pipeline_factory: Callable[[SessionContext, DataFusionRuntimeProfile], _WriterPort]
    | None = None,
) -> DatasetLocation | None:
    """Ensure the cache run summary table exists and is registered.

    Returns:
    -------
    DatasetLocation | None
        Dataset location for the cache run summary table when available.
    """
    table_path = _cache_ledger_root(profile) / CACHE_RUN_SUMMARY_TABLE_NAME
    if not table_path.exists():
        _bootstrap_cache_ledger_table(
            ctx,
            profile,
            table_path=table_path,
            schema=_cache_run_summary_schema(),
            operation="cache_run_summary_bootstrap",
            write_pipeline_factory=write_pipeline_factory,
        )
    location = DatasetLocation(path=str(table_path), format="delta")
    DataFusionExecutionFacade(
        ctx=ctx,
        runtime_profile=profile,
    ).register_dataset(
        name=CACHE_RUN_SUMMARY_TABLE_NAME,
        location=location,
        overwrite=True,
    )
    return location


def record_cache_run_summary(
    profile: DataFusionRuntimeProfile | None,
    *,
    summary: CacheRunSummary,
    ctx: SessionContext | None = None,
    write_pipeline_factory: Callable[[SessionContext, DataFusionRuntimeProfile], _WriterPort]
    | None = None,
) -> int | None:
    """Append a cache run summary into the ledger table.

    Returns:
    -------
    int | None
        Delta table version when recorded, if available.
    """
    if profile is None:
        return None
    active_ctx = ctx or profile.session_context()
    pipeline_factory = write_pipeline_factory or _default_write_pipeline
    location = ensure_cache_run_summary_table(
        active_ctx,
        profile,
        write_pipeline_factory=pipeline_factory,
    )
    if location is None:
        return None
    table = pa.Table.from_pylist([summary.to_row()], schema=_cache_run_summary_schema())
    df = datafusion_from_arrow(
        active_ctx,
        name=_append_source_name(CACHE_RUN_SUMMARY_TABLE_NAME),
        value=table,
    )
    commit_metadata = cache_commit_metadata(
        CacheCommitMetadataRequest(
            operation="cache_run_summary_append",
            cache_policy="cache_ledger",
            cache_scope="ledger",
            cache_key=summary.run_id,
            result="write",
        )
    )
    pipeline = pipeline_factory(active_ctx, profile)
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
    version = result.delta_result.version
    DataFusionExecutionFacade(
        ctx=active_ctx,
        runtime_profile=profile,
    ).register_dataset(
        name=CACHE_RUN_SUMMARY_TABLE_NAME,
        location=DatasetLocation(
            path=str(location.path),
            format="delta",
            delta_version=version,
        ),
        overwrite=True,
    )
    return version


def ensure_cache_snapshot_registry_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    write_pipeline_factory: Callable[[SessionContext, DataFusionRuntimeProfile], _WriterPort]
    | None = None,
) -> DatasetLocation | None:
    """Ensure the cache snapshot registry table exists and is registered.

    Returns:
    -------
    DatasetLocation | None
        Dataset location for the cache snapshot registry table when available.
    """
    table_path = _cache_ledger_root(profile) / CACHE_SNAPSHOT_REGISTRY_TABLE_NAME
    if not table_path.exists():
        _bootstrap_cache_ledger_table(
            ctx,
            profile,
            table_path=table_path,
            schema=_cache_snapshot_registry_schema(),
            operation="cache_snapshot_registry_bootstrap",
            write_pipeline_factory=write_pipeline_factory,
        )
    location = DatasetLocation(path=str(table_path), format="delta")
    DataFusionExecutionFacade(
        ctx=ctx,
        runtime_profile=profile,
    ).register_dataset(
        name=CACHE_SNAPSHOT_REGISTRY_TABLE_NAME,
        location=location,
        overwrite=True,
    )
    return location


def record_cache_snapshot_registry(
    profile: DataFusionRuntimeProfile | None,
    *,
    entry: CacheSnapshotRegistryEntry,
    ctx: SessionContext | None = None,
    write_pipeline_factory: Callable[[SessionContext, DataFusionRuntimeProfile], _WriterPort]
    | None = None,
) -> int | None:
    """Append a cache snapshot registry entry.

    Returns:
    -------
    int | None
        Delta table version when recorded, if available.
    """
    if profile is None:
        return None
    active_ctx = ctx or profile.session_context()
    pipeline_factory = write_pipeline_factory or _default_write_pipeline
    location = ensure_cache_snapshot_registry_table(
        active_ctx,
        profile,
        write_pipeline_factory=pipeline_factory,
    )
    if location is None:
        return None
    table = pa.Table.from_pylist([entry.to_row()], schema=_cache_snapshot_registry_schema())
    df = datafusion_from_arrow(
        active_ctx,
        name=_append_source_name(CACHE_SNAPSHOT_REGISTRY_TABLE_NAME),
        value=table,
    )
    commit_metadata = cache_commit_metadata(
        CacheCommitMetadataRequest(
            operation="cache_snapshot_registry_append",
            cache_policy="cache_ledger",
            cache_scope="ledger",
            cache_key=entry.snapshot_name,
            result="error" if entry.error else "write",
            extra={"cache_table": entry.cache_table},
        )
    )
    pipeline = pipeline_factory(active_ctx, profile)
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
    version = result.delta_result.version
    DataFusionExecutionFacade(
        ctx=active_ctx,
        runtime_profile=profile,
    ).register_dataset(
        name=CACHE_SNAPSHOT_REGISTRY_TABLE_NAME,
        location=DatasetLocation(
            path=str(location.path),
            format="delta",
            delta_version=version,
        ),
        overwrite=True,
    )
    return version


def _bootstrap_cache_ledger_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    table_path: Path,
    schema: pa.Schema,
    operation: str,
    write_pipeline_factory: Callable[[SessionContext, DataFusionRuntimeProfile], _WriterPort]
    | None = None,
) -> None:
    table_path.parent.mkdir(parents=True, exist_ok=True)
    empty = empty_table_for_schema(schema)
    df = datafusion_from_arrow(ctx, name=f"{table_path.name}_bootstrap", value=empty)
    commit_metadata = cache_commit_metadata(
        CacheCommitMetadataRequest(
            operation=operation,
            cache_policy="cache_ledger",
            cache_scope="ledger",
            cache_key=table_path.name,
            result="write",
        )
    )
    pipeline_factory = write_pipeline_factory or _default_write_pipeline
    pipeline = pipeline_factory(ctx, profile)
    pipeline.write(
        WriteRequest(
            source=df,
            destination=str(table_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
            format_options={"commit_metadata": commit_metadata},
        )
    )


def _cache_ledger_root(profile: DataFusionRuntimeProfile) -> Path:
    return Path(profile.io_ops.cache_root()) / "cache_ledgers"


def _cache_run_summary_schema() -> pa.Schema:
    return pa.schema(
        [
            int64_field("event_time_unix_ms"),
            string_field("run_id"),
            int64_field("start_time_unix_ms"),
            int64_field("end_time_unix_ms", nullable=True),
            string_field("cache_root"),
            int64_field("total_writes"),
            int64_field("total_reads"),
            int64_field("error_count"),
        ]
    )


def _cache_snapshot_registry_schema() -> pa.Schema:
    return pa.schema(
        [
            int64_field("event_time_unix_ms"),
            string_field("run_id", nullable=True),
            string_field("snapshot_name"),
            string_field("cache_table"),
            string_field("cache_path", nullable=True),
            int64_field("snapshot_version", nullable=True),
            string_field("error", nullable=True),
        ]
    )


def cache_run_summary_schema() -> pa.Schema:
    """Return the cache run summary schema.

    Returns:
    -------
    pa.Schema
        Schema for cache run summary rows.
    """
    return _cache_run_summary_schema()


def cache_snapshot_registry_schema() -> pa.Schema:
    """Return the cache snapshot registry schema.

    Returns:
    -------
    pa.Schema
        Schema for cache snapshot registry rows.
    """
    return _cache_snapshot_registry_schema()


__all__ = [
    "CACHE_RUN_SUMMARY_TABLE_NAME",
    "CACHE_SNAPSHOT_REGISTRY_TABLE_NAME",
    "CacheRunSummary",
    "CacheSnapshotRegistryEntry",
    "cache_run_summary_schema",
    "cache_snapshot_registry_schema",
    "ensure_cache_run_summary_table",
    "ensure_cache_snapshot_registry_table",
    "record_cache_run_summary",
    "record_cache_snapshot_registry",
]
