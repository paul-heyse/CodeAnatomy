"""Cache introspection and diagnostics for DataFusion runtime."""

from __future__ import annotations

import importlib
import time
from dataclasses import dataclass
from typing import Any

from datafusion import SessionContext

from datafusion_engine.schema_introspection import SchemaIntrospector


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True)
class CacheConfigSnapshot:
    """Configuration snapshot for DataFusion caches.

    Attributes
    ----------
    list_files_cache_ttl : str | None
        TTL setting for list_files cache (e.g., '2m', '5m').
    list_files_cache_limit : str | None
        Size limit for list_files cache (e.g., '128 MiB').
    metadata_cache_limit : str | None
        Size limit for metadata cache (e.g., '256 MiB').
    predicate_cache_size : str | None
        Size limit for predicate cache (e.g., '64 MiB').
    """

    list_files_cache_ttl: str | None
    list_files_cache_limit: str | None
    metadata_cache_limit: str | None
    predicate_cache_size: str | None


@dataclass(frozen=True)
class CacheStateSnapshot:
    """Runtime state snapshot for a DataFusion cache.

    Attributes
    ----------
    cache_name : str
        Name of the cache (e.g., 'list_files', 'metadata').
    event_time_unix_ms : int
        Timestamp when snapshot was taken.
    entry_count : int | None
        Number of entries in the cache, if available.
    hit_count : int | None
        Number of cache hits, if available.
    miss_count : int | None
        Number of cache misses, if available.
    eviction_count : int | None
        Number of cache evictions, if available.
    config_ttl : str | None
        TTL configuration for this cache.
    config_limit : str | None
        Size limit configuration for this cache.
    """

    cache_name: str
    event_time_unix_ms: int
    entry_count: int | None
    hit_count: int | None
    miss_count: int | None
    eviction_count: int | None
    config_ttl: str | None
    config_limit: str | None

    def to_row(self) -> dict[str, object]:
        """Return a row mapping for diagnostics sinks.

        Returns
        -------
        dict[str, object]
            Row mapping for diagnostics table ingestion.
        """
        return {
            "cache_name": self.cache_name,
            "event_time_unix_ms": self.event_time_unix_ms,
            "entry_count": self.entry_count,
            "hit_count": self.hit_count,
            "miss_count": self.miss_count,
            "eviction_count": self.eviction_count,
            "config_ttl": self.config_ttl,
            "config_limit": self.config_limit,
        }


def _extract_cache_config(settings: list[dict[str, object]]) -> CacheConfigSnapshot:
    """Extract cache configuration from df_settings.

    Parameters
    ----------
    settings
        Settings rows from information_schema.df_settings.

    Returns
    -------
    CacheConfigSnapshot
        Cache configuration snapshot.
    """
    settings_map = {str(row.get("name")): str(row.get("value")) for row in settings}
    return CacheConfigSnapshot(
        list_files_cache_ttl=settings_map.get("datafusion.runtime.list_files_cache_ttl"),
        list_files_cache_limit=settings_map.get("datafusion.runtime.list_files_cache_limit"),
        metadata_cache_limit=settings_map.get("datafusion.runtime.metadata_cache_limit"),
        predicate_cache_size=settings_map.get(
            "datafusion.execution.parquet.max_predicate_cache_size"
        ),
    )


def _cache_snapshot_from_table(
    ctx: SessionContext,
    *,
    table_name: str,
) -> CacheStateSnapshot | None:
    query = f"SELECT * FROM {table_name}()"
    try:
        table = ctx.sql(query).to_arrow_table()
    except (RuntimeError, TypeError, ValueError):
        return None
    rows = table.to_pylist()
    if not rows:
        return None
    row = rows[0]
    return CacheStateSnapshot(
        cache_name=str(row.get("cache_name") or table_name),
        event_time_unix_ms=int(row.get("event_time_unix_ms") or _now_ms()),
        entry_count=int(row["entry_count"]) if row.get("entry_count") is not None else None,
        hit_count=int(row["hit_count"]) if row.get("hit_count") is not None else None,
        miss_count=int(row["miss_count"]) if row.get("miss_count") is not None else None,
        eviction_count=(
            int(row["eviction_count"]) if row.get("eviction_count") is not None else None
        ),
        config_ttl=str(row.get("config_ttl")) if row.get("config_ttl") is not None else None,
        config_limit=str(row.get("config_limit")) if row.get("config_limit") is not None else None,
    )


def metadata_cache_snapshot(ctx: SessionContext) -> CacheStateSnapshot:
    """Capture metadata cache state snapshot.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns
    -------
    CacheStateSnapshot
        Snapshot of metadata cache state.

    Notes
    -----
    Currently DataFusion does not expose cache introspection table functions,
    so this returns a snapshot with configuration only and no runtime stats.
    Future DataFusion versions may add table functions like
    ``SELECT * FROM metadata_cache()`` for runtime introspection.
    """
    snapshot = _cache_snapshot_from_table(ctx, table_name="metadata_cache")
    if snapshot is not None:
        return snapshot
    introspector = SchemaIntrospector(ctx, sql_options=None)
    settings = introspector.settings_snapshot()
    config = _extract_cache_config(settings)
    now = _now_ms()
    return CacheStateSnapshot(
        cache_name="metadata",
        event_time_unix_ms=now,
        entry_count=None,
        hit_count=None,
        miss_count=None,
        eviction_count=None,
        config_ttl=None,
        config_limit=config.metadata_cache_limit,
    )


def list_files_cache_snapshot(ctx: SessionContext) -> CacheStateSnapshot:
    """Capture list_files cache state snapshot.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns
    -------
    CacheStateSnapshot
        Snapshot of list_files cache state.

    Notes
    -----
    Currently DataFusion does not expose cache introspection table functions,
    so this returns a snapshot with configuration only and no runtime stats.
    Future DataFusion versions may add table functions like
    ``SELECT * FROM list_files_cache()`` for runtime introspection.
    """
    snapshot = _cache_snapshot_from_table(ctx, table_name="list_files_cache")
    if snapshot is not None:
        return snapshot
    introspector = SchemaIntrospector(ctx, sql_options=None)
    settings = introspector.settings_snapshot()
    config = _extract_cache_config(settings)
    now = _now_ms()
    return CacheStateSnapshot(
        cache_name="list_files",
        event_time_unix_ms=now,
        entry_count=None,
        hit_count=None,
        miss_count=None,
        eviction_count=None,
        config_ttl=config.list_files_cache_ttl,
        config_limit=config.list_files_cache_limit,
    )


def predicate_cache_snapshot(ctx: SessionContext) -> CacheStateSnapshot:
    """Capture predicate cache state snapshot.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns
    -------
    CacheStateSnapshot
        Snapshot of predicate cache state.

    Notes
    -----
    Currently DataFusion does not expose cache introspection table functions,
    so this returns a snapshot with configuration only and no runtime stats.
    """
    snapshot = _cache_snapshot_from_table(ctx, table_name="predicate_cache")
    if snapshot is not None:
        return snapshot
    introspector = SchemaIntrospector(ctx, sql_options=None)
    settings = introspector.settings_snapshot()
    config = _extract_cache_config(settings)
    now = _now_ms()
    return CacheStateSnapshot(
        cache_name="predicate",
        event_time_unix_ms=now,
        entry_count=None,
        hit_count=None,
        miss_count=None,
        eviction_count=None,
        config_ttl=None,
        config_limit=config.predicate_cache_size,
    )


def capture_cache_diagnostics(ctx: SessionContext) -> dict[str, Any]:
    """Capture cache configuration and state for diagnostics.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns
    -------
    dict[str, Any]
        Cache diagnostics payload including configuration and state snapshots.

    Examples
    --------
    >>> ctx = SessionContext()
    >>> diagnostics = capture_cache_diagnostics(ctx)
    >>> diagnostics["cache_snapshots"]
    [{'cache_name': 'list_files', ...}, {'cache_name': 'metadata', ...}]
    """
    introspector = SchemaIntrospector(ctx, sql_options=None)
    settings = introspector.settings_snapshot()
    config = _extract_cache_config(settings)

    snapshots = [
        list_files_cache_snapshot(ctx).to_row(),
        metadata_cache_snapshot(ctx).to_row(),
        predicate_cache_snapshot(ctx).to_row(),
    ]

    return {
        "config": {
            "list_files_cache_ttl": config.list_files_cache_ttl,
            "list_files_cache_limit": config.list_files_cache_limit,
            "metadata_cache_limit": config.metadata_cache_limit,
            "predicate_cache_size": config.predicate_cache_size,
        },
        "cache_snapshots": snapshots,
    }


def register_cache_introspection_functions(ctx: SessionContext) -> None:
    """Register cache introspection table functions in the SessionContext.

    Parameters
    ----------
    ctx
        DataFusion session context to register functions into.

    Notes
    -----
    Currently this is a placeholder. When DataFusion adds native support
    for cache introspection table functions (e.g., ``list_files_cache()``,
    ``metadata_cache()``), this function will register them as UDTFs.

    For now, use the snapshot functions directly:
    - ``list_files_cache_snapshot(ctx)``
    - ``metadata_cache_snapshot(ctx)``
    - ``predicate_cache_snapshot(ctx)``

    Examples
    --------
    >>> ctx = SessionContext()
    >>> register_cache_introspection_functions(ctx)
    # Future: ctx.sql("SELECT * FROM list_files_cache()").collect()

    Raises
    ------
    ImportError
        Raised when the datafusion_ext module is unavailable.
    TypeError
        Raised when datafusion_ext does not provide register_cache_tables.
    """
    introspector = SchemaIntrospector(ctx, sql_options=None)
    settings = introspector.settings_snapshot()
    config = _extract_cache_config(settings)
    payload = {
        "list_files_cache_ttl": config.list_files_cache_ttl,
        "list_files_cache_limit": config.list_files_cache_limit,
        "metadata_cache_limit": config.metadata_cache_limit,
        "predicate_cache_size": config.predicate_cache_size,
    }
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError as exc:
        msg = "datafusion_ext is required for cache introspection tables."
        raise ImportError(msg) from exc
    register = getattr(module, "register_cache_tables", None)
    if not callable(register):
        msg = "datafusion_ext.register_cache_tables is unavailable."
        raise TypeError(msg)
    register(ctx, payload)


__all__ = [
    "CacheConfigSnapshot",
    "CacheStateSnapshot",
    "capture_cache_diagnostics",
    "list_files_cache_snapshot",
    "metadata_cache_snapshot",
    "predicate_cache_snapshot",
    "register_cache_introspection_functions",
]
