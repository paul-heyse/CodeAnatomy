"""
DataFusion catalog introspection snapshot.

This module provides point-in-time snapshots of DataFusion catalog state
including tables, columns, functions, and settings. Snapshots ensure
consistency within a compilation unit and enable schema-driven optimization.
"""

from __future__ import annotations

import importlib
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from datafusion import SQLOptions

if TYPE_CHECKING:
    from datafusion import SessionContext, SQLOptions

from datafusion_engine.sql_options import sql_options_for_profile


@dataclass
class IntrospectionSnapshot:
    """
    Point-in-time snapshot of DataFusion catalog state.

    All schema/function queries should go through this snapshot
    to ensure consistency within a compilation unit.

    Attributes
    ----------
    tables : pa.Table
        Table metadata from information_schema.tables
    columns : pa.Table
        Column metadata from information_schema.columns
    schemata : pa.Table
        Schema metadata from information_schema.schemata
    routines : pa.Table | None
        Function metadata from information_schema.routines (if available)
    parameters : pa.Table | None
        Function parameter metadata from information_schema.parameters (if available)
    table_constraints : pa.Table | None
        Constraint metadata from information_schema.table_constraints (if available)
    key_column_usage : pa.Table | None
        Constraint column metadata from information_schema.key_column_usage (if available)
    settings : pa.Table
        DataFusion configuration from information_schema.df_settings
    """

    tables: pa.Table
    columns: pa.Table
    schemata: pa.Table
    routines: pa.Table | None
    parameters: pa.Table | None
    table_constraints: pa.Table | None
    key_column_usage: pa.Table | None
    settings: pa.Table

    @classmethod
    def capture(
        cls,
        ctx: SessionContext,
        *,
        sql_options: SQLOptions | None = None,
    ) -> IntrospectionSnapshot:
        """
        Capture snapshot from SessionContext.

        Queries information_schema views to extract catalog metadata.
        Gracefully handles missing routines/parameters for backends that
        don't support function introspection.

        Parameters
        ----------
        ctx : SessionContext
            DataFusion session to introspect
        sql_options : SQLOptions | None
            SQL options applied to information_schema queries when provided.

        Returns
        -------
        IntrospectionSnapshot
            Snapshot containing all available catalog metadata
        """

        def _table(sql: str) -> pa.Table:
            options = sql_options or SQLOptions()
            df = ctx.sql_with_options(sql, options)
            return df.to_arrow_table()

        tables = _table(
            "SELECT table_catalog, table_schema, table_name, table_type "
            "FROM information_schema.tables"
        )

        columns = _table(
            "SELECT table_catalog, table_schema, table_name, column_name, "
            "ordinal_position, data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "ORDER BY table_catalog, table_schema, table_name, ordinal_position"
        )

        try:
            schemata = _table(
                "SELECT catalog_name, schema_name FROM information_schema.schemata"
            )
        except (ValueError, TypeError, RuntimeError):
            schemata = pa.Table.from_arrays(
                [pa.array([], type=pa.string()), pa.array([], type=pa.string())],
                names=["catalog_name", "schema_name"],
            )

        settings = _table("SELECT name, value FROM information_schema.df_settings")

        # Routines may not be available in all configurations
        try:
            routines = _table(
                "SELECT specific_catalog, specific_schema, specific_name, "
                "routine_catalog, routine_schema, routine_name, routine_type, data_type "
                "FROM information_schema.routines"
            )

            parameters = _table(
                "SELECT specific_catalog, specific_schema, specific_name, "
                "ordinal_position, parameter_mode, parameter_name, data_type "
                "FROM information_schema.parameters "
                "ORDER BY specific_name, ordinal_position"
            )
        except (ValueError, TypeError, RuntimeError):
            # DataFusion versions may not expose routines/parameters views
            routines = None
            parameters = None

        # Constraints may not be available in all configurations
        try:
            table_constraints = _table("SELECT * FROM information_schema.table_constraints")
            key_column_usage = _table("SELECT * FROM information_schema.key_column_usage")
        except (ValueError, TypeError, RuntimeError):
            table_constraints = None
            key_column_usage = None

        return cls(
            tables=tables,
            columns=columns,
            schemata=schemata,
            routines=routines,
            parameters=parameters,
            table_constraints=table_constraints,
            key_column_usage=key_column_usage,
            settings=settings,
        )

    def schema_map(self) -> dict[str, dict[str, str]]:
        """
        Build schema mapping for DataFusion catalog metadata.

        Returns
        -------
        dict[str, dict[str, str]]
            Mapping of table_name -> {column_name: data_type}
        """
        result: dict[str, dict[str, str]] = {}

        for row in self.columns.to_pylist():
            table_name = row["table_name"]
            if table_name not in result:
                result[table_name] = {}
            result[table_name][row["column_name"]] = row["data_type"]

        return result

    def qualified_schema_map(self) -> dict[str, dict[str, dict[str, str]]]:
        """
        Build fully qualified schema mapping.

        Returns
        -------
        dict[str, dict[str, dict[str, str]]]
            Mapping of catalog.schema -> table -> {column: type}
        """
        result: dict[str, dict[str, dict[str, str]]] = {}

        for row in self.columns.to_pylist():
            db_key = f"{row['table_catalog']}.{row['table_schema']}"
            table_name = row["table_name"]

            if db_key not in result:
                result[db_key] = {}
            if table_name not in result[db_key]:
                result[db_key][table_name] = {}

            result[db_key][table_name][row["column_name"]] = row["data_type"]

        return result

    def table_exists(self, name: str) -> bool:
        """
        Check if table exists in snapshot.

        Parameters
        ----------
        name : str
            Table name to check

        Returns
        -------
        bool
            True if table exists in catalog
        """
        return name in self.tables["table_name"].to_pylist()

    def get_table_columns(self, name: str) -> list[tuple[str, str]]:
        """
        Get columns for a table as (name, type) pairs.

        Parameters
        ----------
        name : str
            Table name to query

        Returns
        -------
        list[tuple[str, str]]
            List of (column_name, data_type) tuples ordered by position
        """
        return [
            (row["column_name"], row["data_type"])
            for row in self.columns.to_pylist()
            if row["table_name"] == name
        ]

    def function_signatures(self) -> dict[str, list[tuple[list[str], str]]]:
        """
        Build function signature map.

        Extracts function signatures from routines and parameters metadata.
        Returns empty dict if function introspection is unavailable.

        Returns
        -------
        dict[str, list[tuple[list[str], str]]]
            Mapping of function_name -> [(param_types, return_type), ...]
        """
        if self.routines is None or self.parameters is None:
            return {}

        result: dict[str, list[tuple[list[str], str]]] = {}

        # Group parameters by function
        param_map: dict[str, list[str]] = {}
        for row in self.parameters.to_pylist():
            fn_key = row["specific_name"]
            if fn_key not in param_map:
                param_map[fn_key] = []
            param_map[fn_key].append(row["data_type"])

        # Build signatures
        for row in self.routines.to_pylist():
            fn_name = row["routine_name"]
            specific_name = row["specific_name"]
            return_type = row["data_type"]

            param_types = param_map.get(specific_name, [])

            if fn_name not in result:
                result[fn_name] = []

            result[fn_name].append((param_types, return_type))

        return result


class IntrospectionCache:
    """
    Cached introspection snapshot with invalidation support.

    Holds a single snapshot and provides invalidation hooks for DDL operations.
    Automatically recaptures when accessed after invalidation.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session to introspect

    Attributes
    ----------
    snapshot : IntrospectionSnapshot
        Current or recaptured snapshot (property)
    """

    def __init__(
        self,
        ctx: SessionContext,
        *,
        sql_options: SQLOptions | None = None,
    ) -> None:
        """Initialize cache with SessionContext."""
        self._ctx = ctx
        self._sql_options = sql_options
        self._snapshot: IntrospectionSnapshot | None = None
        self._invalidated = False

    @property
    def snapshot(self) -> IntrospectionSnapshot:
        """
        Get or capture snapshot.

        Automatically recaptures if cache is invalidated or empty.

        Returns
        -------
        IntrospectionSnapshot
            Current snapshot
        """
        if self._snapshot is None or self._invalidated:
            self._snapshot = IntrospectionSnapshot.capture(
                self._ctx,
                sql_options=self._sql_options,
            )
            self._invalidated = False
        return self._snapshot

    def invalidate(self) -> None:
        """
        Mark cache as stale (call after DDL changes).

        Next access to `snapshot` property will trigger recapture.
        """
        self._invalidated = True

    def schema_map(self) -> dict[str, dict[str, str]]:
        """
        Get schema map from current snapshot.

        Returns
        -------
        dict[str, dict[str, str]]
            Schema map from current snapshot
        """
        return self.snapshot.schema_map()


_INTROSPECTION_CACHE_BY_CONTEXT: dict[int, IntrospectionCache] = {}


def introspection_cache_for_ctx(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> IntrospectionCache:
    """Return a cached IntrospectionCache for a SessionContext.

    Returns
    -------
    IntrospectionCache
        Cache bound to the provided SessionContext.
    """
    key = id(ctx)
    cache = _INTROSPECTION_CACHE_BY_CONTEXT.get(key)
    if cache is None:
        cache = IntrospectionCache(ctx, sql_options=sql_options)
        _INTROSPECTION_CACHE_BY_CONTEXT[key] = cache
    return cache


def invalidate_introspection_cache(ctx: SessionContext) -> None:
    """Invalidate the cached snapshot for a SessionContext, if present."""
    cache = _INTROSPECTION_CACHE_BY_CONTEXT.get(id(ctx))
    if cache is not None:
        cache.invalidate()


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True)
class CacheConfigSnapshot:
    """Configuration snapshot for DataFusion caches."""

    list_files_cache_ttl: str | None
    list_files_cache_limit: str | None
    metadata_cache_limit: str | None
    predicate_cache_size: str | None


@dataclass(frozen=True)
class CacheStateSnapshot:
    """Runtime state snapshot for a DataFusion cache."""

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
            JSON-ready cache snapshot row.
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
    try:
        table = _cache_snapshot_table(ctx, table_name=table_name)
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


def _cache_snapshot_table(ctx: SessionContext, *, table_name: str) -> pa.Table:
    sql = f"SELECT * FROM {table_name}"
    df = ctx.sql_with_options(sql, sql_options_for_profile(None))
    return df.to_arrow_table()


def _settings_snapshot(ctx: SessionContext) -> list[dict[str, object]]:
    snapshot = introspection_cache_for_ctx(ctx, sql_options=sql_options_for_profile(None)).snapshot
    return [dict(row) for row in snapshot.settings.to_pylist()]


def metadata_cache_snapshot(ctx: SessionContext) -> CacheStateSnapshot:
    """Capture metadata cache state snapshot.

    Returns
    -------
    CacheStateSnapshot
        Cache snapshot for metadata cache.
    """
    snapshot = _cache_snapshot_from_table(ctx, table_name="metadata_cache")
    if snapshot is not None:
        return snapshot
    settings = _settings_snapshot(ctx)
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

    Returns
    -------
    CacheStateSnapshot
        Cache snapshot for list_files cache.
    """
    snapshot = _cache_snapshot_from_table(ctx, table_name="list_files_cache")
    if snapshot is not None:
        return snapshot
    settings = _settings_snapshot(ctx)
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


def statistics_cache_snapshot(ctx: SessionContext) -> CacheStateSnapshot:
    """Capture statistics cache state snapshot.

    Returns
    -------
    CacheStateSnapshot
        Cache snapshot for statistics cache.
    """
    snapshot = _cache_snapshot_from_table(ctx, table_name="statistics_cache")
    if snapshot is not None:
        return snapshot
    settings = _settings_snapshot(ctx)
    config = _extract_cache_config(settings)
    now = _now_ms()
    return CacheStateSnapshot(
        cache_name="statistics",
        event_time_unix_ms=now,
        entry_count=None,
        hit_count=None,
        miss_count=None,
        eviction_count=None,
        config_ttl=None,
        config_limit=config.predicate_cache_size,
    )


def predicate_cache_snapshot(ctx: SessionContext) -> CacheStateSnapshot:
    """Capture predicate cache state snapshot.

    Returns
    -------
    CacheStateSnapshot
        Cache snapshot for predicate cache.
    """
    snapshot = _cache_snapshot_from_table(ctx, table_name="predicate_cache")
    if snapshot is not None:
        return snapshot
    settings = _settings_snapshot(ctx)
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

    Returns
    -------
    dict[str, Any]
        Diagnostics payload describing cache configuration and snapshots.
    """
    settings = _settings_snapshot(ctx)
    config = _extract_cache_config(settings)
    snapshots = [
        list_files_cache_snapshot(ctx).to_row(),
        metadata_cache_snapshot(ctx).to_row(),
        statistics_cache_snapshot(ctx).to_row(),
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

    Raises
    ------
    ImportError
        Raised when ``datafusion_ext`` is not available.
    TypeError
        Raised when the cache table registration hook is unavailable.
    """
    settings = _settings_snapshot(ctx)
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
    "IntrospectionCache",
    "IntrospectionSnapshot",
    "capture_cache_diagnostics",
    "introspection_cache_for_ctx",
    "invalidate_introspection_cache",
    "list_files_cache_snapshot",
    "metadata_cache_snapshot",
    "predicate_cache_snapshot",
    "register_cache_introspection_functions",
    "statistics_cache_snapshot",
]
