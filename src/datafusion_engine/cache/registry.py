"""Cache registry helpers for Delta-backed view caches."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

import msgspec
from datafusion import col, lit

from datafusion_engine.cache.inventory import (
    CACHE_INVENTORY_TABLE_NAME,
    CacheInventoryEntry,
    ensure_cache_inventory_table,
    record_cache_inventory_entry,
)
from datafusion_engine.delta.contracts import enforce_schema_evolution
from storage.deltalake import DeltaSchemaRequest
from utils.registry_protocol import MutableRegistry, Registry, SnapshotRegistry

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class CacheInventoryRecord:
    """Structured cache inventory row."""

    view_name: str
    cache_policy: str
    cache_path: str
    plan_fingerprint: str | None
    plan_identity_hash: str | None
    schema_identity_hash: str | None
    snapshot_version: int | None
    snapshot_timestamp: str | None
    run_id: str | None
    result: str | None
    row_count: int | None
    file_count: int | None
    partition_by: tuple[str, ...]
    event_time_unix_ms: int | None


@dataclass(frozen=True)
class CacheHitRequest:
    """Inputs required to resolve a cache hit."""

    view_name: str
    cache_path: str
    plan_identity_hash: str | None
    expected_schema_hash: str | None
    allow_evolution: bool
    storage_options: Mapping[str, str] | None
    log_storage_options: Mapping[str, str] | None


@dataclass
class CacheInventoryRegistry(
    Registry[str, CacheInventoryRecord],
    SnapshotRegistry[str, CacheInventoryRecord],
):
    """Registry for cache inventory records keyed by view name."""

    _entries: MutableRegistry[str, CacheInventoryRecord] = field(default_factory=MutableRegistry)

    def register(self, key: str, value: CacheInventoryRecord) -> None:
        """Register a cache inventory record by view name."""
        self._entries.register(key, value, overwrite=True)

    def get(self, key: str) -> CacheInventoryRecord | None:
        """Return a cache inventory record by view name.

        Returns
        -------
        CacheInventoryRecord | None
            Cache inventory record when present.
        """
        return self._entries.get(key)

    def __contains__(self, key: str) -> bool:
        """Return True when a view has a registered cache record.

        Returns
        -------
        bool
            ``True`` when the view has a cache record.
        """
        return key in self._entries

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered view names.

        Returns
        -------
        Iterator[str]
            Iterator of registered view names.
        """
        return iter(self._entries)

    def __len__(self) -> int:
        """Return the count of registered cache inventory records.

        Returns
        -------
        int
            Number of registered cache inventory records.
        """
        return len(self._entries)

    def snapshot(self) -> Mapping[str, CacheInventoryRecord]:
        """Return a snapshot of the registry entries.

        Returns
        -------
        Mapping[str, CacheInventoryRecord]
            Snapshot of registry entries.
        """
        return self._entries.snapshot()

    def restore(self, snapshot: Mapping[str, CacheInventoryRecord]) -> None:
        """Restore registry entries from a snapshot."""
        self._entries.restore(snapshot)


def resolve_cache_hit(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    request: CacheHitRequest,
) -> CacheInventoryRecord | None:
    """Return the latest cache inventory record when still valid.

    Returns
    -------
    CacheInventoryRecord | None
        Latest cache inventory record when it is still valid.
    """
    if request.plan_identity_hash is None:
        return None
    record = latest_cache_inventory_record(
        ctx,
        profile,
        view_name=request.view_name,
        plan_identity_hash=request.plan_identity_hash,
    )
    if record is None:
        return None
    if record.cache_path != request.cache_path:
        return None
    current_version = profile.delta_service().table_version(
        path=request.cache_path,
        storage_options=request.storage_options,
        log_storage_options=request.log_storage_options,
    )
    if current_version is None:
        return None
    if record.snapshot_version is not None and record.snapshot_version != current_version:
        return None
    enforce_schema_evolution(
        request=DeltaSchemaRequest(
            path=request.cache_path,
            storage_options=request.storage_options,
            log_storage_options=request.log_storage_options,
        ),
        expected_schema_hash=request.expected_schema_hash,
        allow_evolution=request.allow_evolution,
    )
    return replace(record, snapshot_version=current_version)


def latest_cache_inventory_record(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    view_name: str,
    plan_identity_hash: str,
) -> CacheInventoryRecord | None:
    """Return the latest cache inventory record for a view and plan hash.

    Returns
    -------
    CacheInventoryRecord | None
        Latest inventory record for the requested view and plan hash.
    """
    location = ensure_cache_inventory_table(ctx, profile)
    if location is None:
        return None
    try:
        df = (
            ctx.table(CACHE_INVENTORY_TABLE_NAME)
            .filter(col("view_name") == lit(view_name))
            .filter(col("plan_identity_hash") == lit(plan_identity_hash))
            .sort(col("event_time_unix_ms").sort(ascending=False))
            .limit(1)
        )
    except (RuntimeError, TypeError, ValueError):
        return None
    table = df.to_arrow_table()
    if table.num_rows < 1:
        return None
    row = table.to_pylist()[0]
    return _record_from_row(row)


def register_cached_delta_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    name: str,
    location: DatasetLocation,
    snapshot_version: int | None,
) -> None:
    """Register a Delta cache table with optional snapshot pinning."""
    from datafusion_engine.dataset.registration import (
        DatasetRegistrationOptions,
        register_dataset_df,
    )

    pinned = location
    if snapshot_version is not None:
        pinned = msgspec.structs.replace(
            location,
            delta_version=snapshot_version,
            delta_timestamp=None,
        )
    register_dataset_df(
        ctx,
        name=name,
        location=pinned,
        options=DatasetRegistrationOptions(runtime_profile=profile),
    )


def record_cache_inventory(
    profile: DataFusionRuntimeProfile,
    *,
    entry: CacheInventoryEntry,
    ctx: SessionContext | None = None,
) -> int | None:
    """Record a cache inventory entry.

    Parameters
    ----------
    profile
        Runtime profile controlling cache inventory persistence.
    entry
        Inventory entry to record.
    ctx
        Optional session context to reuse for inventory writes.

    Returns
    -------
    int | None
        Delta version for the inventory table write.
    """
    return record_cache_inventory_entry(profile, entry=entry, ctx=ctx)


def _record_from_row(row: Mapping[str, object]) -> CacheInventoryRecord:
    return CacheInventoryRecord(
        view_name=_coerce_str(row.get("view_name")),
        cache_policy=_coerce_str(row.get("cache_policy")),
        cache_path=_coerce_str(row.get("cache_path")),
        plan_fingerprint=_coerce_opt_str(row.get("plan_fingerprint")),
        plan_identity_hash=_coerce_opt_str(row.get("plan_identity_hash")),
        schema_identity_hash=_coerce_opt_str(row.get("schema_identity_hash")),
        snapshot_version=_coerce_opt_int(row.get("snapshot_version")),
        snapshot_timestamp=_coerce_opt_str(row.get("snapshot_timestamp")),
        run_id=_coerce_opt_str(row.get("run_id")),
        result=_coerce_opt_str(row.get("result")),
        row_count=_coerce_opt_int(row.get("row_count")),
        file_count=_coerce_opt_int(row.get("file_count")),
        partition_by=_coerce_str_tuple(row.get("partition_by")),
        event_time_unix_ms=_coerce_opt_int(row.get("event_time_unix_ms")),
    )


def _coerce_str(value: object) -> str:
    if isinstance(value, str) and value:
        return value
    msg = "Cache inventory row missing required string field."
    raise ValueError(msg)


def _coerce_opt_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and value:
        return value
    return None


def _coerce_opt_int(value: object) -> int | None:
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


def _coerce_str_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value if item is not None)
    return ()


__all__ = [
    "CacheHitRequest",
    "CacheInventoryRecord",
    "CacheInventoryRegistry",
    "latest_cache_inventory_record",
    "record_cache_inventory",
    "register_cached_delta_table",
    "resolve_cache_hit",
]
