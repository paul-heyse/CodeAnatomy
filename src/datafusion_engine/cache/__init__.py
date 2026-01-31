"""Cache helpers for DataFusion view materialization."""

from __future__ import annotations

from datafusion_engine.cache.inventory import (
    CACHE_INVENTORY_TABLE_NAME,
    CacheInventoryEntry,
    cache_inventory_schema,
    ensure_cache_inventory_table,
    record_cache_inventory_entry,
)
from datafusion_engine.cache.registry import (
    CacheHitRequest,
    CacheInventoryRecord,
    latest_cache_inventory_record,
    record_cache_inventory,
    register_cached_delta_table,
    resolve_cache_hit,
)

__all__ = [
    "CACHE_INVENTORY_TABLE_NAME",
    "CacheHitRequest",
    "CacheInventoryEntry",
    "CacheInventoryRecord",
    "cache_inventory_schema",
    "ensure_cache_inventory_table",
    "latest_cache_inventory_record",
    "record_cache_inventory",
    "record_cache_inventory_entry",
    "register_cached_delta_table",
    "resolve_cache_hit",
]
