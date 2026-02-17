"""Tests for cache telemetry process-default store seams."""

from __future__ import annotations

from tools.cq.core.cache.telemetry import (
    get_default_cache_telemetry_store,
    record_cache_get,
    set_default_cache_telemetry_store,
    snapshot_cache_telemetry,
)
from tools.cq.core.cache.telemetry_store import CacheTelemetryStore


def test_set_default_cache_telemetry_store_replaces_process_store() -> None:
    """Setting default telemetry store should redirect subsequent writes."""
    original = get_default_cache_telemetry_store()
    custom = CacheTelemetryStore()
    try:
        set_default_cache_telemetry_store(custom)
        record_cache_get(namespace="search", hit=True, key="k")
        snapshot = snapshot_cache_telemetry()
        assert snapshot["search"].gets == 1
        assert snapshot["search"].hits == 1

        set_default_cache_telemetry_store(None)
        reset_snapshot = snapshot_cache_telemetry()
        assert "search" not in reset_snapshot
    finally:
        set_default_cache_telemetry_store(original)
