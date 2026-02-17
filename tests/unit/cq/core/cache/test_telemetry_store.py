"""Tests for cache telemetry store state handling."""

from __future__ import annotations

from tools.cq.core.cache.telemetry_store import CacheTelemetryStore

EXPECTED_CARDINALITY = 2


def test_telemetry_store_key_tracking_and_snapshot() -> None:
    """Telemetry store should track key-cardinality and size buckets."""
    store = CacheTelemetryStore()

    store.record_cache_key(
        namespace="scope",
        key="abc",
        max_key_size_64=64,
        max_key_size_128=128,
        max_key_size_256=256,
    )
    store.record_cache_key(
        namespace="scope",
        key="x" * 300,
        max_key_size_64=64,
        max_key_size_128=128,
        max_key_size_256=256,
    )

    snapshot = store.snapshot()["scope"]
    assert snapshot["key_cardinality"] == EXPECTED_CARDINALITY
    assert snapshot["key_size_le_64"] == 1
    assert snapshot["key_size_gt_256"] == 1


def test_telemetry_store_reset_clears_state() -> None:
    """Reset should clear all telemetry and seen-key state."""
    store = CacheTelemetryStore()
    store.record_cache_key(
        namespace="scope",
        key="abc",
        max_key_size_64=64,
        max_key_size_128=128,
        max_key_size_256=256,
    )

    store.reset()

    assert store.snapshot() == {}
