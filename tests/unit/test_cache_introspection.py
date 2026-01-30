"""Unit tests for cache introspection functionality."""

from __future__ import annotations

from datafusion_engine.catalog.introspection import (
    CacheConfigSnapshot,
    CacheStateSnapshot,
    capture_cache_diagnostics,
    list_files_cache_snapshot,
    metadata_cache_snapshot,
    predicate_cache_snapshot,
)
from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.diagnostics import diagnostic_profile
from tests.test_helpers.optional_deps import require_datafusion

require_datafusion()

EVENT_TIME_UNIX_MS = 1234567890000
ENTRY_COUNT = 10
HIT_COUNT = 5
MISS_COUNT = 2
EVICTION_COUNT = 1
EXPECTED_CACHE_SNAPSHOTS = 3


def test_cache_config_snapshot_construction() -> None:
    """CacheConfigSnapshot can be constructed with all fields."""
    snapshot = CacheConfigSnapshot(
        list_files_cache_ttl="2m",
        list_files_cache_limit="128 MiB",
        metadata_cache_limit="256 MiB",
        predicate_cache_size="64 MiB",
    )
    assert snapshot.list_files_cache_ttl == "2m"
    assert snapshot.list_files_cache_limit == "128 MiB"
    assert snapshot.metadata_cache_limit == "256 MiB"
    assert snapshot.predicate_cache_size == "64 MiB"


def test_cache_state_snapshot_construction() -> None:
    """CacheStateSnapshot can be constructed and serialized to row."""
    snapshot = CacheStateSnapshot(
        cache_name="list_files",
        event_time_unix_ms=EVENT_TIME_UNIX_MS,
        entry_count=ENTRY_COUNT,
        hit_count=HIT_COUNT,
        miss_count=MISS_COUNT,
        eviction_count=EVICTION_COUNT,
        config_ttl="2m",
        config_limit="128 MiB",
    )
    row = snapshot.to_row()
    assert row["cache_name"] == "list_files"
    assert row["event_time_unix_ms"] == EVENT_TIME_UNIX_MS
    assert row["entry_count"] == ENTRY_COUNT
    assert row["hit_count"] == HIT_COUNT
    assert row["miss_count"] == MISS_COUNT
    assert row["eviction_count"] == EVICTION_COUNT
    assert row["config_ttl"] == "2m"
    assert row["config_limit"] == "128 MiB"


def test_list_files_cache_snapshot() -> None:
    """list_files_cache_snapshot returns cache state."""
    profile = df_profile()
    ctx = profile.session_context()
    snapshot = list_files_cache_snapshot(ctx)
    assert snapshot.cache_name == "list_files"
    assert snapshot.event_time_unix_ms > 0
    assert snapshot.config_ttl is not None
    assert snapshot.config_limit is not None


def test_metadata_cache_snapshot() -> None:
    """metadata_cache_snapshot returns cache state."""
    profile = df_profile()
    ctx = profile.session_context()
    snapshot = metadata_cache_snapshot(ctx)
    assert snapshot.cache_name == "metadata"
    assert snapshot.event_time_unix_ms > 0
    assert snapshot.config_limit is not None


def test_predicate_cache_snapshot() -> None:
    """predicate_cache_snapshot returns cache state."""
    profile = df_profile()
    ctx = profile.session_context()
    snapshot = predicate_cache_snapshot(ctx)
    assert snapshot.cache_name == "predicate"
    assert snapshot.event_time_unix_ms > 0
    assert snapshot.config_limit is not None


def test_capture_cache_diagnostics() -> None:
    """capture_cache_diagnostics returns config and snapshots."""
    profile = df_profile()
    ctx = profile.session_context()
    diagnostics = capture_cache_diagnostics(ctx)
    assert "config" in diagnostics
    assert "cache_snapshots" in diagnostics
    config = diagnostics["config"]
    assert config["list_files_cache_ttl"] is not None
    assert config["list_files_cache_limit"] is not None
    assert config["metadata_cache_limit"] is not None
    assert config["predicate_cache_size"] is not None
    snapshots = diagnostics["cache_snapshots"]
    assert len(snapshots) == EXPECTED_CACHE_SNAPSHOTS
    cache_names = {s["cache_name"] for s in snapshots}
    assert cache_names == {"list_files", "metadata", "predicate"}


def test_cache_diagnostics_recorded_in_session_context() -> None:
    """Cache diagnostics are recorded when session context is created."""
    profile, sink = diagnostic_profile()
    _ctx = profile.session_context()
    artifacts = sink.artifacts_snapshot()
    assert "datafusion_cache_config_v1" in artifacts
    config = artifacts["datafusion_cache_config_v1"]
    assert isinstance(config, list)
    assert len(config) > 0
    config_entry = config[0]
    assert "list_files_cache_ttl" in config_entry
    assert "list_files_cache_limit" in config_entry
    events = sink.events_snapshot()
    assert "datafusion_cache_state_v1" in events
    cache_events = events["datafusion_cache_state_v1"]
    assert len(cache_events) >= EXPECTED_CACHE_SNAPSHOTS
    cache_names = {e["cache_name"] for e in cache_events}
    assert "list_files" in cache_names
    assert "metadata" in cache_names
    assert "predicate" in cache_names
