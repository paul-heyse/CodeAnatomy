"""Unit tests for runtime capability summary helper module."""

from __future__ import annotations

from obs.runtime_capabilities_summary import (
    collect_runtime_capability_events,
    latest_runtime_capability_event,
    runtime_capability_summary_payload,
    summarize_runtime_execution_metrics,
)

PRIMARY_EVENT_TIME_MS = 42
MEMORY_RESERVED_BYTES = 2048
METADATA_CACHE_ENTRIES = 12
METADATA_CACHE_HITS = 21
LIST_FILES_CACHE_ENTRIES = 5
STATISTICS_CACHE_ENTRIES = 9


def test_collect_runtime_capability_events_prefers_primary_runtime_event() -> None:
    """Use runtime capability events when present, ignoring fallback sources."""
    logs = [
        {
            "attributes": {
                "event.name": "datafusion_extension_parity_v1",
                "runtime_capabilities": {"event_time_unix_ms": 7, "delta_compatible": False},
            }
        },
        {
            "attributes": {
                "event.name": "datafusion_runtime_capabilities_v1",
                "event_time_unix_ms": 42,
                "delta_compatible": True,
            }
        },
    ]
    events = collect_runtime_capability_events(logs)
    assert len(events) == 1
    assert events[0].event_time_unix_ms == PRIMARY_EVENT_TIME_MS
    assert events[0].delta_compatible is True


def test_collect_runtime_capability_events_falls_back_to_service_provider() -> None:
    """Use service-provider events when primary and parity payloads are absent."""
    logs = [
        {
            "attributes": {
                "event.name": "delta_service_provider_v1",
                "event_time_unix_ms": 11,
                "strict_native_provider_enabled": True,
                "available": True,
                "compatible": True,
                "probe_result": "ok",
                "ctx_kind": "outer",
                "module": "datafusion_ext",
            }
        }
    ]
    events = collect_runtime_capability_events(logs)
    payload = runtime_capability_summary_payload(events)
    assert payload["total"] == 1
    assert payload["delta_available"] is True
    assert payload["delta_probe_result"] == "ok"
    assert payload["delta_module"] == "datafusion_ext"


def test_summarize_runtime_execution_metrics_normalizes_values() -> None:
    """Normalize execution metrics summary into deterministic integer fields."""
    summary = summarize_runtime_execution_metrics(
        {
            "rows": [{"metric_name": "memory_reserved_bytes", "value": 10}],
            "summary": {
                "memory_reserved_bytes": 2048.0,
                "metadata_cache_entries": 12,
                "metadata_cache_hits": 21,
                "list_files_cache_entries": 5,
                "statistics_cache_entries": 9,
            },
        }
    )
    assert summary.rows == 1
    assert summary.memory_reserved_bytes == MEMORY_RESERVED_BYTES
    assert summary.metadata_cache_entries == METADATA_CACHE_ENTRIES
    assert summary.metadata_cache_hits == METADATA_CACHE_HITS
    assert summary.list_files_cache_entries == LIST_FILES_CACHE_ENTRIES
    assert summary.statistics_cache_entries == STATISTICS_CACHE_ENTRIES


def test_latest_runtime_capability_event_returns_none_on_empty() -> None:
    """Return None when no runtime capability events are available."""
    assert latest_runtime_capability_event(()) is None
