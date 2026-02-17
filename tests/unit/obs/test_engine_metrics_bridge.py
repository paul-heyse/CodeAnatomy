"""Tests for engine metrics bridge runtime execution metrics emission."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import obs.otel
import obs.otel.metrics as otel_metrics
from obs.engine_metrics_bridge import record_engine_metrics

_MEMORY_RESERVED_BYTES = 42
_METADATA_CACHE_ENTRIES = 3
_METADATA_CACHE_HITS = 2
_STATISTICS_CACHE_ENTRIES = 7
_TOP_LEVEL_ROWS = 2
_TOP_LEVEL_MEMORY_RESERVED_BYTES = 100


def _capture_event(
    sink: list[tuple[str, Mapping[str, object], str]],
) -> Any:
    def _emit(name: str, payload: Mapping[str, object], event_kind: str = "event") -> None:
        sink.append((name, payload, event_kind))

    return _emit


def test_record_engine_metrics_emits_runtime_execution_metrics_from_runtime_caps(
    monkeypatch: Any,
) -> None:
    """Record runtime metrics from nested runtime_capabilities execution payloads."""
    events: list[tuple[str, Mapping[str, object], str]] = []
    stage_calls: list[tuple[str, float, str]] = []
    task_calls: list[tuple[str, float, str]] = []

    monkeypatch.setattr(obs.otel, "emit_diagnostics_event", _capture_event(events))
    monkeypatch.setattr(
        otel_metrics,
        "record_stage_duration",
        lambda stage, duration, status="ok": stage_calls.append((stage, duration, status)),
    )
    monkeypatch.setattr(
        otel_metrics,
        "record_task_duration",
        lambda task, duration, status="ok": task_calls.append((task, duration, status)),
    )

    run_result = {
        "trace_metrics_summary": {"elapsed_compute_nanos": 2_000_000_000},
        "collected_metrics": {
            "operator_metrics": [
                {"operator_name": "hash_join", "elapsed_compute_nanos": 1_000_000_000}
            ]
        },
        "runtime_capabilities": {
            "execution_metrics": {
                "rows": [{"node": "scan"}],
                "summary": {
                    "memory_reserved_bytes": _MEMORY_RESERVED_BYTES,
                    "metadata_cache_entries": _METADATA_CACHE_ENTRIES,
                    "metadata_cache_hits": _METADATA_CACHE_HITS,
                    "list_files_cache_entries": 1,
                    "statistics_cache_entries": _STATISTICS_CACHE_ENTRIES,
                },
            }
        },
    }

    record_engine_metrics(run_result)

    assert stage_calls == [("engine_execute", 2.0, "ok")]
    assert task_calls == [("hash_join", 1.0, "ok")]
    assert len(events) == 1
    name, payload, event_kind = events[0]
    assert name == "engine_runtime_execution_metrics_v1"
    assert event_kind == "event"
    assert payload["rows"] == 1
    assert payload["memory_reserved_bytes"] == _MEMORY_RESERVED_BYTES
    assert payload["metadata_cache_entries"] == _METADATA_CACHE_ENTRIES
    assert payload["metadata_cache_hits"] == _METADATA_CACHE_HITS
    assert payload["list_files_cache_entries"] == 1
    assert payload["statistics_cache_entries"] == _STATISTICS_CACHE_ENTRIES


def test_record_engine_metrics_emits_runtime_execution_metrics_from_top_level(
    monkeypatch: Any,
) -> None:
    """Record runtime metrics when execution_metrics is exposed at top level."""
    events: list[tuple[str, Mapping[str, object], str]] = []

    monkeypatch.setattr(obs.otel, "emit_diagnostics_event", _capture_event(events))
    monkeypatch.setattr(otel_metrics, "record_stage_duration", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(otel_metrics, "record_task_duration", lambda *_args, **_kwargs: None)

    run_result = {
        "execution_metrics": {
            "rows": [{"node": "scan"}, {"node": "projection"}],
            "summary": {"memory_reserved_bytes": _TOP_LEVEL_MEMORY_RESERVED_BYTES},
        }
    }

    record_engine_metrics(run_result)

    assert len(events) == 1
    _name, payload, _event_kind = events[0]
    assert payload["rows"] == _TOP_LEVEL_ROWS
    assert payload["memory_reserved_bytes"] == _TOP_LEVEL_MEMORY_RESERVED_BYTES


def test_record_engine_metrics_skips_runtime_metrics_when_absent(
    monkeypatch: Any,
) -> None:
    """Skip runtime metrics diagnostics emission when metrics are absent."""
    events: list[tuple[str, Mapping[str, object], str]] = []
    monkeypatch.setattr(obs.otel, "emit_diagnostics_event", _capture_event(events))
    monkeypatch.setattr(otel_metrics, "record_stage_duration", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(otel_metrics, "record_task_duration", lambda *_args, **_kwargs: None)

    record_engine_metrics({"warnings": []})

    assert events == []
