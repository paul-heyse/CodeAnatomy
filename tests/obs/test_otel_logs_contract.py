"""Contract tests for OpenTelemetry logs."""

from __future__ import annotations

import importlib

import pytest

from obs.otel.logs import OtelDiagnosticsSink, emit_diagnostics_event
from serde_schema_registry import ArtifactSpec
from tests.obs._support.otel_harness import get_otel_harness


def test_diagnostics_logs_emit() -> None:
    """Ensure diagnostics logs include correlation attributes."""
    harness = get_otel_harness()
    harness.reset()
    emit_diagnostics_event(
        "diagnostics.test",
        payload={"artifact": "cpg_nodes", "status": "ok"},
        event_kind="artifact",
    )
    logs = harness.log_exporter.get_finished_logs()
    assert logs
    log_record = logs[-1].log_record
    attributes = log_record.attributes or {}
    assert attributes.get("event.name") == "diagnostics.test"
    assert attributes.get("event.kind") == "artifact"


def test_log_attribute_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure log attribute limits are applied during normalization."""
    monkeypatch.setenv("OTEL_LOGRECORD_ATTRIBUTE_COUNT_LIMIT", "1")
    monkeypatch.setenv("OTEL_LOGRECORD_ATTRIBUTE_VALUE_LENGTH_LIMIT", "4")
    from obs.otel import attributes as otel_attributes

    otel_attributes = importlib.reload(otel_attributes)
    normalized = otel_attributes.normalize_log_attributes({"alpha": "abcdefgh", "beta": "zz"})
    assert normalized.get("alpha") == "abcd"
    assert len(normalized) == 1
    monkeypatch.delenv("OTEL_LOGRECORD_ATTRIBUTE_COUNT_LIMIT", raising=False)
    monkeypatch.delenv("OTEL_LOGRECORD_ATTRIBUTE_VALUE_LENGTH_LIMIT", raising=False)
    importlib.reload(otel_attributes)


def test_otel_diagnostics_sink_emits() -> None:
    """Ensure the OTel diagnostics sink emits logs."""
    harness = get_otel_harness()
    harness.reset()
    sink = OtelDiagnosticsSink()
    sink.record_artifact(
        ArtifactSpec(canonical_name="cache.test", description="OTel diagnostics sink test."),
        {"status": "ok"},
    )
    logs = harness.log_exporter.get_finished_logs()
    assert logs
    log_record = logs[-1].log_record
    attributes = log_record.attributes or {}
    assert attributes.get("event.name") == "cache.test"
    assert attributes.get("event.kind") == "artifact"
