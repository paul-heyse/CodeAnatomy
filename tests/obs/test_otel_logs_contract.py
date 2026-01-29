"""Contract tests for OpenTelemetry logs."""

from __future__ import annotations

from obs.otel.logs import emit_diagnostics_event
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
