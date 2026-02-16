"""Tests for summary envelope contracts."""

from __future__ import annotations

from tools.cq.core.summary_contracts import (
    SummaryEnvelopeV1,
    build_summary_envelope,
    summary_envelope_to_mapping,
)


def test_build_summary_envelope_and_mapping() -> None:
    """Test build summary envelope and mapping."""
    envelope = build_summary_envelope(
        summary={"query": "foo"},
        diagnostics=[{"code": "ML001"}],
        telemetry={"attempted": 1},
    )
    assert isinstance(envelope, SummaryEnvelopeV1)
    mapping = summary_envelope_to_mapping(envelope)
    assert mapping["summary"] == {"query": "foo"}
    assert mapping["diagnostics"] == [{"code": "ML001"}]
