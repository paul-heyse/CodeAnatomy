"""Tests for front-door risk helpers."""

from __future__ import annotations

from tools.cq.core.front_door_contracts import InsightRiskCountersV1
from tools.cq.core.front_door_risk import risk_from_counters


def test_risk_from_counters_escalates_with_hazards() -> None:
    """Hazards should elevate risk level."""
    risk = risk_from_counters(
        InsightRiskCountersV1(callers=2, callees=1, hazard_count=1, forwarding_count=0)
    )
    assert risk.level == "high"
    assert "dynamic_hazards" in risk.drivers
