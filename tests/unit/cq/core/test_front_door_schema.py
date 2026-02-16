"""Tests for front-door schema exports."""

from __future__ import annotations

from tools.cq.core.front_door_schema import FrontDoorInsightV1, InsightTargetV1


def test_front_door_schema_constructs_insight() -> None:
    """Schema exports should construct canonical front-door payloads."""
    insight = FrontDoorInsightV1(
        source="search",
        target=InsightTargetV1(symbol="target_symbol", kind="function"),
    )

    assert insight.source == "search"
    assert insight.target.symbol == "target_symbol"
    assert insight.schema_version == "cq.insight.v1"
