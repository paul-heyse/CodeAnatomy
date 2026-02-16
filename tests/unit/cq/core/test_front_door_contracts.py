"""Tests for front-door contract exports."""

from __future__ import annotations

from tools.cq.core.front_door_contracts import FrontDoorInsightV1, InsightTargetV1


def test_front_door_contract_defaults() -> None:
    """Front-door contract applies canonical default values."""
    insight = FrontDoorInsightV1(
        source="search",
        target=InsightTargetV1(symbol="build_graph"),
    )
    assert insight.source == "search"
    assert insight.target.symbol == "build_graph"
    assert insight.target.kind == "unknown"
    assert insight.schema_version == "cq.insight.v1"
