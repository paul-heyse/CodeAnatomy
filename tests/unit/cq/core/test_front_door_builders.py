"""Tests for front-door insight builders."""

from __future__ import annotations

from tools.cq.core.front_door_assembly import (
    CallsInsightBuildRequestV1,
    InsightConfidenceV1,
    InsightLocationV1,
    InsightNeighborhoodV1,
    SearchInsightBuildRequestV1,
    build_calls_insight,
    build_search_insight,
)


def test_build_search_insight_uses_query_fallback_target() -> None:
    """Search builder should use query text when no primary finding exists."""
    insight = build_search_insight(
        SearchInsightBuildRequestV1(
            summary={"query": "target_symbol", "scan_method": "resolved_ast"},
            primary_target=None,
            target_candidates=(),
        )
    )

    assert insight.source == "search"
    assert insight.target.symbol == "target_symbol"
    assert insight.target.selection_reason == "fallback_query"


def test_build_calls_insight_adds_hazard_drivers() -> None:
    """Calls builder should carry hazard keys into risk drivers."""
    insight = build_calls_insight(
        CallsInsightBuildRequestV1(
            function_name="pkg.target",
            signature="target(x)",
            location=InsightLocationV1(file="src/mod.py", line=10),
            neighborhood=InsightNeighborhoodV1(),
            files_with_calls=4,
            arg_shape_count=2,
            forwarding_count=1,
            hazard_counts={"dynamic_dispatch": 2},
            confidence=InsightConfidenceV1(
                evidence_kind="resolved_ast",
                score=0.9,
                bucket="high",
            ),
        )
    )

    assert insight.source == "calls"
    assert insight.target.symbol == "pkg.target"
    assert "dynamic_dispatch" in insight.risk.drivers
