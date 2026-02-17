"""Tests for front-door insight assembly."""

from __future__ import annotations

from tools.cq.core.front_door_assembly import build_search_insight
from tools.cq.core.front_door_contracts import SearchInsightBuildRequestV1


def test_build_search_insight_fallback_target_symbol() -> None:
    """Search insight falls back to query text when no target finding exists."""
    insight = build_search_insight(
        SearchInsightBuildRequestV1(
            summary={"query": "build_graph"},
            primary_target=None,
            target_candidates=(),
        )
    )
    assert insight.target.symbol == "build_graph"
    assert insight.source == "search"
