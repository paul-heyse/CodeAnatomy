"""Tests for diagnostics quality-metric builders."""

from __future__ import annotations

from datafusion import SessionContext

from semantics.diagnostics.quality_metrics import (
    build_relationship_candidates_view,
    build_relationship_quality_metrics,
)

EXPECTED_EDGE_COUNT = 2
EXPECTED_DISTINCT_SOURCES = 2


def test_build_relationship_quality_metrics_none_when_missing() -> None:
    """Missing relationship tables return None."""
    ctx = SessionContext()

    result = build_relationship_quality_metrics(ctx, "missing_rel")

    assert result is None


def test_build_relationship_quality_metrics_returns_counts() -> None:
    """Relationship metrics compute total edges and distinct cardinalities."""
    ctx = SessionContext()
    ctx.from_pydict(
        {
            "entity_id": ["e1", "e2"],
            "symbol": ["s1", "s1"],
            "confidence": [0.9, 0.2],
            "score": [1.0, 2.0],
        },
        name="rel_test",
    )

    result = build_relationship_quality_metrics(ctx, "rel_test")

    assert result is not None
    row = result.to_arrow_table().to_pylist()[0]
    assert row["relationship_name"] == "rel_test"
    assert row["total_edges"] == EXPECTED_EDGE_COUNT
    assert row["distinct_sources"] == EXPECTED_DISTINCT_SOURCES


def test_build_relationship_candidates_view_empty_when_no_relationships() -> None:
    """Relationship candidates view is empty when no relationship tables exist."""
    ctx = SessionContext()

    result = build_relationship_candidates_view(ctx)

    assert result.to_arrow_table().num_rows == 0
