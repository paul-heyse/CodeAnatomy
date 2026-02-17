"""Tests for smart-search relevance scoring helpers."""

from __future__ import annotations

from typing import cast

from tools.cq.core.locations import SourceSpan
from tools.cq.search.pipeline.classifier import MatchCategory
from tools.cq.search.pipeline.relevance import compute_relevance_score
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch


def _match(*, category: str, file: str, confidence: float = 1.0) -> EnrichedMatch:
    return EnrichedMatch(
        span=SourceSpan(file=file, start_line=1, start_col=0),
        text="target()",
        match_text="target",
        category=cast("MatchCategory", category),
        confidence=confidence,
        evidence_kind="classifier",
    )


def test_relevance_prefers_definitions_over_callsites() -> None:
    """Definitions should rank above callsites with equal confidence and file role."""
    definition = _match(category="definition", file="src/mod.py")
    callsite = _match(category="callsite", file="src/mod.py")
    assert compute_relevance_score(definition) > compute_relevance_score(callsite)


def test_relevance_penalizes_test_paths() -> None:
    """Test-path matches should rank below source-path matches."""
    src_match = _match(category="definition", file="src/pkg/module.py")
    test_match = _match(category="definition", file="tests/test_module.py")
    assert compute_relevance_score(src_match) > compute_relevance_score(test_match)
