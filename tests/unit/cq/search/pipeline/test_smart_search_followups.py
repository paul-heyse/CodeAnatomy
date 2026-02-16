"""Tests for smart-search follow-up suggestions."""

from __future__ import annotations

from typing import cast

from tools.cq.core.locations import SourceSpan
from tools.cq.search._shared.types import QueryMode
from tools.cq.search.pipeline.classifier import MatchCategory
from tools.cq.search.pipeline.smart_search_followups import generate_followup_suggestions
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch


def _match(category: str) -> EnrichedMatch:
    return EnrichedMatch(
        span=SourceSpan(file="src/mod.py", start_line=1, start_col=0),
        text="target()",
        match_text="target",
        category=cast("MatchCategory", category),
        confidence=0.9,
        evidence_kind="resolved_ast",
        language="python",
    )


def test_generate_followup_suggestions_identifier_mode() -> None:
    """Identifier mode emits caller and impact follow-up commands."""
    matches = [_match("definition"), _match("callsite")]
    findings = generate_followup_suggestions(matches, "target", QueryMode.IDENTIFIER)
    messages = [finding.message for finding in findings]
    assert "Find callers: /cq calls target" in messages
    assert "Analyze impact: /cq impact target" in messages
