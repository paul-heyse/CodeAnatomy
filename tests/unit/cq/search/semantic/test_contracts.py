"""Tests for semantic contracts."""

from __future__ import annotations

from tools.cq.search.semantic.models import SemanticOutcomeV1


def test_semantic_outcome_defaults() -> None:
    """Test semantic outcome defaults."""
    outcome = SemanticOutcomeV1()
    assert outcome.payload is None
    assert outcome.timed_out is False
