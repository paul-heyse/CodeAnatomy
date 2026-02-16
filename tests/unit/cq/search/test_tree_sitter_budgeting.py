"""Tests for tree-sitter budgeting helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.core.runtime_support import (
    budget_ms_per_anchor,
    deadline_from_budget_ms,
    is_deadline_expired,
)

MIN_BUDGET_MS = 25
MAX_BUDGET_MS = 2_000


def test_budget_ms_per_anchor_is_bounded() -> None:
    """Test budget ms per anchor is bounded."""
    budget = budget_ms_per_anchor(timeout_seconds=30.0, max_anchors=500)
    assert MIN_BUDGET_MS <= budget <= MAX_BUDGET_MS


def test_deadline_helpers() -> None:
    """Test deadline helpers."""
    deadline = deadline_from_budget_ms(50)
    assert deadline is not None
    assert is_deadline_expired(None) is False
