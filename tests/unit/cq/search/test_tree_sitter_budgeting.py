"""Tests for tree-sitter budgeting helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter_budgeting import (
    budget_ms_per_anchor,
    deadline_from_budget_ms,
    is_deadline_expired,
)


def test_budget_ms_per_anchor_is_bounded() -> None:
    budget = budget_ms_per_anchor(timeout_seconds=30.0, max_anchors=500)
    assert 25 <= budget <= 2_000


def test_deadline_helpers() -> None:
    deadline = deadline_from_budget_ms(50)
    assert deadline is not None
    assert is_deadline_expired(None) is False
