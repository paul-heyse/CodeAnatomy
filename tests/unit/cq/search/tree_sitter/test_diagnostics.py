"""Tests for flattened diagnostics module (recovery hints and collection)."""

from __future__ import annotations

from tools.cq.search.tree_sitter.diagnostics import (
    collect_tree_sitter_diagnostics,
    recovery_hints_for_node,
)


def test_recovery_hints_returns_empty_when_no_next_state() -> None:
    """Verify recovery hints returns empty tuple for language without next_state."""

    class _Lang:
        pass

    class _Node:
        parse_state = 1
        grammar_id = 2

    result = recovery_hints_for_node(language=_Lang(), node=_Node())
    assert result == ()


def test_collect_tree_sitter_diagnostics_is_importable() -> None:
    """Verify the collect function is available from the flattened module."""
    assert callable(collect_tree_sitter_diagnostics)
