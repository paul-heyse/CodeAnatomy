"""Tests for tree-sitter runtime context state container."""

from __future__ import annotations

from tools.cq.search.tree_sitter.core.runtime_context import (
    TreeSitterRuntimeContext,
    get_default_context,
    set_default_context,
)


def test_default_context_is_singleton_instance() -> None:
    """Ensure repeated access returns the same process-wide context object."""
    first = get_default_context()
    second = get_default_context()

    assert first is second


def test_set_default_context_replaces_context() -> None:
    """Ensure explicit context override swaps the active singleton context."""
    original = get_default_context()
    replacement = TreeSitterRuntimeContext()

    set_default_context(replacement)
    assert get_default_context() is replacement

    set_default_context(original)
