"""Tests for tree-sitter language registry introspection."""

from __future__ import annotations

from tools.cq.search.tree_sitter.core.language_registry import load_language_registry


def test_load_language_registry_python_returns_node_and_field_sets() -> None:
    """Test load language registry python returns node and field sets."""
    registry = load_language_registry("python")
    assert registry is not None
    assert registry.language == "python"
    assert registry.node_kinds
    assert registry.field_names is not None


def test_load_language_registry_rust_returns_node_and_field_sets() -> None:
    """Test load language registry rust returns node and field sets."""
    registry = load_language_registry("rust")
    assert registry is not None
    assert registry.language == "rust"
    assert registry.node_kinds
