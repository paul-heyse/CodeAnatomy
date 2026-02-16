"""Tests for tree-sitter query predicate wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.query import predicates as predicates_module


def test_has_custom_predicates_detects_cq_predicate() -> None:
    """Test has custom predicates detects cq predicate."""
    assert predicates_module.has_custom_predicates('(identifier) @x (#cq-regex? @x "foo")')
