"""Tests for tree-sitter core runtime wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.core import runtime as runtime_module


def test_core_runtime_exports_callable_functions() -> None:
    """Test core runtime exports callable functions."""
    assert callable(runtime_module.run_bounded_query_captures)
    assert callable(runtime_module.run_bounded_query_matches)
