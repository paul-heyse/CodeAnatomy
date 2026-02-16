"""Tests for tree-sitter core parse wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.core import parse as parse_module


def test_core_parse_exports_expected_symbols() -> None:
    """Test core parse exports expected symbols."""
    assert hasattr(parse_module, "ParseSession")
    assert callable(parse_module.get_parse_session)
    assert callable(parse_module.clear_parse_session)
