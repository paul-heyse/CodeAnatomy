"""Tests for tree-sitter query registry wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.query import registry as registry_module


def test_query_registry_exports_symbols() -> None:
    assert hasattr(registry_module, "QueryPackSourceV1")
    assert callable(registry_module.load_query_pack_sources)
