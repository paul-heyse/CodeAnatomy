"""Tests for tree-sitter query registry cache adapter."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search.tree_sitter.query.support import query_registry_cache


def test_query_registry_cache_returns_none_when_root_provided() -> None:
    """Query registry cache is intentionally unavailable under hard cutover."""
    assert query_registry_cache(root=Path()) is None


def test_query_registry_cache_returns_none_when_root_missing() -> None:
    """Root-less calls also degrade to uncached mode."""
    assert query_registry_cache(root=None) is None
