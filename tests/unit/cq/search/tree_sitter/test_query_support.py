"""Tests for merged query support module (resource paths, pack metadata, cache adapter)."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search.tree_sitter.query.support import (
    diagnostics_query_path,
    first_capture,
    pattern_settings,
    query_contracts_path,
    query_pack_dir,
    query_pack_path,
    query_registry_cache,
)

# -- Resource Paths -----------------------------------------------------------


def test_query_pack_dir_points_to_repository_queries() -> None:
    """Verify query pack directories resolve into the queries/ directory."""
    python_dir = query_pack_dir("python")
    rust_dir = query_pack_dir("rust")
    assert python_dir.name == "python"
    assert rust_dir.name == "rust"
    assert python_dir.parent == rust_dir.parent
    assert python_dir.parent.name == "queries"


def test_query_pack_path_resolves_existing_assets() -> None:
    """Verify named query pack paths resolve to existing files."""
    assert query_pack_path("python", "00_defs.scm").exists()
    assert query_pack_path("rust", "80_tags.scm").exists()
    assert query_contracts_path("python").exists()
    assert diagnostics_query_path("rust").exists()


# -- Pack Metadata ------------------------------------------------------------


class _FakeQuery:
    def __init__(self, settings: dict[int, dict[object, object]]) -> None:
        self._settings = settings

    def pattern_settings(self, idx: int) -> dict[object, object]:
        return self._settings.get(idx, {})


def test_pattern_settings_returns_normalized_string_dict() -> None:
    """Verify pattern_settings returns only string key-value pairs."""
    query = _FakeQuery({0: {"cq.emit": "scope", "cq.kind": "function", 42: "ignored"}})
    result = pattern_settings(query, 0)
    assert result == {"cq.emit": "scope", "cq.kind": "function"}


def test_first_capture_returns_none_for_missing_name() -> None:
    """Verify first_capture returns None when capture name is absent."""
    assert first_capture({}, "name") is None


def test_first_capture_returns_first_node() -> None:
    """Verify first_capture returns the first node in the list."""
    sentinel = object()
    result = first_capture({"name": [sentinel]}, "name")
    assert result is sentinel


# -- Cache Adapter ------------------------------------------------------------


def test_query_registry_cache_returns_none_when_no_cache() -> None:
    """Verify cache adapter intentionally degrades to uncached mode."""
    assert query_registry_cache(root=Path()) is None


def test_query_registry_cache_returns_none_without_root() -> None:
    """Root-less calls should also disable query-registry cache access."""
    assert query_registry_cache(root=None) is None
