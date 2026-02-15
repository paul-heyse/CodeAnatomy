"""Tests for canonical tree-sitter query resource paths."""

from __future__ import annotations

from tools.cq.search.tree_sitter.query.support import (
    diagnostics_query_path,
    query_contracts_path,
    query_pack_dir,
    query_pack_path,
)


def test_query_pack_dir_points_to_repository_queries() -> None:
    python_dir = query_pack_dir("python")
    rust_dir = query_pack_dir("rust")
    assert python_dir.name == "python"
    assert rust_dir.name == "rust"
    assert python_dir.parent == rust_dir.parent
    assert python_dir.parent.name == "queries"


def test_query_pack_paths_resolve_existing_assets() -> None:
    assert query_pack_path("python", "00_defs.scm").exists()
    assert query_pack_path("rust", "80_tags.scm").exists()
    assert query_contracts_path("python").exists()
    assert diagnostics_query_path("rust").exists()
