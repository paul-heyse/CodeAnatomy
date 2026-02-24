"""Rust query-pack lint regression tests."""

from __future__ import annotations

from tools.cq.search.tree_sitter.query.lint import lint_search_query_packs


def test_rust_query_packs_compile_cleanly() -> None:
    """Rust query packs should compile without field/node-type errors."""
    result = lint_search_query_packs()
    assert result.status == "ok", result.errors
    assert not any("rust:50_modules_imports.scm:compile_error" in err for err in result.errors)
    assert not any("rust:85_locals.scm:compile_error" in err for err in result.errors)
