"""Tests for search query-pack lint wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.query.lint import (
    QueryPackLintResultV1,
    lint_search_query_packs,
)


def test_lint_search_query_packs_returns_typed_result() -> None:
    result = lint_search_query_packs()
    assert isinstance(result, QueryPackLintResultV1)
    assert result.status in {"ok", "failed"}
    assert isinstance(result.errors, tuple)
