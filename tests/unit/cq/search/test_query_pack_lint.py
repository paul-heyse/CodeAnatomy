"""Tests for search query-pack lint wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.query.lint import (
    QueryPackLintResultV1,
    _missing_required_metadata,
    lint_search_query_packs,
)


def test_lint_search_query_packs_returns_typed_result() -> None:
    """Test lint search query packs returns typed result."""
    result = lint_search_query_packs()
    assert isinstance(result, QueryPackLintResultV1)
    assert result.status in {"ok", "failed"}
    assert isinstance(result.errors, tuple)


def test_missing_required_metadata_helper_reports_missing_keys() -> None:
    """Test missing required metadata helper reports missing keys."""
    missing = _missing_required_metadata(
        {"cq.emit": "definitions"},
        ("cq.emit", "cq.kind", "cq.anchor"),
    )
    assert missing == ("cq.kind", "cq.anchor")
