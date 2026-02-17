"""Tests for smart-search request coercion helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.request_parsing import coerce_search_request


def test_coerce_search_request_defaults_and_filters_invalid_values() -> None:
    """Coercion should normalize invalid inputs to safe defaults."""
    request = coerce_search_request(
        root=Path(),
        query="target",
        kwargs={
            "lang_scope": "invalid",
            "include_globs": ["*.py", 123],
            "exclude_globs": "not-a-list",
            "argv": [1, "cq"],
            "started_ms": True,
            "run_id": "   ",
            "incremental_enrichment_enabled": "yes",
        },
    )

    assert request.lang_scope == "auto"
    assert request.include_globs == ["*.py"]
    assert request.exclude_globs is None
    assert request.argv == ["1", "cq"]
    assert request.started_ms is None
    assert request.run_id is None
    assert request.incremental_enrichment_enabled is True


def test_coerce_search_request_preserves_typed_mode() -> None:
    """Typed query modes should be preserved verbatim."""
    request = coerce_search_request(
        root=Path(),
        query="target",
        kwargs={"mode": QueryMode.LITERAL},
    )

    assert request.mode is QueryMode.LITERAL
