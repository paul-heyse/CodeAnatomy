"""Tests for smart-search summary wrapper module."""

from __future__ import annotations

from pathlib import Path

import pytest
import tools.cq.search.pipeline.smart_search as smart_search_module
from tools.cq.search._shared.types import QueryMode, SearchLimits
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search_summary import (
    build_language_summary,
    build_search_summary,
)
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch, LanguageSearchResult


def test_build_search_summary_delegates_to_smart_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Summary wrapper should delegate to smart_search._build_search_summary."""
    expected: tuple[dict[str, object], list[object]] = ({"ok": True}, [])

    def _fake(*_args: object, **_kwargs: object) -> tuple[dict[str, object], list[object]]:
        return expected

    monkeypatch.setattr(smart_search_module, "_build_search_summary", _fake)

    ctx = SearchConfig(
        root=Path(),
        query="target",
        mode=QueryMode.IDENTIFIER,
        limits=SearchLimits(),
    )
    partition_results: list[LanguageSearchResult] = []
    enriched_matches: list[EnrichedMatch] = []
    result = build_search_summary(
        ctx,
        partition_results,
        enriched_matches,
        python_semantic_overview={},
        python_semantic_telemetry={},
        python_semantic_diagnostics=[],
    )
    assert result == expected


def test_build_language_summary_delegates_to_smart_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Language summary wrapper should delegate to smart_search.build_summary."""
    monkeypatch.setattr(
        smart_search_module,
        "build_summary",
        lambda *_args, **_kwargs: {"mode": "ok"},
    )

    assert build_language_summary("x") == {"mode": "ok"}
