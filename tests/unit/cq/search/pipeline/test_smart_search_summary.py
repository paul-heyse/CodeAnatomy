"""Tests for smart-search summary helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search._shared.types import QueryMode, SearchLimits
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search_summary import (
    build_language_summary,
    build_search_summary,
)
from tools.cq.search.pipeline.smart_search_types import (
    LanguageSearchResult,
    SearchStats,
    SearchSummaryInputs,
)


def _config() -> SearchConfig:
    return SearchConfig(
        root=Path(),
        query="target",
        mode=QueryMode.IDENTIFIER,
        limits=SearchLimits(),
    )


def test_build_language_summary_shapes_payload() -> None:
    """Language summary helper includes canonical top-level keys."""
    inputs = SearchSummaryInputs(
        config=_config(),
        stats=SearchStats(scanned_files=2, matched_files=1, total_matches=1),
        matches=[],
        languages=("python",),
        language_stats={"python": SearchStats(scanned_files=2, matched_files=1, total_matches=1)},
    )
    summary = build_language_summary(inputs)
    assert summary["query"] == "target"
    assert summary["mode"] == "identifier"
    assert summary["lang_scope"] == "auto"


def test_build_search_summary_returns_summary_and_diagnostics() -> None:
    """Search summary helper returns summary with diagnostics list."""
    partition_results = [
        LanguageSearchResult(
            lang="python",
            raw_matches=[],
            stats=SearchStats(scanned_files=2, matched_files=1, total_matches=1),
            pattern="target",
            enriched_matches=[],
            dropped_by_scope=0,
        )
    ]
    summary, diagnostics = build_search_summary(
        _config(),
        partition_results,
        [],
        python_semantic_overview={},
        python_semantic_telemetry={},
        python_semantic_diagnostics=[],
    )
    assert summary["query"] == "target"
    assert isinstance(diagnostics, list)
    summary_diagnostics = summary["cross_language_diagnostics"]
    assert isinstance(summary_diagnostics, list)
    assert len(summary_diagnostics) == len(diagnostics)
