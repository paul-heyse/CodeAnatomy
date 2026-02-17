"""Tests for partition-pipeline result typing."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search._shared.types import QueryMode, SearchLimits
from tools.cq.search.pipeline.contracts import SearchConfig, SearchPartitionPlanV1
from tools.cq.search.pipeline.partition_pipeline import run_search_partition
from tools.cq.search.pipeline.smart_search import _run_single_partition
from tools.cq.search.pipeline.smart_search_types import LanguageSearchResult, SearchStats


def test_run_search_partition_declares_language_result_type() -> None:
    """Partition pipeline must expose precise LanguageSearchResult return annotation."""
    assert run_search_partition.__annotations__["return"] is LanguageSearchResult


def test_run_single_partition_delegates_to_partition_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Smart-search partition helper should call run_search_partition directly."""
    expected = LanguageSearchResult(
        lang="python",
        raw_matches=[],
        stats=SearchStats(scanned_files=0, matched_files=0, total_matches=0),
        pattern="target",
        enriched_matches=[],
        dropped_by_scope=0,
    )
    captured: dict[str, object] = {}

    def _fake_run(
        plan: SearchPartitionPlanV1,
        *,
        ctx: SearchConfig,
        mode: QueryMode,
    ) -> LanguageSearchResult:
        captured["plan"] = plan
        captured["ctx"] = ctx
        captured["mode"] = mode
        return expected

    monkeypatch.setattr("tools.cq.search.pipeline.smart_search.run_search_partition", _fake_run)
    config = SearchConfig(
        root=Path(),
        query="target",
        mode=QueryMode.IDENTIFIER,
        limits=SearchLimits(),
    )
    result = _run_single_partition(config, "python", mode=QueryMode.IDENTIFIER)

    assert result is expected
    assert isinstance(captured["plan"], SearchPartitionPlanV1)
    assert captured["ctx"] == config
    assert captured["mode"] == QueryMode.IDENTIFIER
