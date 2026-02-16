"""Tests for search enrichment phase orchestration."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search._shared.types import QueryMode, SearchLimits
from tools.cq.search.pipeline.contracts import SearchConfig, SearchPartitionPlanV1
from tools.cq.search.pipeline.enrichment_phase import run_enrichment_phase
from tools.cq.search.pipeline.smart_search_types import LanguageSearchResult, SearchStats


def test_run_enrichment_phase_delegates_to_partition_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enrichment phase delegates plan execution to partition pipeline."""
    expected = LanguageSearchResult(
        lang="python",
        raw_matches=[],
        stats=SearchStats(scanned_files=0, matched_files=0, total_matches=0),
        pattern="target",
        enriched_matches=[],
        dropped_by_scope=0,
        python_semantic_prefetch=None,
    )
    captured: dict[str, object] = {}

    def _fake_run(
        plan: SearchPartitionPlanV1, *, ctx: SearchConfig, mode: QueryMode
    ) -> LanguageSearchResult:
        captured["plan"] = plan
        captured["ctx"] = ctx
        captured["mode"] = mode
        return expected

    monkeypatch.setattr("tools.cq.search.pipeline.enrichment_phase.run_search_partition", _fake_run)
    config = SearchConfig(
        root=Path(),
        query="target",
        mode=QueryMode.IDENTIFIER,
        limits=SearchLimits(),
    )
    plan = SearchPartitionPlanV1(
        root=str(Path().resolve()),
        language="python",
        query="target",
        mode="identifier",
    )

    result = run_enrichment_phase(plan, config=config, mode=QueryMode.IDENTIFIER)

    assert result is expected
    assert captured["plan"] == plan
    assert captured["ctx"] == config
    assert captured["mode"] == QueryMode.IDENTIFIER
