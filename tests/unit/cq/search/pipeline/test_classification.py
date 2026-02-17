"""Tests for pipeline classification wrappers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
from tools.cq.search._shared.enrichment_contracts import IncrementalEnrichmentModeV1
from tools.cq.search._shared.types import QueryMode, SearchLimits
from tools.cq.search.pipeline import smart_search as classification_module
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search import RawMatch


def test_run_classification_phase_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test run classification phase delegates."""
    sentinel = ["enriched"]
    context = cast("SearchConfig", object())

    def _fake(
        context: object,
        *,
        lang: object,
        raw_matches: object,
        cache_context: object,
    ) -> Any:
        assert context is not None
        assert lang == "python"
        assert isinstance(raw_matches, list)
        assert isinstance(cache_context, ClassifierCacheContext)
        return sentinel

    monkeypatch.setattr(classification_module, "run_classify_phase", _fake)
    assert (
        classification_module.run_classification_phase(
            context,
            lang="python",
            raw_matches=cast("list[RawMatch]", []),
            cache_context=ClassifierCacheContext(),
        )
        == sentinel
    )


def test_run_classification_phase_preserves_incremental_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classification wrapper preserves incremental settings on search config."""
    captured: dict[str, object] = {}
    sentinel = ["ok"]

    def _fake(
        context: SearchConfig,
        *,
        lang: object,
        raw_matches: object,
        cache_context: object,
    ) -> Any:
        captured["context"] = context
        captured["lang"] = lang
        captured["raw_matches"] = raw_matches
        captured["cache_context"] = cache_context
        return sentinel

    monkeypatch.setattr(classification_module, "run_classify_phase", _fake)
    context = SearchConfig(
        root=Path(),
        query="target",
        mode=QueryMode.IDENTIFIER,
        limits=SearchLimits(),
        incremental_enrichment_enabled=False,
        incremental_enrichment_mode=IncrementalEnrichmentModeV1.FULL,
    )

    result = classification_module.run_classification_phase(
        context,
        lang="python",
        raw_matches=cast("list[RawMatch]", []),
        cache_context=ClassifierCacheContext(),
    )

    assert result == sentinel
    seen = cast("SearchConfig", captured["context"])
    assert seen.incremental_enrichment_enabled is False
    assert seen.incremental_enrichment_mode is IncrementalEnrichmentModeV1.FULL
