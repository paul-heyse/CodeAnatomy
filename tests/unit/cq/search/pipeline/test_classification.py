"""Tests for pipeline classification wrappers."""

from __future__ import annotations

from typing import Any, cast

import pytest
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
