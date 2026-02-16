"""Tests for pipeline enrichment prefetch wrappers."""

from __future__ import annotations

from typing import Any, cast

import pytest
from tools.cq.search.pipeline import python_semantic as enrichment_module
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search_types import RawMatch


def test_run_enrichment_prefetch_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test run enrichment prefetch delegates."""
    sentinel: dict[str, object] = {"payloads": {}}
    context = cast("SearchConfig", object())

    def _fake(context: object, *, lang: object, raw_matches: object) -> Any:
        assert context is not None
        assert lang == "python"
        assert isinstance(raw_matches, list)
        return sentinel

    monkeypatch.setattr(enrichment_module, "_prefetch_python_semantic_for_raw_matches", _fake)
    assert (
        enrichment_module.run_prefetch_python_semantic_for_raw_matches(
            context,
            lang="python",
            raw_matches=cast("list[RawMatch]", []),
        )
        == sentinel
    )
