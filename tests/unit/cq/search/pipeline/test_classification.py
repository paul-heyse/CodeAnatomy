"""Tests for pipeline classification wrappers."""

from __future__ import annotations

from typing import Any, cast

import pytest
from tools.cq.search.pipeline import smart_search as classification_module
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search import RawMatch


def test_run_classification_phase_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test run classification phase delegates."""
    sentinel = ["enriched"]
    context = cast("SearchConfig", object())

    def _fake(context: object, *, lang: object, raw_matches: object) -> Any:
        assert context is not None
        assert lang == "python"
        assert isinstance(raw_matches, list)
        return sentinel

    monkeypatch.setattr(classification_module, "_run_classification_phase", _fake)
    assert (
        classification_module.run_classification_phase(
            context,
            lang="python",
            raw_matches=cast("list[RawMatch]", []),
        )
        == sentinel
    )
