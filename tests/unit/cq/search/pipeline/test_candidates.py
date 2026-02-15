"""Tests for pipeline candidate-phase wrappers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from tools.cq.search.pipeline import smart_search as candidates_module
from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.models import SearchConfig


def test_run_candidate_phase_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = (["raw"], SimpleNamespace(total=1), "pattern")
    context = cast("SearchConfig", object())

    def _fake(context: object, *, lang: object, mode: object) -> Any:
        assert context is not None
        assert lang == "python"
        assert mode == QueryMode.REGEX
        return sentinel

    monkeypatch.setattr(candidates_module, "_run_candidate_phase", _fake)
    assert (
        candidates_module.run_candidate_phase(
            context,
            lang="python",
            mode=QueryMode.REGEX,
        )
        == sentinel
    )
