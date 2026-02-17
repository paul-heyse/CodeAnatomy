"""Tests for search candidate phase."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search._shared.requests import CandidateCollectionRequest
from tools.cq.search._shared.types import QueryMode, SearchLimits
from tools.cq.search.pipeline.candidate_phase import run_candidate_phase
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search_types import RawMatch, SearchStats


def test_run_candidate_phase_identifier_escapes_pattern(monkeypatch: pytest.MonkeyPatch) -> None:
    """Identifier-mode candidate phase escapes dotted query text."""
    monkeypatch.setattr(
        "tools.cq.search.pipeline.candidate_phase._adaptive_limits_from_count",
        lambda request, *, _lang: request.limits or SearchLimits(),
    )
    captured: dict[str, object] = {}

    def _fake_collect(request: CandidateCollectionRequest) -> tuple[list[RawMatch], SearchStats]:
        captured["pattern"] = request.pattern
        captured["mode"] = request.mode
        captured["lang"] = request.lang
        return [], SearchStats(scanned_files=0, matched_files=0, total_matches=0)

    monkeypatch.setattr(
        "tools.cq.search.pipeline.candidate_phase.collect_candidates", _fake_collect
    )
    config = SearchConfig(
        root=Path(),
        query="pkg.target",
        mode=QueryMode.IDENTIFIER,
        limits=SearchLimits(),
    )

    _, _, pattern = run_candidate_phase(config, lang="python", mode=QueryMode.IDENTIFIER)

    assert pattern == r"pkg\.target"
    assert captured["pattern"] == r"pkg\.target"
    assert captured["mode"] == QueryMode.IDENTIFIER
    assert captured["lang"] == "python"
