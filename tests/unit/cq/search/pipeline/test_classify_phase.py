"""Tests for search classification phase."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.core.locations import SourceSpan
from tools.cq.search._shared.types import QueryMode, SearchLimits
from tools.cq.search.pipeline.classify_phase import run_classify_phase
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch, RawMatch


def _raw(file: str) -> RawMatch:
    return RawMatch(
        span=SourceSpan(file=file, start_line=1, start_col=0),
        text="target()",
        match_text="target",
        match_start=0,
        match_end=6,
        match_byte_start=0,
        match_byte_end=6,
    )


def _enriched(raw: RawMatch) -> EnrichedMatch:
    return EnrichedMatch(
        span=raw.span,
        text=raw.text,
        match_text=raw.match_text,
        category="definition",
        confidence=0.9,
        evidence_kind="resolved_ast",
        language="python",
    )


def test_run_classify_phase_filters_by_language_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    """Classify phase filters out matches outside requested language scope."""
    calls: list[str] = []

    def _fake_classify(
        raw: RawMatch,
        root: Path,
        *,
        _lang: str = "python",
        **_kwargs: object,
    ) -> EnrichedMatch:
        _ = root
        calls.append(raw.file)
        return _enriched(raw)

    monkeypatch.setattr("tools.cq.search.pipeline.smart_search.classify_match", _fake_classify)
    config = SearchConfig(
        root=Path(),
        query="target",
        mode=QueryMode.IDENTIFIER,
        limits=SearchLimits(),
    )

    result = run_classify_phase(
        config,
        lang="python",
        raw_matches=[_raw("src/app.py"), _raw("src/lib.rs")],
    )

    assert len(result) == 1
    assert calls == ["src/app.py"]
