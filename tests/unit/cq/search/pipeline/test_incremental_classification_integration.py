"""Tests for incremental classification integration."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.core.locations import SourceSpan
from tools.cq.search._shared.enrichment_contracts import IncrementalEnrichmentModeV1
from tools.cq.search.pipeline.classification import (
    ClassificationResult,
    classify_match,
)
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
from tools.cq.search.pipeline.smart_search_types import MatchClassifyOptions, RawMatch


def test_classify_match_attaches_incremental(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Attach incremental enrichment payload during classify_match execution."""
    file_path = tmp_path / "sample.py"
    line_text = "value = foo(value)"
    file_path.write_text(f"{line_text}\n", encoding="utf-8")

    def _fake_classification(_request: object) -> ClassificationResult:
        return ClassificationResult(
            category="reference",
            confidence=0.9,
            evidence_kind="heuristic",
            node_kind="identifier",
            containing_scope=None,
        )

    monkeypatch.setattr(
        "tools.cq.search.pipeline.classification._resolve_match_classification",
        _fake_classification,
    )

    raw = RawMatch(
        span=SourceSpan(file="sample.py", start_line=1, start_col=8),
        text=line_text,
        match_text="foo",
        match_start=8,
        match_end=11,
        match_byte_start=8,
        match_byte_end=11,
    )

    enriched = classify_match(
        raw,
        tmp_path,
        lang="python",
        cache_context=ClassifierCacheContext(),
        options=MatchClassifyOptions(
            incremental_enabled=True,
            incremental_mode=IncrementalEnrichmentModeV1.TS_SYM,
        ),
    )
    assert enriched.incremental_enrichment is not None
    assert enriched.incremental_enrichment.mode.value == "ts_sym"
