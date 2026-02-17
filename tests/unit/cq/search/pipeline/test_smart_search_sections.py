"""Tests for smart-search section helpers."""

from __future__ import annotations

from pathlib import Path
from typing import cast

from tools.cq.core.locations import SourceSpan
from tools.cq.search._shared.types import QueryMode
from tools.cq.search.pipeline.classifier import MatchCategory
from tools.cq.search.pipeline.enrichment_contracts import (
    IncrementalEnrichmentModeV1,
    IncrementalEnrichmentV1,
)
from tools.cq.search.pipeline.smart_search_sections import build_finding, build_sections
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch


def _match(category: str, *, with_incremental: bool = False) -> EnrichedMatch:
    incremental = (
        IncrementalEnrichmentV1(
            mode=IncrementalEnrichmentModeV1.TS_SYM_DIS,
            payload={"sym": {"binding_id": "sample:1:2:target"}},
        )
        if with_incremental
        else None
    )
    return EnrichedMatch(
        span=SourceSpan(file="src/mod.py", start_line=1, start_col=0),
        text="target()",
        match_text="target",
        category=cast("MatchCategory", category),
        confidence=0.9,
        evidence_kind="resolved_ast",
        language="python",
        incremental_enrichment=incremental,
    )


def test_build_finding_shapes_details(tmp_path: Path) -> None:
    """Build finding helper maps enriched match fields into details."""
    finding = build_finding(_match("definition"), tmp_path)
    assert finding.category == "definition"
    assert finding.anchor is not None
    assert finding.anchor.file == "src/mod.py"
    assert finding.details.get("match_text") == "target"


def test_build_sections_includes_followups(tmp_path: Path) -> None:
    """Section builder includes follow-up section for identifier matches."""
    sections = build_sections(
        [_match("definition")],
        tmp_path,
        "target",
        QueryMode.IDENTIFIER,
    )
    titles = [section.title for section in sections]
    assert "Resolved Objects" in titles
    assert "Suggested Follow-ups" in titles


def test_build_finding_embeds_incremental_enrichment_payload(tmp_path: Path) -> None:
    """Finding details include incremental payload under enrichment.python.incremental."""
    finding = build_finding(_match("reference", with_incremental=True), tmp_path)
    enrichment = finding.details.get("enrichment")
    assert isinstance(enrichment, dict)
    python_enrichment = enrichment.get("python")
    assert isinstance(python_enrichment, dict)
    incremental = python_enrichment.get("incremental")
    assert isinstance(incremental, dict)
    assert incremental.get("mode") == "ts_sym_dis"
    assert "symtable" not in enrichment
    assert "python_semantic" not in enrichment
