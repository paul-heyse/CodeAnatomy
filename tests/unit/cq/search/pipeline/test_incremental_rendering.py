"""Tests for incremental payload rendering into findings."""

from __future__ import annotations

from pathlib import Path
from typing import cast

from tools.cq.core.locations import SourceSpan
from tools.cq.search._shared.enrichment_contracts import (
    IncrementalEnrichmentModeV1,
    IncrementalEnrichmentV1,
)
from tools.cq.search.enrichment.incremental_facts import IncrementalFacts
from tools.cq.search.pipeline.classifier import MatchCategory
from tools.cq.search.pipeline.smart_search_sections import build_finding
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch


def test_build_finding_embeds_incremental_payload(tmp_path: Path) -> None:
    """Embed incremental enrichment payload in rendered finding details."""
    match = EnrichedMatch(
        span=SourceSpan(file="src/mod.py", start_line=1, start_col=0),
        text="target()",
        match_text="target",
        category=cast("MatchCategory", "definition"),
        confidence=0.9,
        evidence_kind="resolved_ast",
        language="python",
        incremental_enrichment=IncrementalEnrichmentV1(
            mode=IncrementalEnrichmentModeV1.TS_SYM,
            payload=IncrementalFacts(sym={"scope_graph": {"tables_count": 2}}),
        ),
    )
    finding = build_finding(match, tmp_path)
    enrichment = finding.details.get("enrichment")
    assert isinstance(enrichment, dict)
    python_payload = enrichment.get("python")
    assert isinstance(python_payload, dict)
    incremental = python_payload.get("incremental")
    assert isinstance(incremental, dict)
    assert incremental.get("mode") == "ts_sym"
