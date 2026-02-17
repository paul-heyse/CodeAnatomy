"""Tests for target-resolution extraction helpers."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tools.cq.core.locations import SourceSpan
from tools.cq.search._shared.types import QueryMode, SearchLimits
from tools.cq.search.objects.render import (
    ResolvedObjectRef,
    SearchObjectResolvedViewV1,
    SearchObjectSummaryV1,
)
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch
from tools.cq.search.pipeline.target_resolution import (
    collect_definition_candidates,
    resolve_primary_target_match,
)


def _config(root: Path) -> SearchConfig:
    return SearchConfig(
        root=root,
        query="target",
        mode=QueryMode.IDENTIFIER,
        limits=SearchLimits(),
        with_neighborhood=True,
        argv=["search", "target"],
        started_ms=0.0,
    )


def _match(*, file: str = "a.py", line: int = 1) -> EnrichedMatch:
    return EnrichedMatch(
        span=SourceSpan(file=file, start_line=line, start_col=0),
        text="def target(): pass",
        match_text="target",
        category="definition",
        confidence=1.0,
        evidence_kind="classifier",
        language="python",
    )


def test_collect_definition_candidates_builds_target_candidate_findings(tmp_path: Path) -> None:
    """Target resolution should produce candidate findings from object summaries."""
    representative = _match()
    object_ref = ResolvedObjectRef(
        object_id="obj-1",
        language="python",
        symbol="target",
        kind="function",
        canonical_file="a.py",
        canonical_line=1,
        resolution_quality="strong",
    )
    summary = SearchObjectSummaryV1(object_ref=object_ref, occurrence_count=1, files=["a.py"])
    object_runtime = SimpleNamespace(
        view=SearchObjectResolvedViewV1(summaries=[summary], occurrences=[], snippets={}),
        representative_matches={"obj-1": representative},
    )

    definition_matches, candidate_findings = collect_definition_candidates(
        _config(tmp_path),
        object_runtime,
    )

    assert definition_matches == [representative]
    assert len(candidate_findings) == 1
    assert candidate_findings[0].details.get("object_id") == "obj-1"


def test_resolve_primary_target_match_prefers_candidate_object_id(tmp_path: Path) -> None:
    """Primary target resolution should prefer candidate object-id representative."""
    representative = _match()
    object_ref = ResolvedObjectRef(
        object_id="obj-1",
        language="python",
        symbol="target",
        kind="function",
    )
    summary = SearchObjectSummaryV1(object_ref=object_ref, occurrence_count=1, files=["a.py"])
    object_runtime = SimpleNamespace(
        view=SearchObjectResolvedViewV1(summaries=[summary], occurrences=[], snippets={}),
        representative_matches={"obj-1": representative},
    )
    candidate_findings = collect_definition_candidates(_config(tmp_path), object_runtime)[1]

    primary = resolve_primary_target_match(
        candidate_findings=candidate_findings,
        object_runtime=object_runtime,
        definition_matches=[],
        enriched_matches=[],
    )

    assert primary is representative
