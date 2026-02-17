"""Unit tests for smart-search definition candidate normalization."""

from __future__ import annotations

from pathlib import Path
from typing import cast

from tools.cq.core.locations import SourceSpan
from tools.cq.core.schema import Anchor, DetailPayload, Finding
from tools.cq.search.pipeline.candidate_normalizer import (
    build_definition_candidate_finding,
    definition_kind_from_text,
    definition_name_from_text,
    is_definition_candidate_match,
)
from tools.cq.search.pipeline.classifier import MatchCategory
from tools.cq.search.pipeline.smart_search import EnrichedMatch


def _build_match(
    *, text: str, category: str = "reference", node_kind: str | None = None
) -> EnrichedMatch:
    return EnrichedMatch(
        span=SourceSpan(file="src/lib.rs", start_line=10, start_col=0),
        text=text,
        match_text="compile_target",
        category=cast("MatchCategory", category),
        confidence=0.4,
        evidence_kind="heuristic",
        node_kind=node_kind,
        language="rust",
    )


def _build_finding(match: EnrichedMatch, _root: Path) -> Finding:
    return Finding(
        category=match.category,
        message=match.match_text,
        anchor=Anchor(file=match.file, line=match.line, col=match.col),
        details=DetailPayload(
            kind=match.category,
            data_items=(("match_text", match.match_text),),
        ),
    )


def test_definition_name_parses_pub_crate_fn() -> None:
    """Test definition name parses pub crate fn."""
    text = "pub(crate) async fn compile_target(input: &str) -> String {"
    assert definition_name_from_text(text, fallback="fallback") == "compile_target"
    assert definition_kind_from_text(text) == "function"


def test_is_definition_candidate_match_accepts_definition_text() -> None:
    """Test is definition candidate match accepts definition text."""
    match = _build_match(text="pub(crate) fn compile_target(input: &str) -> String {")
    assert is_definition_candidate_match(match) is True


def test_build_definition_candidate_finding_promotes_rust_visibility_forms() -> None:
    """Test build definition candidate finding promotes rust visibility forms."""
    match = _build_match(
        text="pub(crate) async unsafe fn compile_target(input: &str) -> String {",
        category="reference",
        node_kind="identifier",
    )
    finding = build_definition_candidate_finding(
        match,
        Path(),
        build_finding_fn=_build_finding,
    )
    assert finding is not None
    assert finding.category == "definition"
    assert finding.details.get("name") == "compile_target"
    assert finding.details.get("kind") == "function"
