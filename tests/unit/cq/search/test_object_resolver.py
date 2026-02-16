"""Tests for object-resolved search aggregation."""

from __future__ import annotations

from tools.cq.core.locations import SourceSpan
from tools.cq.search.objects.resolve import build_object_resolved_view
from tools.cq.search.pipeline.smart_search import EnrichedMatch

OCCURRENCE_COUNT_TWO = 2
BLOCK_START_LINE = 8
BLOCK_END_LINE = 12
DEFAULT_CONTEXT_LINE = 40


def _span(file: str, line: int, col: int = 0) -> SourceSpan:
    return SourceSpan(
        file=file,
        start_line=line,
        start_col=col,
        end_line=line,
        end_col=col + 1,
    )


def test_object_resolver_groups_occurrences_by_qualified_name() -> None:
    """Test object resolver groups occurrences by qualified name."""
    matches = [
        EnrichedMatch(
            span=_span("src/a.py", 10),
            text="build_graph(x)",
            match_text="build_graph",
            category="callsite",
            confidence=0.9,
            evidence_kind="resolved_ast",
            python_enrichment={
                "resolution": {
                    "qualified_name_candidates": [{"name": "pkg.core.build_graph"}],
                }
            },
            context_window={"start_line": 8, "end_line": 12},
            context_snippet="def run():\n    build_graph(x)",
        ),
        EnrichedMatch(
            span=_span("src/b.py", 22),
            text="build_graph(y)",
            match_text="build_graph",
            category="callsite",
            confidence=0.8,
            evidence_kind="resolved_ast",
            python_enrichment={
                "resolution": {
                    "qualified_name_candidates": [{"name": "pkg.core.build_graph"}],
                }
            },
            context_window={"start_line": 20, "end_line": 25},
            context_snippet="def run2():\n    build_graph(y)",
        ),
    ]

    runtime = build_object_resolved_view(matches, query="build_graph")
    assert len(runtime.view.summaries) == 1
    assert runtime.view.summaries[0].occurrence_count == OCCURRENCE_COUNT_TWO
    assert len(runtime.view.occurrences) == OCCURRENCE_COUNT_TWO
    assert runtime.view.occurrences[0].line_id is not None
    assert runtime.view.occurrences[0].block_start_line == BLOCK_START_LINE
    assert runtime.view.occurrences[0].block_end_line == BLOCK_END_LINE
    assert runtime.view.occurrences[0].context_start_line == BLOCK_START_LINE
    assert runtime.view.occurrences[0].context_end_line == BLOCK_END_LINE
    assert len(runtime.view.snippets) == OCCURRENCE_COUNT_TWO


def test_object_resolver_marks_non_callable_applicability() -> None:
    """Test object resolver marks non callable applicability."""
    match = EnrichedMatch(
        span=_span("src/model.py", 12),
        text="stable_id = value",
        match_text="stable_id",
        category="reference",
        confidence=0.7,
        evidence_kind="resolved_ast",
        python_enrichment={
            "structural": {"item_role": "attribute"},
            "resolution": {"binding_candidates": [{"name": "stable_id"}]},
        },
    )

    runtime = build_object_resolved_view([match], query="stable_id")
    summary = runtime.view.summaries[0]
    assert summary.coverage_level == "partial_signal"
    assert summary.applicability["call_graph"] == "not_applicable_non_callable"
    assert summary.applicability["type_contract"] == "not_applicable_non_callable"


def test_object_resolver_defaults_block_bounds_to_match_line() -> None:
    """Test object resolver defaults block bounds to match line."""
    match = EnrichedMatch(
        span=_span("src/model.py", 40),
        text="stable_id",
        match_text="stable_id",
        category="reference",
        confidence=0.7,
        evidence_kind="heuristic",
    )
    runtime = build_object_resolved_view([match], query="stable_id")
    occurrence = runtime.view.occurrences[0]
    assert occurrence.block_start_line == DEFAULT_CONTEXT_LINE
    assert occurrence.block_end_line == DEFAULT_CONTEXT_LINE
    assert occurrence.context_start_line == DEFAULT_CONTEXT_LINE
    assert occurrence.context_end_line == DEFAULT_CONTEXT_LINE


def test_object_resolver_omits_empty_python_enrichment_payload() -> None:
    """Test object resolver omits empty python enrichment payload."""
    match = EnrichedMatch(
        span=_span("src/model.py", 12),
        text="stable_id = value",
        match_text="stable_id",
        category="reference",
        confidence=0.7,
        evidence_kind="resolved_ast",
        language="python",
    )
    runtime = build_object_resolved_view([match], query="stable_id")
    code_facts = runtime.view.summaries[0].code_facts
    enrichment = code_facts.get("enrichment")
    assert isinstance(enrichment, dict)
    assert enrichment.get("language") == "python"
    assert "python" not in enrichment
