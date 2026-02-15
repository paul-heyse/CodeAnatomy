"""Tests for object-resolved search aggregation."""

from __future__ import annotations

from tools.cq.core.locations import SourceSpan
from tools.cq.search.object_resolver import build_object_resolved_view
from tools.cq.search.smart_search import EnrichedMatch


def _span(file: str, line: int, col: int = 0) -> SourceSpan:
    return SourceSpan(
        file=file,
        start_line=line,
        start_col=col,
        end_line=line,
        end_col=col + 1,
    )


def test_object_resolver_groups_occurrences_by_qualified_name() -> None:
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
    assert runtime.view.summaries[0].occurrence_count == 2
    assert len(runtime.view.occurrences) == 2
    assert runtime.view.occurrences[0].context_start_line == 8
    assert runtime.view.occurrences[0].context_end_line == 12
    assert len(runtime.view.snippets) == 2


def test_object_resolver_marks_non_callable_applicability() -> None:
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
    assert occurrence.context_start_line == 40
    assert occurrence.context_end_line == 40


def test_object_resolver_omits_empty_python_enrichment_payload() -> None:
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
