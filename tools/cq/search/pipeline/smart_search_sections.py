"""Section-building helpers for Smart Search rendering."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.schema import Anchor, DetailPayload, Finding, ScoreDetails, Section
from tools.cq.core.types import is_python_language
from tools.cq.search._shared.types import QueryMode
from tools.cq.search.objects.render import (
    SearchOccurrenceV1,
    build_non_code_occurrence_section,
    build_occurrence_hot_files_section,
    build_occurrence_kind_counts_section,
    build_occurrences_section,
    build_resolved_objects_section,
    is_non_code_occurrence,
)
from tools.cq.search.objects.resolve import ObjectResolutionRuntime, build_object_resolved_view
from tools.cq.search.pipeline.classifier import MatchCategory
from tools.cq.search.pipeline.enrichment_contracts import (
    incremental_enrichment_payload,
    python_enrichment_payload,
    rust_enrichment_payload,
)
from tools.cq.search.pipeline.smart_search_followups import build_followups
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch


def _evidence_to_bucket(evidence_kind: str) -> str:
    if evidence_kind in {"resolved_ast", "resolved_ast_record"}:
        return "high"
    if evidence_kind in {"heuristic"}:
        return "medium"
    return "low"


def _category_message(category: MatchCategory, match: EnrichedMatch) -> str:
    messages = {
        "definition": "Function/class definition",
        "callsite": "Function call",
        "import": "Import statement",
        "from_import": "From import",
        "reference": "Reference",
        "assignment": "Assignment",
        "annotation": "Type annotation",
        "docstring_match": "Match in docstring",
        "comment_match": "Match in comment",
        "string_match": "Match in string literal",
        "text_match": "Text match",
    }
    base = messages.get(category, "Match")
    if match.containing_scope:
        return f"{base} in {match.containing_scope}"
    return base


def _build_score_details(match: EnrichedMatch) -> ScoreDetails:
    return ScoreDetails(
        confidence_score=match.confidence,
        confidence_bucket=_evidence_to_bucket(match.evidence_kind),
        evidence_kind=match.evidence_kind,
    )


def _populate_optional_fields(data: dict[str, object], match: EnrichedMatch) -> None:
    if match.context_window:
        data["context_window"] = match.context_window
    if match.context_snippet:
        data["context_snippet"] = match.context_snippet
    if match.containing_scope:
        data["containing_scope"] = match.containing_scope
    if match.node_kind:
        data["node_kind"] = match.node_kind


def _merge_enrichment_payloads(data: dict[str, object], match: EnrichedMatch) -> None:
    enrichment: dict[str, object] = {"language": match.language}
    if match.rust_tree_sitter:
        enrichment["rust"] = rust_enrichment_payload(match.rust_tree_sitter)
    python_payload: dict[str, object] | None = None
    if match.python_enrichment:
        python_payload = python_enrichment_payload(match.python_enrichment)
    elif is_python_language(match.language):
        python_payload = {}
    if match.incremental_enrichment:
        incremental_payload = incremental_enrichment_payload(match.incremental_enrichment)
        if python_payload is None:
            python_payload = {}
        python_payload["incremental"] = incremental_payload
    if python_payload is not None:
        enrichment["python"] = python_payload
    if len(enrichment) > 1:
        data["enrichment"] = enrichment


def _build_match_data(match: EnrichedMatch) -> dict[str, object]:
    data: dict[str, object] = {
        "match_text": match.match_text,
        "language": match.language,
    }
    if not match.context_snippet:
        data["line_text"] = match.text
    _populate_optional_fields(data, match)
    _merge_enrichment_payloads(data, match)
    return data


def build_finding(match: EnrichedMatch, _root: Path) -> Finding:
    """Build one finding from an enriched match.

    Returns:
    -------
    Finding
        Render-ready finding contract.
    """
    score = _build_score_details(match)
    data = _build_match_data(match)
    details = DetailPayload(
        kind=match.category,
        score=score,
        data_items=tuple(sorted(data.items())),
    )
    return Finding(
        category=match.category,
        message=_category_message(match.category, match),
        anchor=Anchor.from_span(match.span),
        severity="info",
        details=details,
    )


def _split_occurrences_for_render(
    occurrences: list[SearchOccurrenceV1],
    *,
    include_strings: bool,
) -> tuple[list[SearchOccurrenceV1], list[SearchOccurrenceV1]]:
    if include_strings:
        return occurrences, []
    visible: list[SearchOccurrenceV1] = []
    non_code: list[SearchOccurrenceV1] = []
    for row in occurrences:
        if is_non_code_occurrence(row.category):
            non_code.append(row)
        else:
            visible.append(row)
    return visible, non_code


def _build_followups_section(
    matches: list[EnrichedMatch],
    query: str,
    mode: QueryMode,
) -> Section | None:
    followup_findings = build_followups(matches, query, mode)
    if not followup_findings:
        return None
    return Section(title="Suggested Follow-ups", findings=followup_findings)


def build_sections(
    matches: list[EnrichedMatch],
    root: Path,
    query: str,
    mode: QueryMode,
    *,
    include_strings: bool = False,
    object_runtime: ObjectResolutionRuntime | None = None,
) -> list[Section]:
    """Build object-resolved sections for Smart Search output.

    Returns:
    -------
    list[Section]
        Ordered rendered sections for result output.
    """
    _ = root
    runtime = object_runtime or build_object_resolved_view(matches, query=query)
    visible_occurrences, non_code_occurrences = _split_occurrences_for_render(
        runtime.view.occurrences,
        include_strings=include_strings,
    )
    object_symbols = {
        summary.object_ref.object_id: summary.object_ref.symbol
        for summary in runtime.view.summaries
    }
    occurrences_by_object: dict[str, list[SearchOccurrenceV1]] = {}
    for row in runtime.view.occurrences:
        occurrences_by_object.setdefault(row.object_id, []).append(row)
    occurrence_rows = runtime.view.occurrences if include_strings else visible_occurrences
    sections: list[Section] = [
        build_resolved_objects_section(
            runtime.view.summaries,
            occurrences_by_object=occurrences_by_object,
        ),
        build_occurrences_section(occurrence_rows, object_symbols=object_symbols),
        build_occurrence_kind_counts_section(occurrence_rows),
    ]
    non_code_section = build_non_code_occurrence_section(non_code_occurrences)
    if non_code_section is not None and not include_strings:
        sections.append(non_code_section)
    sections.append(build_occurrence_hot_files_section(occurrence_rows))

    followups_section = _build_followups_section(matches, query, mode)
    if followups_section is not None:
        sections.append(followups_section)

    return sections


__all__ = ["build_finding", "build_sections"]
