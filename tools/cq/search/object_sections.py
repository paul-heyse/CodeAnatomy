"""Section builders for object-resolved smart-search output."""

from __future__ import annotations

from collections import Counter

from tools.cq.core.schema import Anchor, DetailPayload, Finding, Section
from tools.cq.search.object_resolution_contracts import SearchObjectSummaryV1, SearchOccurrenceV1

_NON_CODE_CATEGORIES: frozenset[str] = frozenset(
    {
        "docstring_match",
        "comment_match",
        "string_match",
    }
)
_DEFAULT_CONTEXT_LINE = 1
_MAX_SECTION_ROWS = 200
_MAX_NON_CODE_ROWS = 100


def is_non_code_occurrence(category: str) -> bool:
    """Return whether occurrence category is non-code text."""
    return category in _NON_CODE_CATEGORIES


def build_resolved_objects_section(
    summaries: list[SearchObjectSummaryV1],
    *,
    occurrences_by_object: dict[str, list[SearchOccurrenceV1]] | None = None,
) -> Section:
    """Build one summary finding per resolved object.

    Returns:
        Section: Section with resolved-object findings.
    """
    findings: list[Finding] = []
    by_object = occurrences_by_object or {}
    for summary in summaries[:_MAX_SECTION_ROWS]:
        object_ref = summary.object_ref
        kind = object_ref.kind or "reference"
        symbol = object_ref.symbol
        object_occurrences = by_object.get(object_ref.object_id, [])
        occurrence_rows = [
            {
                "line_id": row.line_id or row.occurrence_id,
                "file": row.file,
                "line": row.line,
                "col": row.col,
                "block_start_line": row.block_start_line or row.context_start_line,
                "block_end_line": row.block_end_line or row.context_end_line,
                "context_start_line": row.context_start_line,
                "context_end_line": row.context_end_line,
                "block_ref": _format_block(
                    row.file,
                    row.block_start_line or row.context_start_line,
                    row.block_end_line or row.context_end_line,
                ),
                "category": row.category,
                "byte_start": row.byte_start,
                "byte_end": row.byte_end,
            }
            for row in object_occurrences
        ]
        details_data: dict[str, object] = {
            "name": symbol,
            "kind": kind,
            "object_id": object_ref.object_id,
            "qualified_name": object_ref.qualified_name,
            "occurrence_count": summary.occurrence_count,
            "files": list(summary.files),
            "resolution_quality": object_ref.resolution_quality,
            "evidence_planes": list(object_ref.evidence_planes),
            "agreement": object_ref.agreement,
            "fallback_used": object_ref.fallback_used,
            "coverage_level": summary.coverage_level,
            "applicability": dict(summary.applicability),
            "coverage_reasons": list(summary.coverage_reasons),
            "occurrences": occurrence_rows,
        }
        enrichment_payload = summary.code_facts.get("enrichment")
        if isinstance(enrichment_payload, dict) and enrichment_payload:
            details_data["enrichment"] = enrichment_payload
        if summary.code_facts:
            details_data["code_facts"] = dict(summary.code_facts)

        anchor: Anchor | None = None
        if isinstance(object_ref.canonical_file, str) and isinstance(
            object_ref.canonical_line, int
        ):
            anchor = Anchor(
                file=object_ref.canonical_file,
                line=max(_DEFAULT_CONTEXT_LINE, int(object_ref.canonical_line)),
            )
        findings.append(
            Finding(
                category="resolved_object",
                message=(
                    f"{kind}: {symbol} ({summary.occurrence_count} occurrences)"
                    f" [object_id={object_ref.object_id}]"
                ),
                anchor=anchor,
                severity="info",
                details=DetailPayload(kind=kind, data=details_data),
            )
        )
    return Section(title="Resolved Objects", findings=findings)


def build_occurrences_section(
    occurrences: list[SearchOccurrenceV1],
    *,
    object_symbols: dict[str, str],
) -> Section:
    """Build occurrence listing section with location and enclosing block bounds.

    Returns:
        Section: Section with occurrence findings.
    """
    findings: list[Finding] = []
    for row in occurrences[:_MAX_SECTION_ROWS]:
        symbol = object_symbols.get(row.object_id, row.object_id)
        location = _format_location(row.file, row.line, row.col)
        block = _format_block(
            row.file,
            row.block_start_line or row.context_start_line,
            row.block_end_line or row.context_end_line,
        )
        findings.append(
            Finding(
                category="occurrence",
                message=(
                    f"{symbol}: {location} (block {block}) [{row.category}] "
                    f"[object_id={row.object_id}] [line_id={row.line_id or row.occurrence_id}]"
                ),
                anchor=Anchor(
                    file=row.file, line=max(_DEFAULT_CONTEXT_LINE, row.line), col=row.col
                ),
                severity="info",
                details=DetailPayload(
                    kind="occurrence",
                    data={
                        "occurrence_id": row.occurrence_id,
                        "line_id": row.line_id or row.occurrence_id,
                        "object_id": row.object_id,
                        "symbol": symbol,
                        "file": row.file,
                        "line": row.line,
                        "col": row.col,
                        "block_start_line": row.block_start_line or row.context_start_line,
                        "block_end_line": row.block_end_line or row.context_end_line,
                        "context_start_line": row.context_start_line,
                        "context_end_line": row.context_end_line,
                        "byte_start": row.byte_start,
                        "byte_end": row.byte_end,
                        "category": row.category,
                        "node_kind": row.node_kind,
                        "containing_scope": row.containing_scope,
                        "confidence": row.confidence,
                        "evidence_kind": row.evidence_kind,
                    },
                ),
            )
        )
    return Section(title="Occurrences", findings=findings)


def build_occurrence_kind_counts_section(occurrences: list[SearchOccurrenceV1]) -> Section:
    """Build category histogram section from occurrence rows.

    Returns:
        Section: Section with category histogram findings.
    """
    counts = Counter(row.category for row in occurrences)
    findings = [
        Finding(
            category="count",
            message=f"{category}: {count}",
            severity="info",
            details=DetailPayload(
                kind="count",
                data={"category": category, "count": count},
            ),
        )
        for category, count in counts.most_common()
    ]
    return Section(title="Uses by Kind", findings=findings, collapsed=True)


def build_occurrence_hot_files_section(occurrences: list[SearchOccurrenceV1]) -> Section:
    """Build top files section from occurrence rows.

    Returns:
        Section: Section with hot file findings.
    """
    counts = Counter(row.file for row in occurrences)
    findings = [
        Finding(
            category="hot_file",
            message=f"{file}: {count} matches",
            anchor=Anchor(file=file, line=_DEFAULT_CONTEXT_LINE),
            severity="info",
            details=DetailPayload(kind="hot_file", data={"count": count}),
        )
        for file, count in counts.most_common(10)
    ]
    return Section(title="Hot Files", findings=findings, collapsed=True)


def build_non_code_occurrence_section(occurrences: list[SearchOccurrenceV1]) -> Section | None:
    """Build collapsed non-code section from filtered occurrences.

    Returns:
        Section | None: Section when non-code rows exist, else ``None``.
    """
    non_code = [row for row in occurrences if is_non_code_occurrence(row.category)]
    if not non_code:
        return None
    findings = [
        Finding(
            category="non_code_occurrence",
            message=f"{row.file}:{row.line} [{row.category}]",
            anchor=Anchor(file=row.file, line=max(_DEFAULT_CONTEXT_LINE, row.line), col=row.col),
            severity="info",
            details=DetailPayload(
                kind="non_code_occurrence",
                data={
                    "occurrence_id": row.occurrence_id,
                    "object_id": row.object_id,
                    "category": row.category,
                },
            ),
        )
        for row in non_code[:_MAX_NON_CODE_ROWS]
    ]
    return Section(
        title="Non-Code Matches (Strings / Comments / Docstrings)",
        findings=findings,
        collapsed=True,
    )


def _format_location(file: str, line: int, col: int | None) -> str:
    if col is None:
        return f"{file}:{line}"
    return f"{file}:{line}:{col}"


def _format_block(file: str, start_line: int | None, end_line: int | None) -> str:
    if isinstance(start_line, int) and isinstance(end_line, int):
        return f"{file}:{start_line}-{end_line}"
    return f"{file}:?"


__all__ = [
    "build_non_code_occurrence_section",
    "build_occurrence_hot_files_section",
    "build_occurrence_kind_counts_section",
    "build_occurrences_section",
    "build_resolved_objects_section",
    "is_non_code_occurrence",
]
