"""Contracts for object-resolved search aggregation."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqOutputStruct, CqStruct
from tools.cq.query.language import QueryLanguage


class ResolvedObjectRef(CqOutputStruct, frozen=True):
    """Canonical object identity derived from one or more match evidence planes."""

    object_id: str
    language: QueryLanguage
    symbol: str
    qualified_name: str | None = None
    kind: str | None = None
    canonical_file: str | None = None
    canonical_line: int | None = None
    resolution_quality: str = "weak"
    evidence_planes: tuple[str, ...] = ()
    agreement: str | None = None
    fallback_used: bool = False


class SearchOccurrenceV1(CqOutputStruct, frozen=True):
    """One concrete occurrence attached to an object identity."""

    occurrence_id: str
    object_id: str
    file: str
    line: int
    line_id: str | None = None
    col: int | None = None
    block_start_line: int | None = None
    block_end_line: int | None = None
    context_start_line: int | None = None
    context_end_line: int | None = None
    byte_start: int | None = None
    byte_end: int | None = None
    category: str = "reference"
    node_kind: str | None = None
    containing_scope: str | None = None
    confidence: float | None = None
    evidence_kind: str | None = None


class SearchObjectSummaryV1(CqOutputStruct, frozen=True):
    """One deduplicated object summary with shared code facts."""

    object_ref: ResolvedObjectRef
    occurrence_count: int
    files: list[str] = msgspec.field(default_factory=list)
    representative_category: str | None = None
    code_facts: dict[str, object] = msgspec.field(default_factory=dict)
    coverage_level: str = "structural_only"
    applicability: dict[str, str] = msgspec.field(default_factory=dict)
    coverage_reasons: tuple[str, ...] = ()


class SearchObjectResolvedViewV1(CqStruct, frozen=True):
    """Serializable object-resolved search view."""

    summaries: list[SearchObjectSummaryV1] = msgspec.field(default_factory=list)
    occurrences: list[SearchOccurrenceV1] = msgspec.field(default_factory=list)
    snippets: dict[str, str] = msgspec.field(default_factory=dict)


__all__ = [
    "ResolvedObjectRef",
    "SearchObjectResolvedViewV1",
    "SearchObjectSummaryV1",
    "SearchOccurrenceV1",
]
