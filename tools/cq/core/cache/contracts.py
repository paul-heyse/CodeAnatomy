"""Typed cache payload contracts for CQ runtime cache namespaces."""

from __future__ import annotations

import msgspec

from tools.cq.astgrep.sgpy_scanner import RecordType
from tools.cq.core.contracts_constraints import NonNegativeInt
from tools.cq.core.structs import CqCacheStruct
from tools.cq.search.objects.render import SearchObjectSummaryV1, SearchOccurrenceV1
from tools.cq.search.tree_sitter.contracts.core_models import TreeSitterArtifactBundleV1


class SgRecordCacheV1(CqCacheStruct, frozen=True):
    """Cache-safe serialization contract for one ast-grep record."""

    record: RecordType = "def"
    kind: str = ""
    file: str = ""
    start_line: NonNegativeInt = 0
    start_col: NonNegativeInt = 0
    end_line: NonNegativeInt = 0
    end_col: NonNegativeInt = 0
    text: str = ""
    rule_id: str = ""


class SearchPartitionCacheV1(CqCacheStruct, frozen=True):
    """Legacy aggregate search partition payload (kept for compatibility)."""

    pattern: str
    raw_matches: list[dict[str, object]]
    stats: dict[str, object]
    enriched_matches: list[dict[str, object]]


class SearchCandidatesCacheV1(CqCacheStruct, frozen=True):
    """Cached search candidate payload."""

    pattern: str
    raw_matches: list[dict[str, object]]
    stats: dict[str, object]


class SearchEnrichmentAnchorCacheV1(CqCacheStruct, frozen=True):
    """Cached enrichment payload for one search anchor."""

    file: str
    line: NonNegativeInt
    col: NonNegativeInt
    match_text: str
    file_content_hash: str
    language: str
    enriched_match: dict[str, object]


class QueryEntityScanCacheV1(CqCacheStruct, frozen=True):
    """Cached entity-query scan payload."""

    records: list[SgRecordCacheV1]


class PatternFragmentCacheV1(CqCacheStruct, frozen=True):
    """Cached per-file ast-grep pattern fragment payload."""

    findings: list[dict[str, object]] = msgspec.field(default_factory=list)
    records: list[SgRecordCacheV1] = msgspec.field(default_factory=list)
    raw_matches: list[dict[str, object]] = msgspec.field(default_factory=list)


class ScopeFileStatCacheV1(CqCacheStruct, frozen=True):
    """Stable file stat tuple persisted in scope snapshot artifacts."""

    path: str
    size_bytes: NonNegativeInt = 0
    mtime_ns: NonNegativeInt = 0


class ScopeSnapshotCacheV1(CqCacheStruct, frozen=True):
    """Cached scope snapshot fingerprint payload."""

    language: str
    scope_globs: tuple[str, ...] = ()
    scope_roots: tuple[str, ...] = ()
    inventory_token: dict[str, object] = msgspec.field(default_factory=dict)
    files: list[ScopeFileStatCacheV1] = msgspec.field(default_factory=list)
    digest: str = ""


class CallsTargetCacheV1(CqCacheStruct, frozen=True):
    """Cached calls-target metadata payload."""

    target_location: tuple[str, int] | None = None
    target_callees: dict[str, int] = msgspec.field(default_factory=dict)
    snapshot_digest: str | None = None


class SearchArtifactBundleV1(CqCacheStruct, frozen=True):
    """Cache-backed object-resolved search artifact bundle."""

    run_id: str
    query: str
    macro: str = "search"
    summary: dict[str, object] = msgspec.field(default_factory=dict)
    object_summaries: list[SearchObjectSummaryV1] = msgspec.field(default_factory=list)
    occurrences: list[SearchOccurrenceV1] = msgspec.field(default_factory=list)
    diagnostics: dict[str, object] = msgspec.field(default_factory=dict)
    snippets: dict[str, str] = msgspec.field(default_factory=dict)
    created_ms: float = 0.0


class SearchArtifactIndexEntryV1(CqCacheStruct, frozen=True):
    """Index row for one cache-backed search artifact bundle."""

    run_id: str
    cache_key: str
    query: str
    macro: str = "search"
    created_ms: float = 0.0


class SearchArtifactIndexV1(CqCacheStruct, frozen=True):
    """Run-scoped index of cache-backed search artifact bundles."""

    entries: list[SearchArtifactIndexEntryV1] = msgspec.field(default_factory=list)


__all__ = [
    "CallsTargetCacheV1",
    "PatternFragmentCacheV1",
    "QueryEntityScanCacheV1",
    "ScopeFileStatCacheV1",
    "ScopeSnapshotCacheV1",
    "SearchArtifactBundleV1",
    "SearchArtifactIndexEntryV1",
    "SearchArtifactIndexV1",
    "SearchCandidatesCacheV1",
    "SearchEnrichmentAnchorCacheV1",
    "SearchPartitionCacheV1",
    "SgRecordCacheV1",
    "TreeSitterArtifactBundleV1",
]
