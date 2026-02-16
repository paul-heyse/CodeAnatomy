"""Search-scoped cache payload contracts."""

from __future__ import annotations

import msgspec

from tools.cq.astgrep.sgpy_scanner import RecordType
from tools.cq.core.contracts_constraints import NonNegativeInt
from tools.cq.core.structs import CqCacheStruct


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


__all__ = [
    "PatternFragmentCacheV1",
    "QueryEntityScanCacheV1",
    "SearchCandidatesCacheV1",
    "SearchEnrichmentAnchorCacheV1",
    "SearchPartitionCacheV1",
    "SgRecordCacheV1",
]
