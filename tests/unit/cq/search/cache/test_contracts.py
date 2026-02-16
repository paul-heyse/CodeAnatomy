"""Tests for search-scoped cache contracts."""

from __future__ import annotations

import msgspec
from tools.cq.search.cache.contracts import (
    PatternFragmentCacheV1,
    QueryEntityScanCacheV1,
    SearchCandidatesCacheV1,
    SgRecordCacheV1,
)


def test_search_candidates_cache_roundtrip() -> None:
    """Search candidates cache contract round-trips through JSON."""
    payload = SearchCandidatesCacheV1(
        pattern="build_graph",
        raw_matches=[{"file": "src/a.py", "line": 1}],
        stats={"matched_files": 1, "total_matches": 1},
    )

    decoded = msgspec.json.decode(msgspec.json.encode(payload), type=SearchCandidatesCacheV1)
    assert decoded.pattern == "build_graph"
    assert decoded.stats["matched_files"] == 1


def test_query_entity_scan_cache_contract() -> None:
    """Query entity scan contract preserves typed records."""
    row = SgRecordCacheV1(
        record="def",
        kind="function_definition",
        file="src/a.py",
        start_line=1,
        start_col=0,
        end_line=1,
        end_col=10,
        text="def build_graph():",
        rule_id="rule",
    )
    cache = QueryEntityScanCacheV1(records=[row])

    assert cache.records[0].file == "src/a.py"


def test_pattern_fragment_cache_defaults() -> None:
    """Pattern fragment cache defaults are empty collections."""
    payload = PatternFragmentCacheV1()

    assert payload.findings == []
    assert payload.records == []
    assert payload.raw_matches == []
