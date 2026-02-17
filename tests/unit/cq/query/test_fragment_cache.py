"""Tests for shared query fragment-cache helpers."""

from __future__ import annotations

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.cache.fragment_contracts import FragmentEntryV1, FragmentHitV1
from tools.cq.query.fragment_cache import (
    entity_records_from_hits,
    pattern_data_from_hits,
    raw_match_sort_key,
)
from tools.cq.query.match_contracts import MatchData, MatchRange, MatchRangePoint


def _record(line: int) -> SgRecord:
    return SgRecord(
        record="def",
        kind="function",
        file="a.py",
        start_line=line,
        start_col=0,
        end_line=line + 1,
        end_col=0,
        text=f"def fn_{line}(): pass",
        rule_id="py_def_function",
    )


def _entry() -> FragmentEntryV1:
    return FragmentEntryV1(file="a.py", cache_key="k")


def test_entity_records_from_hits_extracts_only_list_payloads() -> None:
    """Entity hit decoding should ignore non-list payload rows."""
    record = _record(1)
    hits = (
        FragmentHitV1(entry=_entry(), payload=[record]),
        FragmentHitV1(entry=_entry(), payload={"bad": "payload"}),
    )

    data = entity_records_from_hits(hits)

    assert data == {"a.py": [record]}


def test_pattern_data_from_hits_extracts_triplet_payloads() -> None:
    """Pattern hit decoding should extract findings, records, and raw matches."""
    record = _record(1)
    raw = MatchData(
        file="a.py",
        pattern="target($$$)",
        range=MatchRange(
            start=MatchRangePoint(line=0, column=0),
            end=MatchRangePoint(line=0, column=8),
        ),
    )
    hit = FragmentHitV1(entry=_entry(), payload=([], [record], [raw]))

    findings_by_rel, records_by_rel, raw_by_rel = pattern_data_from_hits((hit,))

    assert findings_by_rel == {"a.py": []}
    assert records_by_rel == {"a.py": [record]}
    assert raw_by_rel == {"a.py": [raw]}
    assert raw_match_sort_key(raw) == ("a.py", 0, 0)
