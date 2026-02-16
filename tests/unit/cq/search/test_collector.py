"""Tests for smart search collector utilities."""

from __future__ import annotations

from tools.cq.search.pipeline.profiles import SearchLimits
from tools.cq.search.pipeline.smart_search import RawMatch
from tools.cq.search.rg.codec import RgEvent
from tools.cq.search.rg.collector import RgCollector

MATCH_LINE_NUMBER = 5
MATCH_BYTE_END = 3
UNICODE_MATCH_BYTE_START = 5
UNICODE_MATCH_BYTE_END = 11
UNICODE_MATCH_CHAR_START = 5
UNICODE_MATCH_CHAR_END = 10


def test_rgcollector_collects_matches_and_summary() -> None:
    """Test rgcollector collects matches and summary."""
    limits = SearchLimits(max_files=10, max_total_matches=10, max_matches_per_file=5)
    collector = RgCollector(limits=limits, match_factory=RawMatch)

    match_event = RgEvent(
        type="match",
        data={
            "path": {"text": "src/foo.py"},
            "lines": {"text": "foo()"},
            "line_number": 5,
            "submatches": [{"start": 0, "end": 3, "match": {"text": "foo"}}],
        },
    )
    summary_event = RgEvent(
        type="summary",
        data={"stats": {"searches": 1, "searches_with_match": 1, "matches": 1}},
    )

    collector.handle_event(match_event)
    collector.handle_event(summary_event)
    collector.finalize()

    assert len(collector.matches) == 1
    match = collector.matches[0]
    assert match.span.file == "src/foo.py"
    assert match.span.start_line == MATCH_LINE_NUMBER
    assert match.match_byte_start == 0
    assert match.match_byte_end == MATCH_BYTE_END
    assert collector.summary_stats is not None
    assert collector.summary_stats.get("matches") == 1


def test_rgcollector_finalize_when_missing_summary() -> None:
    """Test rgcollector finalize when missing summary."""
    limits = SearchLimits(max_files=10, max_total_matches=10, max_matches_per_file=5)
    collector = RgCollector(limits=limits, match_factory=RawMatch)

    collector.handle_event(
        RgEvent(
            type="match",
            data={
                "path": {"text": "src/foo.py"},
                "lines": {"text": "foo()"},
                "line_number": 1,
                "submatches": [{"start": 0, "end": 3, "match": {"text": "foo"}}],
            },
        )
    )
    collector.finalize()

    assert collector.summary_stats is not None
    assert collector.summary_stats.get("matches") == 1


def test_rgcollector_converts_submatch_byte_offsets_to_char_columns() -> None:
    """Test rgcollector converts submatch byte offsets to char columns."""
    limits = SearchLimits(max_files=10, max_total_matches=10, max_matches_per_file=5)
    collector = RgCollector(limits=limits, match_factory=RawMatch)
    collector.handle_event(
        RgEvent(
            type="match",
            data={
                "path": {"text": "src/unicode.py"},
                "lines": {"text": 'x = "héllo"\n'},
                "line_number": 1,
                # "héllo" starts at byte 5; "é" is multibyte in UTF-8.
                "submatches": [{"start": 5, "end": 11, "match": {"text": "héllo"}}],
            },
        )
    )
    collector.finalize()
    assert len(collector.matches) == 1
    match = collector.matches[0]
    assert match.match_byte_start == UNICODE_MATCH_BYTE_START
    assert match.match_byte_end == UNICODE_MATCH_BYTE_END
    assert match.match_start == UNICODE_MATCH_CHAR_START
    assert match.match_end == UNICODE_MATCH_CHAR_END
