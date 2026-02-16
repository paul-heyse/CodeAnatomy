"""Tests for ripgrep collector wrappers."""

from __future__ import annotations

from tools.cq.search.pipeline.profiles import SearchLimits
from tools.cq.search.pipeline.smart_search import RawMatch
from tools.cq.search.rg.codec import RgEvent
from tools.cq.search.rg.collector import collect_events


def test_collect_events_builds_collector_state() -> None:
    events = [
        RgEvent(type="begin", data={"path": {"text": "src/foo.py"}}),
        RgEvent(
            type="match",
            data={
                "path": {"text": "src/foo.py"},
                "lines": {"text": "foo()"},
                "line_number": 2,
                "absolute_offset": 100,
                "submatches": [{"start": 0, "end": 3, "match": {"text": "foo"}}],
            },
        ),
        RgEvent(
            type="context",
            data={
                "path": {"text": "src/foo.py"},
                "lines": {"text": "def foo():"},
                "line_number": 1,
            },
        ),
        RgEvent(type="end", data={"path": {"text": "src/foo.py"}, "binary_offset": 0}),
        RgEvent(
            type="summary",
            data={
                "stats": {
                    "searches": 1,
                    "searches_with_match": 1,
                    "matches": 1,
                    "matched_lines": 1,
                    "bytes_searched": 200,
                    "bytes_printed": 32,
                }
            },
        ),
    ]
    collector = collect_events(
        events=events,
        limits=SearchLimits(max_files=10, max_total_matches=10),
        match_factory=RawMatch,
    )
    assert len(collector.matches) == 1
    assert collector.summary_stats is not None
    assert collector.files_started == {"src/foo.py"}
    assert collector.files_completed == {"src/foo.py"}
    assert collector.binary_files == {"src/foo.py"}
    assert collector.context_lines["src/foo.py"][1] == "def foo():"
    assert collector.matches[0].match_abs_byte_start == 100
    assert collector.matches[0].match_abs_byte_end == 103
