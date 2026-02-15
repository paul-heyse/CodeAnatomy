"""Tests for ripgrep collector wrappers."""

from __future__ import annotations

from tools.cq.search.pipeline.profiles import SearchLimits
from tools.cq.search.pipeline.smart_search import RawMatch
from tools.cq.search.rg.codec import RgEvent
from tools.cq.search.rg.collector import collect_events


def test_collect_events_builds_collector_state() -> None:
    events = [
        RgEvent(
            type="match",
            data={
                "path": {"text": "src/foo.py"},
                "lines": {"text": "foo()"},
                "line_number": 2,
                "submatches": [{"start": 0, "end": 3, "match": {"text": "foo"}}],
            },
        ),
        RgEvent(type="summary", data={"stats": {"searches": 1, "searches_with_match": 1}}),
    ]
    collector = collect_events(
        events=events,
        limits=SearchLimits(max_files=10, max_total_matches=10),
        match_factory=RawMatch,
    )
    assert len(collector.matches) == 1
    assert collector.summary_stats is not None
