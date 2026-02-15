"""Tests for typed ripgrep event decoding and normalization."""

from __future__ import annotations

import msgspec
from tools.cq.search.rg.codec import (
    RgMatchData,
    RgMatchEvent,
    RgSummaryData,
    RgSummaryEvent,
    as_match_data,
    as_summary_data,
    decode_rg_event,
    match_line_number,
    match_line_text,
    match_path,
    summary_stats,
)


def test_decode_rg_event_match_line() -> None:
    payload = msgspec.json.encode(
        {
            "type": "match",
            "data": {
                "path": {"text": "src/foo.py"},
                "line_number": 5,
                "lines": {"text": "build_graph()"},
                "submatches": [{"start": 0, "end": 11, "match": {"text": "build_graph"}}],
            },
        }
    )
    event = decode_rg_event(payload)
    assert event is not None
    assert isinstance(event, RgMatchEvent)
    match_data = as_match_data(event)
    assert isinstance(match_data, RgMatchData)
    assert match_path(match_data) == "src/foo.py"
    assert match_line_number(match_data) == 5
    assert match_line_text(match_data) == "build_graph()"
    assert match_data.submatches
    assert match_data.submatches[0].start == 0


def test_decode_rg_event_summary_line() -> None:
    payload = msgspec.json.encode(
        {
            "type": "summary",
            "data": {"stats": {"searches": 4, "searches_with_match": 2, "matches": 3}},
        }
    )
    event = decode_rg_event(payload)
    assert isinstance(event, RgSummaryEvent)
    summary = as_summary_data(event)
    assert isinstance(summary, RgSummaryData)
    stats = summary_stats(summary)
    assert stats == {"searches": 4, "searches_with_match": 2, "matches": 3}


def test_decode_rg_event_invalid_line_returns_none() -> None:
    assert decode_rg_event(b"{invalid json") is None
