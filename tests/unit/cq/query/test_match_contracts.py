"""Tests for typed ast-grep match contracts."""

from __future__ import annotations

import msgspec
from tools.cq.query.match_contracts import MatchData, MatchRange, MatchRangePoint

EXPECTED_START_LINE = 10
EXPECTED_END_COLUMN = 15


def test_match_data_roundtrip_json() -> None:
    """MatchData JSON round-trip preserves required fields."""
    payload = MatchData(
        file="src/example.py",
        pattern="pattern_0",
        range=MatchRange(
            start=MatchRangePoint(line=10, column=4),
            end=MatchRangePoint(line=10, column=15),
        ),
        message="Pattern match",
        text="target_name(value)",
        metavars={"$X": "value"},
    )

    encoded = msgspec.json.encode(payload)
    decoded = msgspec.json.decode(encoded, type=MatchData)

    assert decoded.file == "src/example.py"
    assert decoded.pattern == "pattern_0"
    assert decoded.range.start.line == EXPECTED_START_LINE
    assert decoded.range.end.column == EXPECTED_END_COLUMN
    assert decoded.metavars["$X"] == "value"


def test_match_data_defaults() -> None:
    """MatchData applies default values for optional fields."""
    payload = MatchData(
        file="src/example.py",
        pattern="pattern_1",
        range=MatchRange(
            start=MatchRangePoint(line=1, column=0),
            end=MatchRangePoint(line=1, column=3),
        ),
    )

    assert payload.message == "Pattern match"
    assert not payload.text
    assert payload.metavars == {}
