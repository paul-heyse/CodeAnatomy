"""Typed match payload contracts for ast-grep query execution."""

from __future__ import annotations

import msgspec


class MatchRangePoint(msgspec.Struct, frozen=True):
    """One 0-indexed line/column point."""

    line: int
    column: int


class MatchRange(msgspec.Struct, frozen=True):
    """Inclusive-exclusive match range."""

    start: MatchRangePoint
    end: MatchRangePoint


class MatchData(msgspec.Struct, frozen=True):
    """Canonical typed ast-grep match payload."""

    file: str
    pattern: str
    range: MatchRange
    message: str = "Pattern match"
    text: str = ""
    metavars: dict[str, object] = msgspec.field(default_factory=dict)


__all__ = [
    "MatchData",
    "MatchRange",
    "MatchRangePoint",
]
