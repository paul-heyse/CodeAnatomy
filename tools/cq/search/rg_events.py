"""Typed ripgrep JSON event decoding and normalization helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

import msgspec


class RgText(msgspec.Struct, omit_defaults=True):
    """Ripgrep text payload."""

    text: str | None = None
    bytes: str | None = None


class RgPath(msgspec.Struct, omit_defaults=True):
    """Ripgrep path payload."""

    text: str | None = None
    bytes: str | None = None


class RgSubmatch(msgspec.Struct, omit_defaults=True):
    """Ripgrep submatch payload."""

    start: int
    end: int
    match: RgText | None = None


class RgMatchData(msgspec.Struct, omit_defaults=True):
    """Typed ripgrep match data payload."""

    path: RgPath | None = None
    lines: RgText | None = None
    line_number: int | None = None
    submatches: list[RgSubmatch] = msgspec.field(default_factory=list)


class RgSummaryStats(msgspec.Struct, omit_defaults=True):
    """Typed summary stats payload."""

    searches: int | None = None
    searches_with_match: int | None = None
    matches: int | None = None


class RgSummaryData(msgspec.Struct, omit_defaults=True):
    """Typed ripgrep summary data payload."""

    stats: RgSummaryStats | None = None


class RgMatchEvent(msgspec.Struct, frozen=True, tag_field="type", tag="match"):
    """Typed ripgrep match event."""

    data: RgMatchData

    @property
    def type(self) -> Literal["match"]:
        """Return tagged event type for compatibility with generic event paths."""
        return "match"


class RgSummaryEvent(msgspec.Struct, frozen=True, tag_field="type", tag="summary"):
    """Typed ripgrep summary event."""

    data: RgSummaryData

    @property
    def type(self) -> Literal["summary"]:
        """Return tagged event type for compatibility with generic event paths."""
        return "summary"


class RgEvent(msgspec.Struct, frozen=True):
    """Minimal ripgrep JSON event."""

    type: str
    data: object | None = None


type RgTypedEvent = RgMatchEvent | RgSummaryEvent
type RgAnyEvent = RgMatchEvent | RgSummaryEvent | RgEvent

_RG_TYPED_DECODER = msgspec.json.Decoder(type=RgTypedEvent)
_RG_FALLBACK_DECODER = msgspec.json.Decoder(type=RgEvent)


def decode_rg_event(line: str | bytes) -> RgAnyEvent | None:
    """Decode a ripgrep JSON event line.

    Returns:
    -------
    RgEvent | None
        Decoded event, or None when decoding fails.
    """
    try:
        if isinstance(line, str):
            line = line.encode("utf-8")
        return _RG_TYPED_DECODER.decode(line)
    except (msgspec.DecodeError, msgspec.ValidationError):
        try:
            return _RG_FALLBACK_DECODER.decode(line)
        except (msgspec.DecodeError, msgspec.ValidationError):
            return None


def as_match_data(event: RgAnyEvent) -> RgMatchData | None:
    """Coerce an event payload into typed match data.

    Returns:
    -------
    RgMatchData | None
        Typed match payload when event is a decodable match record.
    """
    if isinstance(event, RgMatchEvent):
        return event.data
    if event.type != "match":
        return None
    if not isinstance(event.data, Mapping):
        return None
    try:
        return msgspec.convert(event.data, type=RgMatchData, strict=False)
    except (msgspec.ValidationError, msgspec.DecodeError, TypeError, ValueError):
        return None


def as_summary_data(event: RgAnyEvent) -> RgSummaryData | None:
    """Coerce an event payload into typed summary data.

    Returns:
    -------
    RgSummaryData | None
        Typed summary payload when event is a decodable summary record.
    """
    if isinstance(event, RgSummaryEvent):
        return event.data
    if event.type != "summary":
        return None
    if not isinstance(event.data, Mapping):
        return None
    try:
        return msgspec.convert(event.data, type=RgSummaryData, strict=False)
    except (msgspec.ValidationError, msgspec.DecodeError, TypeError, ValueError):
        return None


def match_path(data: RgMatchData) -> str | None:
    """Extract path text from typed match data.

    Returns:
    -------
    str | None
        Match file path text when present.
    """
    if data.path is None:
        return None
    return data.path.text or data.path.bytes


def match_line_text(data: RgMatchData) -> str:
    """Extract line text from typed match data.

    Returns:
    -------
    str
        Source line text associated with the match.
    """
    if data.lines is None:
        return ""
    return data.lines.text or data.lines.bytes or ""


def match_line_number(data: RgMatchData) -> int | None:
    """Extract line number from typed match data.

    Returns:
    -------
    int | None
        1-based line number when available.
    """
    return data.line_number


def submatch_text(submatch: RgSubmatch, line_text: str) -> str:
    """Extract a safe submatch text fallback.

    Returns:
    -------
    str
        Direct submatch text, or the line fallback.
    """
    if submatch.match is not None:
        direct = submatch.match.text or submatch.match.bytes
        if isinstance(direct, str) and direct:
            return direct
    return line_text


def summary_stats(data: RgSummaryData) -> dict[str, object] | None:
    """Convert typed summary stats to dict payload for existing callers.

    Returns:
    -------
    dict[str, object] | None
        Existing summary stats payload shape, or None when unavailable.
    """
    if data.stats is None:
        return None
    return {
        "searches": data.stats.searches if isinstance(data.stats.searches, int) else 0,
        "searches_with_match": (
            data.stats.searches_with_match if isinstance(data.stats.searches_with_match, int) else 0
        ),
        "matches": data.stats.matches if isinstance(data.stats.matches, int) else 0,
    }


__all__ = [
    "RgAnyEvent",
    "RgEvent",
    "RgMatchData",
    "RgMatchEvent",
    "RgPath",
    "RgSubmatch",
    "RgSummaryData",
    "RgSummaryEvent",
    "RgSummaryStats",
    "RgText",
    "as_match_data",
    "as_summary_data",
    "decode_rg_event",
    "match_line_number",
    "match_line_text",
    "match_path",
    "submatch_text",
    "summary_stats",
]
