"""Typed ripgrep JSON event decoding and normalization helpers."""

from __future__ import annotations

# ruff: noqa: DOC201
from collections.abc import Mapping

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


class RgEvent(msgspec.Struct, frozen=True):
    """Minimal ripgrep JSON event."""

    type: str
    data: object | None = None


_RG_DECODER = msgspec.json.Decoder(type=RgEvent)


def decode_rg_event(line: str | bytes) -> RgEvent | None:
    """Decode a ripgrep JSON event line.

    Returns:
    -------
    RgEvent | None
        Decoded event, or None when decoding fails.
    """
    try:
        if isinstance(line, str):
            line = line.encode("utf-8")
        return _RG_DECODER.decode(line)
    except (msgspec.DecodeError, msgspec.ValidationError):
        return None


def as_match_data(event: RgEvent) -> RgMatchData | None:
    """Coerce an event payload into typed match data."""
    if event.type != "match":
        return None
    if not isinstance(event.data, Mapping):
        return None
    try:
        return msgspec.convert(event.data, type=RgMatchData, strict=False)
    except (msgspec.ValidationError, msgspec.DecodeError, TypeError, ValueError):
        return None


def as_summary_data(event: RgEvent) -> RgSummaryData | None:
    """Coerce an event payload into typed summary data."""
    if event.type != "summary":
        return None
    if not isinstance(event.data, Mapping):
        return None
    try:
        return msgspec.convert(event.data, type=RgSummaryData, strict=False)
    except (msgspec.ValidationError, msgspec.DecodeError, TypeError, ValueError):
        return None


def match_path(data: RgMatchData) -> str | None:
    """Extract path text from typed match data."""
    if data.path is None:
        return None
    return data.path.text or data.path.bytes


def match_line_text(data: RgMatchData) -> str:
    """Extract line text from typed match data."""
    if data.lines is None:
        return ""
    return data.lines.text or data.lines.bytes or ""


def match_line_number(data: RgMatchData) -> int | None:
    """Extract line number from typed match data."""
    return data.line_number


def submatch_text(submatch: RgSubmatch, line_text: str) -> str:
    """Extract a safe submatch text fallback."""
    if submatch.match is not None:
        direct = submatch.match.text or submatch.match.bytes
        if isinstance(direct, str) and direct:
            return direct
    return line_text


def summary_stats(data: RgSummaryData) -> dict[str, object] | None:
    """Convert typed summary stats to dict payload for existing callers."""
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
    "RgEvent",
    "RgMatchData",
    "RgPath",
    "RgSubmatch",
    "RgSummaryData",
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
