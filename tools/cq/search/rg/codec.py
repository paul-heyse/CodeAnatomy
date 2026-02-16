"""Typed ripgrep JSON event decoding and normalization helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

import msgspec

from tools.cq.core.typed_boundary import BoundaryDecodeError, convert_lax


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
    absolute_offset: int | None = None
    submatches: list[RgSubmatch] = msgspec.field(default_factory=list)


class RgContextData(msgspec.Struct, omit_defaults=True):
    """Typed ripgrep context data payload."""

    path: RgPath | None = None
    lines: RgText | None = None
    line_number: int | None = None
    submatches: list[RgSubmatch] = msgspec.field(default_factory=list)


class RgSummaryStats(msgspec.Struct, omit_defaults=True):
    """Typed summary stats payload."""

    searches: int | None = None
    searches_with_match: int | None = None
    matches: int | None = None
    matched_lines: int | None = None
    bytes_searched: int | None = None
    bytes_printed: int | None = None


class RgSummaryData(msgspec.Struct, omit_defaults=True):
    """Typed ripgrep summary data payload."""

    stats: RgSummaryStats | None = None


class RgBeginData(msgspec.Struct, omit_defaults=True):
    """Typed ripgrep begin event payload."""

    path: RgPath | None = None


class RgEndData(msgspec.Struct, omit_defaults=True):
    """Typed ripgrep end event payload."""

    path: RgPath | None = None
    binary_offset: int | None = None
    stats: RgSummaryStats | None = None


class RgMatchEvent(msgspec.Struct, frozen=True, tag_field="type", tag="match"):
    """Typed ripgrep match event."""

    data: RgMatchData

    @property
    def type(self) -> Literal["match"]:
        """Return tagged event type for compatibility with generic event paths."""
        return "match"


class RgContextEvent(msgspec.Struct, frozen=True, tag_field="type", tag="context"):
    """Typed ripgrep context event."""

    data: RgContextData

    @property
    def type(self) -> Literal["context"]:
        """Return tagged event type for compatibility with generic event paths."""
        return "context"


class RgSummaryEvent(msgspec.Struct, frozen=True, tag_field="type", tag="summary"):
    """Typed ripgrep summary event."""

    data: RgSummaryData

    @property
    def type(self) -> Literal["summary"]:
        """Return tagged event type for compatibility with generic event paths."""
        return "summary"


class RgBeginEvent(msgspec.Struct, frozen=True, tag_field="type", tag="begin"):
    """Typed ripgrep begin event."""

    data: RgBeginData

    @property
    def type(self) -> Literal["begin"]:
        """Return tagged event type for compatibility with generic event paths."""
        return "begin"


class RgEndEvent(msgspec.Struct, frozen=True, tag_field="type", tag="end"):
    """Typed ripgrep end event."""

    data: RgEndData

    @property
    def type(self) -> Literal["end"]:
        """Return tagged event type for compatibility with generic event paths."""
        return "end"


class RgEvent(msgspec.Struct, frozen=True):
    """Minimal ripgrep JSON event."""

    type: str
    data: object | None = None


type RgTypedEvent = RgMatchEvent | RgSummaryEvent | RgContextEvent | RgBeginEvent | RgEndEvent
type RgAnyEvent = (
    RgMatchEvent | RgSummaryEvent | RgContextEvent | RgBeginEvent | RgEndEvent | RgEvent
)

_RG_TYPED_DECODER = msgspec.json.Decoder(type=RgTypedEvent)
_RG_FALLBACK_DECODER = msgspec.json.Decoder(type=RgEvent)
_RG_MAPPING_DECODER = msgspec.json.Decoder(type=dict[str, object])


def _as_bytes(line: str | bytes) -> bytes:
    return line.encode("utf-8") if isinstance(line, str) else line


def decode_rg_event(line: str | bytes) -> RgAnyEvent | None:
    """Decode one ripgrep JSON event line with typed-first fallback."""
    raw = _as_bytes(line)
    try:
        return _RG_TYPED_DECODER.decode(raw)
    except (msgspec.DecodeError, msgspec.ValidationError):
        try:
            return _RG_FALLBACK_DECODER.decode(raw)
        except (msgspec.DecodeError, msgspec.ValidationError):
            return None


def decode_event(line: str | bytes) -> dict[str, object] | None:
    """Decode one JSON line into a mapping payload."""
    raw = _as_bytes(line)
    try:
        return _RG_MAPPING_DECODER.decode(raw)
    except (msgspec.DecodeError, msgspec.ValidationError):
        return None


def decode_event_strict(line: str | bytes) -> dict[str, object]:
    """Decode one JSON line into a mapping payload, raising on failure."""
    return _RG_MAPPING_DECODER.decode(_as_bytes(line))


def decode_rg_event_mapping(line: str | bytes) -> dict[str, object] | None:
    """Compatibility alias for mapping decode."""
    return decode_event(line)


def _coerce_event_data(event: RgAnyEvent, event_type: str, type_: object) -> object | None:
    if event.type != event_type:
        return None
    if not isinstance(event.data, Mapping):
        return None
    try:
        return convert_lax(event.data, type_=type_)
    except BoundaryDecodeError:
        return None


def as_match_data(event: RgAnyEvent) -> RgMatchData | None:
    """Coerce event payload into typed match data."""
    if isinstance(event, RgMatchEvent):
        return event.data
    value = _coerce_event_data(event, "match", RgMatchData)
    return value if isinstance(value, RgMatchData) else None


def as_context_data(event: RgAnyEvent) -> RgContextData | None:
    """Coerce event payload into typed context data."""
    if isinstance(event, RgContextEvent):
        return event.data
    value = _coerce_event_data(event, "context", RgContextData)
    return value if isinstance(value, RgContextData) else None


def as_summary_data(event: RgAnyEvent) -> RgSummaryData | None:
    """Coerce event payload into typed summary data."""
    if isinstance(event, RgSummaryEvent):
        return event.data
    value = _coerce_event_data(event, "summary", RgSummaryData)
    return value if isinstance(value, RgSummaryData) else None


def as_begin_data(event: RgAnyEvent) -> RgBeginData | None:
    """Coerce event payload into typed begin data."""
    if isinstance(event, RgBeginEvent):
        return event.data
    value = _coerce_event_data(event, "begin", RgBeginData)
    return value if isinstance(value, RgBeginData) else None


def as_end_data(event: RgAnyEvent) -> RgEndData | None:
    """Coerce event payload into typed end data."""
    if isinstance(event, RgEndEvent):
        return event.data
    value = _coerce_event_data(event, "end", RgEndData)
    return value if isinstance(value, RgEndData) else None


def match_path(data: RgMatchData | RgContextData | RgBeginData | RgEndData) -> str | None:
    """Extract path text from typed event data."""
    path = getattr(data, "path", None)
    if path is None:
        return None
    return path.text or path.bytes


def match_line_text(data: RgMatchData | RgContextData) -> str:
    """Extract line text from typed match/context data."""
    lines = getattr(data, "lines", None)
    if lines is None:
        return ""
    return lines.text or lines.bytes or ""


def match_line_number(data: RgMatchData | RgContextData) -> int | None:
    """Extract 1-based line number from typed match/context data."""
    return data.line_number


def submatch_text(submatch: RgSubmatch, line_text: str) -> str:
    """Extract safe submatch text fallback."""
    if submatch.match is not None:
        direct = submatch.match.text or submatch.match.bytes
        if isinstance(direct, str) and direct:
            return direct
    return line_text


def summary_stats(data: RgSummaryData) -> dict[str, object] | None:
    """Convert typed summary stats to mapping payload."""
    if data.stats is None:
        return None
    return {
        "searches": data.stats.searches if isinstance(data.stats.searches, int) else 0,
        "searches_with_match": (
            data.stats.searches_with_match if isinstance(data.stats.searches_with_match, int) else 0
        ),
        "matches": data.stats.matches if isinstance(data.stats.matches, int) else 0,
        "matched_lines": data.stats.matched_lines
        if isinstance(data.stats.matched_lines, int)
        else 0,
        "bytes_searched": data.stats.bytes_searched
        if isinstance(data.stats.bytes_searched, int)
        else 0,
        "bytes_printed": data.stats.bytes_printed
        if isinstance(data.stats.bytes_printed, int)
        else 0,
    }


__all__ = [
    "RgAnyEvent",
    "RgBeginData",
    "RgBeginEvent",
    "RgContextData",
    "RgContextEvent",
    "RgEndData",
    "RgEndEvent",
    "RgEvent",
    "RgMatchData",
    "RgMatchEvent",
    "RgPath",
    "RgSubmatch",
    "RgSummaryData",
    "RgSummaryEvent",
    "RgSummaryStats",
    "RgText",
    "as_begin_data",
    "as_context_data",
    "as_end_data",
    "as_match_data",
    "as_summary_data",
    "decode_event",
    "decode_event_strict",
    "decode_rg_event",
    "decode_rg_event_mapping",
    "match_line_number",
    "match_line_text",
    "match_path",
    "submatch_text",
    "summary_stats",
]
