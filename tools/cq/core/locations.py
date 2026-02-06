"""Shared source span helpers for CQ."""

from __future__ import annotations

from typing import Annotated

import msgspec

from tools.cq.core.structs import CqStruct

_NEWLINE_BYTE = 0x0A


class SourceSpan(CqStruct, frozen=True):
    """Source code span using 1-indexed lines and 0-indexed columns."""

    file: str
    start_line: Annotated[int, msgspec.Meta(ge=1)]
    start_col: Annotated[int, msgspec.Meta(ge=0)]
    end_line: Annotated[int, msgspec.Meta(ge=1)] | None = None
    end_col: Annotated[int, msgspec.Meta(ge=0)] | None = None

    @property
    def line(self) -> int:
        """Backward-compatible alias for start_line."""
        return self.start_line

    @property
    def col(self) -> int:
        """Backward-compatible alias for start_col."""
        return self.start_col


def byte_col_to_char_col(line_text: str, byte_col: int) -> int:
    """Convert a line-relative UTF-8 byte column to a character column.

    Returns:
    -------
    int
        Character column in the decoded line.
    """
    if byte_col <= 0:
        return 0
    line_bytes = line_text.encode("utf-8", errors="replace")
    clamped = min(byte_col, len(line_bytes))
    return len(line_bytes[:clamped].decode("utf-8", errors="replace"))


def byte_offset_to_line_col(source_bytes: bytes, byte_offset: int) -> tuple[int, int]:
    """Convert an absolute byte offset to ``(line, char_col)``.

    Returns:
    -------
    tuple[int, int]
        ``(line, col)`` where line is 1-indexed and col is 0-indexed characters.
    """
    if not source_bytes:
        return 1, 0
    clamped = max(0, min(byte_offset, len(source_bytes)))
    prefix = source_bytes[:clamped]
    line = prefix.count(b"\n") + 1
    line_start = prefix.rfind(b"\n") + 1 if b"\n" in prefix else 0
    col = len(source_bytes[line_start:clamped].decode("utf-8", errors="replace"))
    return line, col


def line_relative_byte_range_to_absolute(
    source_bytes: bytes,
    *,
    line: int,
    byte_start: int,
    byte_end: int,
) -> tuple[int, int] | None:
    """Convert line-relative byte range to absolute file byte offsets.

    Returns:
    -------
    tuple[int, int] | None
        ``(abs_start, abs_end)`` or ``None`` when the line is out of bounds.
    """
    if line < 1 or byte_end <= byte_start:
        return None
    current_line = 1
    line_start = 0
    idx = 0
    size = len(source_bytes)
    while idx < size and current_line < line:
        if source_bytes[idx] == _NEWLINE_BYTE:
            current_line += 1
            line_start = idx + 1
        idx += 1
    if current_line != line:
        return None

    line_end = source_bytes.find(b"\n", line_start)
    if line_end == -1:
        line_end = size
    line_len = max(0, line_end - line_start)
    if line_len == 0:
        return None
    rel_start = max(0, min(byte_start, line_len))
    rel_end = min(max(rel_start + 1, min(byte_end, line_len)), line_len)
    if rel_end <= rel_start:
        return None
    abs_start = line_start + rel_start
    abs_end = line_start + rel_end
    return abs_start, abs_end


def span_from_rg_match(
    *,
    file: str,
    line: int,
    start_col: int,
    end_col: int | None,
) -> SourceSpan:
    """Build a SourceSpan from ripgrep match coordinates.

    Returns:
    -------
    SourceSpan
        Normalized span for the match.
    """
    return SourceSpan(
        file=file,
        start_line=line,
        start_col=start_col,
        end_line=line,
        end_col=end_col,
    )


__all__ = [
    "SourceSpan",
    "byte_col_to_char_col",
    "byte_offset_to_line_col",
    "line_relative_byte_range_to_absolute",
    "span_from_rg_match",
]
