"""Line offset helpers for byte/point conversions."""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass

from extract.coordination.context import FileContext, bytes_from_file_ctx


@dataclass(frozen=True)
class LineOffsets:
    """Line start byte offsets for a source buffer."""

    line_start_bytes: tuple[int, ...]
    total_len: int

    @classmethod
    def from_bytes(cls, data: bytes) -> LineOffsets:
        """Build line offsets from raw bytes.

        Returns:
        -------
        LineOffsets
            Line offset mapping for the buffer.
        """
        starts: list[int] = [0]
        for idx, value in enumerate(data):
            if value == NEWLINE_BYTE:
                starts.append(idx + 1)
        return cls(tuple(starts), len(data))

    def point_from_byte(self, byte_offset: int) -> tuple[int, int]:
        """Return (line0, col) for a byte offset.

        Returns:
        -------
        tuple[int, int]
            Line/column pair for the byte offset.
        """
        if byte_offset <= 0:
            return 0, 0
        offset = min(byte_offset, self.total_len)
        line_idx = bisect_right(self.line_start_bytes, offset) - 1
        line_idx = max(line_idx, 0)
        line_start = self.line_start_bytes[line_idx]
        return int(line_idx), int(offset - line_start)

    def byte_offset(self, line0: int | None, col: int | None) -> int | None:
        """Return byte offset for a (line0, col) pair.

        Returns:
        -------
        int | None
            Byte offset when available.
        """
        if line0 is None or col is None:
            return None
        if line0 < 0 or line0 >= len(self.line_start_bytes):
            return None
        return self.line_start_bytes[line0] + col


NEWLINE_BYTE = 0x0A


def line_offsets_from_file_ctx(file_ctx: FileContext) -> LineOffsets | None:
    """Build line offsets from a file context payload.

    Returns:
    -------
    LineOffsets | None
        Line offsets when the file bytes are available.
    """
    data = bytes_from_file_ctx(file_ctx)
    if data is None:
        return None
    return LineOffsets.from_bytes(data)


__all__ = ["LineOffsets", "line_offsets_from_file_ctx"]
