"""Shared source span helpers for CQ."""

from __future__ import annotations

from typing import Annotated

import msgspec

from tools.cq.core.structs import CqStruct


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


def span_from_rg_match(
    *,
    file: str,
    line: int,
    start_col: int,
    end_col: int | None,
) -> SourceSpan:
    """Build a SourceSpan from rpygrep match coordinates.

    Returns
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
    "span_from_rg_match",
]
