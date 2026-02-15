"""Contracts for coordinate-rich object occurrence grounding."""

from __future__ import annotations

from tools.cq.core.structs import CqOutputStruct


class OccurrenceGroundingV1(CqOutputStruct, frozen=True):
    """Precise occurrence location plus containing block boundaries."""

    file_path: str
    line: int
    col: int
    block_start_line: int
    block_end_line: int
    byte_start: int | None = None
    byte_end: int | None = None


__all__ = ["OccurrenceGroundingV1"]
