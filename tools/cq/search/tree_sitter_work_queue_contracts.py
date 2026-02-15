"""Contracts for changed-range work queue items."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class TreeSitterWorkItemV1(CqStruct, frozen=True):
    """Queued changed-range window work item."""

    language: str
    file_key: str
    start_byte: int
    end_byte: int


__all__ = ["TreeSitterWorkItemV1"]
