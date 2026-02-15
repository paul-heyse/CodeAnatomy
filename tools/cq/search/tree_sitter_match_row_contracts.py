"""Contracts for metadata-driven tree-sitter match evidence rows."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct


class ObjectEvidenceRowV1(CqStruct, frozen=True):
    """One metadata-backed object evidence row derived from query matches."""

    emit: str = "unknown"
    kind: str = "unknown"
    anchor_start_byte: int = 0
    anchor_end_byte: int = 0
    pattern_index: int = 0
    captures: dict[str, str] = msgspec.field(default_factory=dict)


__all__ = ["ObjectEvidenceRowV1"]
