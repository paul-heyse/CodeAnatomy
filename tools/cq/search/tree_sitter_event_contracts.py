"""msgspec contracts for tree-sitter event rows."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct


class TreeSitterEventV1(CqStruct, frozen=True):
    """One normalized tree-sitter event row."""

    file: str
    language: str
    kind: str
    start_byte: int
    end_byte: int
    start_line: int | None = None
    end_line: int | None = None
    captures: dict[str, str] = msgspec.field(default_factory=dict)


class TreeSitterEventBatchV1(CqStruct, frozen=True):
    """Batch of tree-sitter events for one file/language lane."""

    file: str
    language: str
    events: list[TreeSitterEventV1] = msgspec.field(default_factory=list)


__all__ = [
    "TreeSitterEventBatchV1",
    "TreeSitterEventV1",
]
