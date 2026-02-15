"""Contracts for tree-sitter diagnostics rows."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct


class TreeSitterDiagnosticV1(CqStruct, frozen=True):
    """A tree-sitter syntax diagnostic row."""

    kind: str
    start_byte: int
    end_byte: int
    start_line: int
    start_col: int
    end_line: int
    end_col: int
    message: str
    metadata: dict[str, object] = msgspec.field(default_factory=dict)


__all__ = ["TreeSitterDiagnosticV1"]
