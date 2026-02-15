"""Contracts for deterministic tree-sitter structural exports."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct


class TreeSitterStructuralNodeV1(CqStruct, frozen=True):
    """Tree-sitter structural node record."""

    node_id: str
    kind: str
    start_byte: int
    end_byte: int
    start_line: int
    start_col: int
    end_line: int
    end_col: int
    field_name: str | None = None
    child_index: int | None = None


class TreeSitterStructuralEdgeV1(CqStruct, frozen=True):
    """Tree-sitter structural edge record."""

    edge_id: str
    source_node_id: str
    target_node_id: str
    kind: str = "parent_child"
    field_name: str | None = None


class TreeSitterStructuralExportV1(CqStruct, frozen=True):
    """Container for deterministic structural export payload."""

    nodes: list[TreeSitterStructuralNodeV1] = msgspec.field(default_factory=list)
    edges: list[TreeSitterStructuralEdgeV1] = msgspec.field(default_factory=list)


__all__ = [
    "TreeSitterStructuralEdgeV1",
    "TreeSitterStructuralExportV1",
    "TreeSitterStructuralNodeV1",
]
