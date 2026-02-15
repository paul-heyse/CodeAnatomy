"""TreeCursor-based structural export for tree-sitter nodes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from tools.cq.search.tree_sitter_structural_contracts import (
    TreeSitterStructuralEdgeV1,
    TreeSitterStructuralExportV1,
    TreeSitterStructuralNodeV1,
)
from tools.cq.search.tree_sitter_token_export import export_cst_tokens

if TYPE_CHECKING:
    from tree_sitter import Node


def _node_id(*, file_path: str, node: Node | None) -> str:
    if node is None:
        return f"{file_path}:::null"
    return (
        f"{file_path}:{int(getattr(node, 'start_byte', 0))}:"
        f"{int(getattr(node, 'end_byte', 0))}:{node.type}"
    )


def _node_row(
    *,
    node_id: str,
    node: Node,
    field_name: str | None,
    child_index: int | None,
) -> TreeSitterStructuralNodeV1:
    start_point = getattr(node, "start_point", (0, 0))
    end_point = getattr(node, "end_point", start_point)
    return TreeSitterStructuralNodeV1(
        node_id=node_id,
        kind=node.type,
        start_byte=int(getattr(node, "start_byte", 0)),
        end_byte=int(getattr(node, "end_byte", 0)),
        start_line=int(start_point[0]) + 1,
        start_col=int(start_point[1]),
        end_line=int(end_point[0]) + 1,
        end_col=int(end_point[1]),
        field_name=field_name,
        child_index=child_index,
    )


def _edge_row(
    *,
    parent_id: str,
    node_id: str,
    field_name: str | None,
) -> TreeSitterStructuralEdgeV1:
    edge_id = f"{parent_id}->{node_id}:contains:{field_name or '_'}"
    return TreeSitterStructuralEdgeV1(
        edge_id=edge_id,
        source_node_id=parent_id,
        target_node_id=node_id,
        kind="contains",
        field_name=field_name,
    )


def _advance_cursor(
    *,
    cursor: Any,
    node_id: str,
    parent_stack: list[str],
    child_index_stack: list[int],
) -> bool:
    if cursor.goto_first_child():
        parent_stack.append(node_id)
        child_index_stack.append(0)
        return True
    while True:
        if cursor.goto_next_sibling():
            if child_index_stack:
                child_index_stack[-1] += 1
            return True
        if not cursor.goto_parent():
            return False
        if parent_stack:
            parent_stack.pop()
        if child_index_stack:
            child_index_stack.pop()


def export_structural_rows(
    *,
    file_path: str,
    root: Node,
    source_bytes: bytes | None = None,
) -> TreeSitterStructuralExportV1:
    """Export deterministic node/edge rows with field labels.

    Returns:
        TreeSitterStructuralExportV1: A fully populated structural export payload.
    """
    nodes: list[TreeSitterStructuralNodeV1] = []
    edges: list[TreeSitterStructuralEdgeV1] = []
    tokens_source = source_bytes
    if tokens_source is None and hasattr(root, "text"):
        try:
            raw_text = root.text
            if isinstance(raw_text, (bytes, bytearray, memoryview)):
                tokens_source = bytes(raw_text)
            else:
                tokens_source = None
        except (RuntimeError, TypeError, ValueError, AttributeError):
            tokens_source = None
    if tokens_source is None:
        tokens_source = b""
    tokens = export_cst_tokens(
        file_path=file_path,
        root=root,
        source_bytes=tokens_source,
    )
    cursor = root.walk()
    parent_stack: list[str] = []
    child_index_stack: list[int] = [0]

    while True:
        node = cursor.node
        if node is None:
            return TreeSitterStructuralExportV1(nodes=nodes, edges=edges, tokens=tokens)
        field_name = cursor.field_name if isinstance(cursor.field_name, str) else None
        node_id = _node_id(file_path=file_path, node=node)
        child_index = child_index_stack[-1] if child_index_stack else None
        nodes.append(
            _node_row(
                node_id=node_id,
                node=node,
                field_name=field_name,
                child_index=child_index,
            )
        )
        if parent_stack:
            edges.append(
                _edge_row(parent_id=parent_stack[-1], node_id=node_id, field_name=field_name)
            )

        if not _advance_cursor(
            cursor=cursor,
            node_id=node_id,
            parent_stack=parent_stack,
            child_index_stack=child_index_stack,
        ):
            return TreeSitterStructuralExportV1(nodes=nodes, edges=edges, tokens=tokens)


__all__ = ["export_structural_rows"]
