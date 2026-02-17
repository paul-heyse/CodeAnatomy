"""TreeCursor-based structural export for tree-sitter nodes."""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, Any

from tools.cq.search.tree_sitter.contracts.core_models import (
    TreeSitterStructuralEdgeV1,
    TreeSitterStructuralExportV1,
    TreeSitterStructuralNodeV1,
)
from tools.cq.search.tree_sitter.schema.node_schema import load_grammar_schema
from tools.cq.search.tree_sitter.structural.exports import export_cst_tokens
from tools.cq.search.tree_sitter.structural.node_id import build_node_id

if TYPE_CHECKING:
    from tree_sitter import Node


def _node_row(
    *,
    node_id: str,
    node: Node,
    field_name: str | None,
    child_index: int | None,
    is_visible: bool | None,
    is_supertype: bool | None,
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
        is_visible=is_visible,
        is_supertype=is_supertype,
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
    scratch_cursor: Any | None = None,
) -> bool:
    target_byte = int(getattr(cursor.node, "start_byte", 0)) if cursor.node is not None else 0
    if (
        scratch_cursor is not None
        and _reset_cursor_to(scratch_cursor, node=cursor.node)
        and _advance_cursor_to_byte(scratch_cursor, target_byte)
        and _reset_cursor_to(cursor, from_cursor=scratch_cursor)
    ):
        parent_stack.append(node_id)
        child_index_stack.append(0)
        return True
    if _advance_cursor_to_byte(cursor, target_byte):
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


def _advance_cursor_to_byte(cursor: Any, target_byte: int) -> bool:
    goto_for_byte = getattr(cursor, "goto_first_child_for_byte", None)
    if callable(goto_for_byte):
        try:
            child_index = goto_for_byte(target_byte)
        except (TypeError, ValueError, RuntimeError, AttributeError):
            child_index = None
        if child_index is not None:
            return True
    try:
        return bool(cursor.goto_first_child())
    except (TypeError, ValueError, RuntimeError, AttributeError):
        return False


def _fork_cursor(cursor: Any) -> Any | None:
    copy_fn = getattr(cursor, "copy", None)
    if not callable(copy_fn):
        return None
    try:
        return copy_fn()
    except (TypeError, RuntimeError, AttributeError):
        return None


def _native_tree_sitter_cursor(cursor: Any) -> bool:
    cursor_type = type(cursor)
    module_name = str(getattr(cursor_type, "__module__", ""))
    if module_name.startswith("tree_sitter"):
        return True
    return module_name in {"_binding", "tree_sitter._binding"}


def _enable_cursor_reset_path(cursor: Any) -> bool:
    # Native tree-sitter cursor reset/reset_to can segfault intermittently in
    # the Python binding. Keep the optimized reset path for test doubles only.
    return not _native_tree_sitter_cursor(cursor)


def _reset_cursor_to(
    cursor: Any,
    *,
    from_cursor: Any | None = None,
    node: object | None = None,
) -> bool:
    reset_to_fn = getattr(cursor, "reset_to", None)
    if from_cursor is not None and callable(reset_to_fn):
        try:
            reset_to_fn(from_cursor)
        except (TypeError, RuntimeError, AttributeError):
            pass
        else:
            return True
    reset_fn = getattr(cursor, "reset", None)
    if node is not None and callable(reset_fn):
        try:
            reset_fn(node)
        except (TypeError, RuntimeError, AttributeError):
            pass
        else:
            return True
    return False


def _language_name_for_root(root: Node) -> str | None:
    tree = getattr(root, "tree", None)
    language_obj = getattr(tree, "language", None) if tree is not None else None
    language_name = getattr(language_obj, "name", None)
    if isinstance(language_name, str) and language_name:
        return language_name.strip().lower()
    return None


@lru_cache(maxsize=8)
def _classification_map(
    language: str,
) -> dict[str, tuple[bool | None, bool | None]]:
    schema = load_grammar_schema(language)
    if schema is None:
        return {}
    return {
        row.type: (row.is_visible, row.is_supertype)
        for row in schema.node_types
        if row.type and row.type != "__field_registry__"
    }


def _resolve_tokens_source(root: Node, source_bytes: bytes | None) -> bytes:
    if source_bytes is not None:
        return source_bytes
    if not hasattr(root, "text"):
        return b""
    try:
        raw_text = root.text
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return b""
    if isinstance(raw_text, (bytes, bytearray, memoryview)):
        return bytes(raw_text)
    return b""


def _collect_structural_rows(
    *,
    file_path: str,
    root: Node,
    kind_classification: dict[str, tuple[bool | None, bool | None]],
) -> tuple[list[TreeSitterStructuralNodeV1], list[TreeSitterStructuralEdgeV1]]:
    nodes: list[TreeSitterStructuralNodeV1] = []
    edges: list[TreeSitterStructuralEdgeV1] = []
    cursor = root.walk()
    scratch_cursor = _fork_cursor(cursor) if _enable_cursor_reset_path(cursor) else None
    parent_stack: list[str] = []
    child_index_stack: list[int] = [0]
    while True:
        node = cursor.node
        if node is None:
            return nodes, edges
        field_name = cursor.field_name if isinstance(cursor.field_name, str) else None
        node_kind = str(getattr(node, "type", "unknown"))
        is_visible, is_supertype = kind_classification.get(node_kind, (None, None))
        node_id = build_node_id(file_path=file_path, node=node)
        child_index = child_index_stack[-1] if child_index_stack else None
        nodes.append(
            _node_row(
                node_id=node_id,
                node=node,
                field_name=field_name,
                child_index=child_index,
                is_visible=is_visible,
                is_supertype=is_supertype,
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
            scratch_cursor=scratch_cursor,
        ):
            return nodes, edges


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
    tokens_source = _resolve_tokens_source(root, source_bytes)
    tokens = export_cst_tokens(
        file_path=file_path,
        root=root,
        source_bytes=tokens_source,
    )
    language_name = _language_name_for_root(root)
    kind_classification = _classification_map(language_name) if language_name else {}
    nodes, edges = _collect_structural_rows(
        file_path=file_path,
        root=root,
        kind_classification=kind_classification,
    )
    return TreeSitterStructuralExportV1(nodes=nodes, edges=edges, tokens=tokens)


__all__ = ["build_node_id", "export_structural_rows"]
