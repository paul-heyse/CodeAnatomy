"""CST token export helpers for tree-sitter structural outputs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter_structural_contracts import TreeSitterCstTokenV1

if TYPE_CHECKING:
    from tree_sitter import Node


def _token_text(node: Node, source_bytes: bytes) -> str:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    return source_bytes[start:end].decode("utf-8", errors="replace")


def _token_row(*, file_path: str, node: Node, source_bytes: bytes) -> TreeSitterCstTokenV1:
    start_byte = int(getattr(node, "start_byte", 0))
    end_byte = int(getattr(node, "end_byte", start_byte))
    start_point = getattr(node, "start_point", (0, 0))
    end_point = getattr(node, "end_point", start_point)
    token_id = f"{file_path}:{start_byte}:{end_byte}:{node.type}"
    return TreeSitterCstTokenV1(
        token_id=token_id,
        kind=str(getattr(node, "type", "unknown")),
        text=_token_text(node, source_bytes),
        start_byte=start_byte,
        end_byte=end_byte,
        start_line=int(start_point[0]) + 1,
        start_col=int(start_point[1]),
        end_line=int(end_point[0]) + 1,
        end_col=int(end_point[1]),
    )


def export_cst_tokens(
    *,
    file_path: str,
    root: Node,
    source_bytes: bytes,
    max_tokens: int = 20_000,
) -> list[TreeSitterCstTokenV1]:
    """Export leaf CST tokens in stable traversal order.

    Returns:
        list[TreeSitterCstTokenV1]: Extracted leaf token rows.
    """
    out: list[TreeSitterCstTokenV1] = []
    stack: list[Node] = [root]
    while stack and len(out) < max_tokens:
        node = stack.pop()
        children = list(getattr(node, "children", ()))
        if not children:
            out.append(_token_row(file_path=file_path, node=node, source_bytes=source_bytes))
            continue
        stack.extend(reversed(children))
    return out


__all__ = ["export_cst_tokens"]
