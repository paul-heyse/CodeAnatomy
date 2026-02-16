"""Merged structural exports: diagnostics, tokens, and query-hit helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Protocol

from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryWindowV1,
    TreeSitterCstTokenV1,
    TreeSitterDiagnosticV1,
    TreeSitterQueryHitV1,
)

if TYPE_CHECKING:
    from tree_sitter import Node


# -- Shared Utilities ---------------------------------------------------------


def _build_node_id(*, file_path: str, node: object | None) -> str:
    """Build a stable structural node identifier from file and byte span.

    Returns:
        str: Function return value.
    """
    if node is None:
        return f"{file_path}:::null"
    node_type = str(getattr(node, "type", "unknown"))
    return (
        f"{file_path}:{int(getattr(node, 'start_byte', 0))}:"
        f"{int(getattr(node, 'end_byte', 0))}:{node_type}"
    )


# -- Diagnostic Export --------------------------------------------------------


def export_diagnostic_rows(
    rows: Iterable[object],
) -> tuple[TreeSitterDiagnosticV1, ...]:
    """Normalize diagnostic iterable into a typed tuple payload.

    Returns:
        tuple[TreeSitterDiagnosticV1, ...]: Function return value.
    """
    return tuple(row for row in rows if isinstance(row, TreeSitterDiagnosticV1))


def collect_diagnostic_rows(
    *,
    language: str,
    root: Node,
    windows: tuple[QueryWindowV1, ...],
    match_limit: int,
) -> tuple[TreeSitterDiagnosticV1, ...]:
    """Collect diagnostics and return typed tuple rows.

    Returns:
        tuple[TreeSitterDiagnosticV1, ...]: Function return value.
    """
    from tools.cq.search.tree_sitter.diagnostics import collect_tree_sitter_diagnostics

    rows = collect_tree_sitter_diagnostics(
        language=language,
        root=root,
        windows=windows,
        match_limit=match_limit,
    )
    return export_diagnostic_rows(rows)


# -- Token Export -------------------------------------------------------------


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


# -- Query Hit Export ---------------------------------------------------------


class NodeLike(Protocol):
    """Structural node protocol for query-hit projection."""

    @property
    def type(self) -> str: ...

    @property
    def start_byte(self) -> int: ...

    @property
    def end_byte(self) -> int: ...


def export_query_hits(
    *,
    file_path: str,
    matches: Sequence[tuple[int, Mapping[str, Sequence[NodeLike]]]],
) -> tuple[TreeSitterQueryHitV1, ...]:
    """Export typed query hit rows from ``matches()`` output.

    Returns:
        tuple[TreeSitterQueryHitV1, ...]: Function return value.
    """
    rows: list[TreeSitterQueryHitV1] = []
    for pattern_index, capture_map in matches:
        if not isinstance(pattern_index, int):
            continue
        for capture_name, nodes in capture_map.items():
            if not isinstance(capture_name, str):
                continue
            for node in nodes:
                if not hasattr(node, "start_byte") or not hasattr(node, "end_byte"):
                    continue
                rows.append(
                    TreeSitterQueryHitV1(
                        query_name=file_path,
                        pattern_index=pattern_index,
                        capture_name=capture_name,
                        node_id=_build_node_id(file_path=file_path, node=node),
                        start_byte=int(getattr(node, "start_byte", 0)),
                        end_byte=int(getattr(node, "end_byte", 0)),
                    )
                )
    return tuple(rows)


__all__ = [
    "collect_diagnostic_rows",
    "export_cst_tokens",
    "export_diagnostic_rows",
    "export_query_hits",
]
