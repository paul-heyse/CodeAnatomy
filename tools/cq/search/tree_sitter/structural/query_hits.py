"""Typed query-hit export helpers for tree-sitter matches."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from tools.cq.search.tree_sitter.contracts.core_models import TreeSitterQueryHitV1
from tools.cq.search.tree_sitter.structural.export import build_node_id


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
    """Export typed query hit rows from ``matches()`` output."""
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
                        node_id=build_node_id(file_path=file_path, node=node),
                        start_byte=int(getattr(node, "start_byte", 0)),
                        end_byte=int(getattr(node, "end_byte", 0)),
                    )
                )
    return tuple(rows)


__all__ = ["export_query_hits"]
