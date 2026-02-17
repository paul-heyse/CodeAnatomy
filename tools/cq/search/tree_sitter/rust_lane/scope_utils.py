"""Scope helpers for Rust tree-sitter enrichment."""

from __future__ import annotations

from tree_sitter import Node

from tools.cq.search.rust.extractors_shared import RUST_SCOPE_KINDS
from tools.cq.search.tree_sitter.core.infrastructure import child_by_field
from tools.cq.search.tree_sitter.core.node_utils import node_text
from tools.cq.search.tree_sitter.rust_lane.runtime_cache import rust_field_ids

_SCOPE_KINDS: tuple[str, ...] = tuple(sorted(RUST_SCOPE_KINDS - {"block"}))
_MAX_SCOPE_NODES = 256


__all__ = ["_find_scope", "_scope_chain", "_scope_name"]


def _scope_name(scope_node: Node, source_bytes: bytes) -> str | None:
    kind = scope_node.type
    if kind == "impl_item":
        return node_text(child_by_field(scope_node, "type", rust_field_ids()), source_bytes)
    if kind == "macro_invocation":
        return node_text(
            child_by_field(scope_node, "macro", rust_field_ids()), source_bytes
        ) or node_text(
            child_by_field(scope_node, "name", rust_field_ids()),
            source_bytes,
        )
    return node_text(child_by_field(scope_node, "name", rust_field_ids()), source_bytes)


def _scope_chain(node: Node, source_bytes: bytes, *, max_depth: int) -> list[str]:
    chain: list[str] = []
    current: Node | None = node
    depth = 0
    nodes_visited = 0
    while current is not None and depth < max_depth:
        if nodes_visited >= _MAX_SCOPE_NODES:
            break
        if current.type in _SCOPE_KINDS:
            name = _scope_name(current, source_bytes)
            chain.append(f"{current.type}:{name}" if name else current.type)
        current = current.parent
        depth += 1
        nodes_visited += 1
    return chain


def _find_scope(node: Node, *, max_depth: int) -> Node | None:
    current: Node | None = node
    depth = 0
    while current is not None and depth < max_depth:
        if current.type in _SCOPE_KINDS:
            return current
        current = current.parent
        depth += 1
    return None
