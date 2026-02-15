"""Helpers for reading tree-sitter query `pattern_settings` metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tree_sitter import Node, Query


def pattern_settings(query: Query, pattern_idx: int) -> dict[str, str]:
    """Return normalized string-only metadata for one pattern index."""
    settings = query.pattern_settings(pattern_idx)
    if not isinstance(settings, dict):
        return {}
    out: dict[str, str] = {}
    for key, value in settings.items():
        if not isinstance(key, str):
            continue
        if value is None:
            continue
        out[key] = str(value)
    return out


def first_capture(capture_map: dict[str, list[Node]], capture_name: str) -> Node | None:
    """Return first node for capture name when available."""
    nodes = capture_map.get(capture_name)
    if not isinstance(nodes, list) or not nodes:
        return None
    return nodes[0]


__all__ = [
    "first_capture",
    "pattern_settings",
]
