"""Shared neighborhood collection helpers."""

from __future__ import annotations

from tools.cq.neighborhood.collector import collect_tree_sitter_neighborhood
from tools.cq.neighborhood.contracts import (
    TreeSitterNeighborhoodCollectRequest,
    TreeSitterNeighborhoodCollectResult,
)

__all__ = ["anchor_byte_to_line_col", "collect_language_neighborhood"]


def anchor_byte_to_line_col(source_bytes: bytes, anchor_byte: int) -> tuple[int, int]:
    """Translate byte offset to 1-based line and 0-based column.

    Returns:
        tuple[int, int]: `(line, col)` for the requested byte offset.
    """
    prefix = source_bytes[: max(0, anchor_byte)]
    line = prefix.count(b"\n") + 1
    col = len(prefix.splitlines()[-1]) if prefix.splitlines() else 0
    return line, col


def collect_language_neighborhood(
    request: TreeSitterNeighborhoodCollectRequest,
) -> TreeSitterNeighborhoodCollectResult:
    """Run neighborhood collection for one language target.

    Returns:
        TreeSitterNeighborhoodCollectResult: Collected neighborhood payload.
    """
    return collect_tree_sitter_neighborhood(request)
