"""Shared neighborhood collection helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.neighborhood.contracts import (
    TreeSitterNeighborhoodCollectRequest,
    TreeSitterNeighborhoodCollectResult,
)
from tools.cq.neighborhood.tree_sitter_collector import collect_tree_sitter_neighborhood

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
    *,
    root: Path,
    target_name: str,
    target_file: str,
    language: str,
    target_line: int,
    target_col: int,
    top_k: int,
) -> TreeSitterNeighborhoodCollectResult:
    """Run neighborhood collection for one language target.

    Returns:
        TreeSitterNeighborhoodCollectResult: Collected neighborhood payload.
    """
    return collect_tree_sitter_neighborhood(
        TreeSitterNeighborhoodCollectRequest(
            root=str(root),
            target_name=target_name,
            target_file=target_file,
            language=language,
            target_line=target_line,
            target_col=target_col,
            max_per_slice=top_k,
        )
    )
