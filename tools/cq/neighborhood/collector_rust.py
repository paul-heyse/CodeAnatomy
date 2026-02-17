"""Rust-specific neighborhood collector."""

from __future__ import annotations

from pathlib import Path

from tools.cq.neighborhood.collector_shared import (
    anchor_byte_to_line_col,
    collect_language_neighborhood,
)
from tools.cq.neighborhood.contracts import TreeSitterNeighborhoodCollectResult

__all__ = ["collect_rust_neighborhood"]


def collect_rust_neighborhood(
    file_path: str,
    *,
    source_bytes: bytes,
    anchor_byte: int,
    root: Path,
    target_name: str,
    top_k: int = 10,
) -> TreeSitterNeighborhoodCollectResult:
    """Collect Rust neighborhood context for one anchor byte.

    Returns:
        TreeSitterNeighborhoodCollectResult: Neighborhood collection output.
    """
    line, col = anchor_byte_to_line_col(source_bytes, anchor_byte)
    return collect_language_neighborhood(
        root=root,
        target_name=target_name,
        target_file=file_path,
        language="rust",
        target_line=line,
        target_col=col,
        top_k=top_k,
    )
