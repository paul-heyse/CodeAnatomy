"""Python-specific neighborhood collector."""

from __future__ import annotations

from pathlib import Path

from tools.cq.neighborhood.collector_shared import (
    anchor_byte_to_line_col,
    collect_language_neighborhood,
)
from tools.cq.neighborhood.contracts import (
    TreeSitterNeighborhoodCollectRequest,
    TreeSitterNeighborhoodCollectResult,
)

__all__ = ["collect_python_neighborhood"]


def collect_python_neighborhood(
    file_path: str,
    *,
    source_bytes: bytes,
    anchor_byte: int,
    root: Path,
    target_name: str,
    top_k: int = 10,
) -> TreeSitterNeighborhoodCollectResult:
    """Collect Python neighborhood context for one anchor byte.

    Returns:
        TreeSitterNeighborhoodCollectResult: Neighborhood collection output.
    """
    line, col = anchor_byte_to_line_col(source_bytes, anchor_byte)
    return collect_language_neighborhood(
        TreeSitterNeighborhoodCollectRequest(
            root=str(root),
            target_name=target_name,
            target_file=file_path,
            language="python",
            target_line=line,
            target_col=col,
            max_per_slice=top_k,
        )
    )
