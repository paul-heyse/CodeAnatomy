"""Semantic neighborhood collection and assembly."""

from __future__ import annotations

from tools.cq.neighborhood.contracts import (
    TreeSitterNeighborhoodCollectRequest,
    TreeSitterNeighborhoodCollectResult,
)
from tools.cq.neighborhood.section_layout import is_section_collapsed
from tools.cq.neighborhood.tree_sitter_collector import (
    collect_tree_sitter_neighborhood,
)

__all__ = [
    "TreeSitterNeighborhoodCollectRequest",
    "TreeSitterNeighborhoodCollectResult",
    "collect_tree_sitter_neighborhood",
    "is_section_collapsed",
]
