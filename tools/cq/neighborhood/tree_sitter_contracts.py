"""Contracts for tree-sitter neighborhood collection."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.core.snb_schema import DegradeEventV1, NeighborhoodSliceV1, SemanticNodeRefV1
from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter_structural_contracts import (
    TreeSitterCstTokenV1,
    TreeSitterStructuralExportV1,
)


class TreeSitterNeighborhoodCollectRequest(CqStruct, frozen=True):
    """Typed request for tree-sitter neighborhood assembly."""

    root: str
    target_name: str
    target_file: str
    language: str = "python"
    target_line: int | None = None
    target_col: int | None = None
    max_per_slice: int = 50
    slice_limits: Mapping[str, int] | None = None


class TreeSitterNeighborhoodCollectResult(CqStruct, frozen=True):
    """Collector output with slices and typed diagnostics."""

    subject: SemanticNodeRefV1 | None = None
    slices: tuple[NeighborhoodSliceV1, ...] = ()
    diagnostics: tuple[DegradeEventV1, ...] = ()
    structural_export: TreeSitterStructuralExportV1 | None = None
    cst_tokens: tuple[TreeSitterCstTokenV1, ...] = ()


__all__ = [
    "TreeSitterNeighborhoodCollectRequest",
    "TreeSitterNeighborhoodCollectResult",
]
