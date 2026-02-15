"""Contracts for tree-sitter neighborhood collection and capability gating."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.core.snb_schema import (
    DegradeEventV1,
    NeighborhoodSliceKind,
    NeighborhoodSliceV1,
    SemanticNodeRefV1,
)
from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.contracts.core_models import (
    TreeSitterCstTokenV1,
    TreeSitterDiagnosticV1,
    TreeSitterQueryHitV1,
    TreeSitterStructuralExportV1,
)

# ── Tree-Sitter Neighborhood Collection Contracts ──


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
    cst_diagnostics: tuple[TreeSitterDiagnosticV1, ...] = ()
    cst_query_hits: tuple[TreeSitterQueryHitV1, ...] = ()


# ── Capability-Gated Slice Planning ──


def normalize_capability_snapshot(
    capabilities: Mapping[str, object] | None,
) -> dict[str, bool]:
    """Normalize raw capability map into a bool capability snapshot.

    Returns:
        dict[str, bool]: Mapping of string capability keys to boolean availability flags.
    """
    if not isinstance(capabilities, Mapping):
        return {}
    return {str(key): bool(value) for key, value in capabilities.items()}


def plan_feasible_slices(
    requested_slices: tuple[NeighborhoodSliceKind, ...],
    capabilities: Mapping[str, object] | None,
    *,
    stage: str = "semantic.planning",
) -> tuple[tuple[NeighborhoodSliceKind, ...], tuple[DegradeEventV1, ...]]:
    """Plan capability-feasible static slices.

    Returns:
        tuple[tuple[NeighborhoodSliceKind, ...], tuple[DegradeEventV1, ...]]:
            Feasible slice kinds and produced degrade events.
    """
    snapshot = normalize_capability_snapshot(capabilities)
    feasible: list[NeighborhoodSliceKind] = []
    degrades: list[DegradeEventV1] = []
    for kind in requested_slices:
        if snapshot.get(kind, False):
            feasible.append(kind)
            continue
        degrades.append(
            DegradeEventV1(
                stage=stage,
                severity="info",
                category="unavailable",
                message=f"Slice '{kind}' unavailable for static capability snapshot",
            )
        )
    return tuple(feasible), tuple(degrades)


__all__ = [
    "TreeSitterNeighborhoodCollectRequest",
    "TreeSitterNeighborhoodCollectResult",
    "normalize_capability_snapshot",
    "plan_feasible_slices",
]
