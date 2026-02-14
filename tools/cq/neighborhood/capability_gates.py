"""Static capability-gated neighborhood slice planning helpers."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.core.snb_schema import DegradeEventV1, NeighborhoodSliceKind


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
    "normalize_capability_snapshot",
    "plan_feasible_slices",
]
