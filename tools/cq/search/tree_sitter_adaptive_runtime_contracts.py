"""Contracts for adaptive tree-sitter runtime control-plane state."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class AdaptiveRuntimeSnapshotV1(CqStruct, frozen=True):
    """Adaptive runtime snapshot for one language lane."""

    language: str
    average_latency_ms: float = 0.0
    sample_count: int = 0
    recommended_budget_ms: int = 0


__all__ = ["AdaptiveRuntimeSnapshotV1"]
