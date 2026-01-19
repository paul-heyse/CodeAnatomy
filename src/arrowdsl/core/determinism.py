"""Determinism tiers for execution policies."""

from __future__ import annotations

from enum import StrEnum


class DeterminismTier(StrEnum):
    """Determinism budgets for the pipeline."""

    CANONICAL = "canonical"
    STABLE_SET = "stable_set"
    BEST_EFFORT = "best_effort"
    FAST = "best_effort"
    STABLE = "stable_set"


__all__ = ["DeterminismTier"]
