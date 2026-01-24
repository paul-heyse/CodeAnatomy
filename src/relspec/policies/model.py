"""Policy model definitions for inference-driven pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

from arrowdsl.core.plan_ops import SortKey


@dataclass(frozen=True)
class WinnerSelectConfig:
    """Policy for selecting a winner among ambiguous results."""

    keys: tuple[str, ...] = ()
    score_col: str = "score"
    score_order: Literal["ascending", "descending"] = "descending"
    tie_breakers: tuple[SortKey, ...] = ()


@dataclass(frozen=True)
class ConfidencePolicy:
    """Policy for computing confidence scores."""

    base: float = 0.5
    source_weight: Mapping[str, float] = field(default_factory=dict)
    penalty: float = 0.0


@dataclass(frozen=True)
class AmbiguityPolicy:
    """Policy for ambiguity resolution."""

    winner_select: WinnerSelectConfig | None = None
    tie_breakers: tuple[SortKey, ...] = ()


__all__ = ["AmbiguityPolicy", "ConfidencePolicy", "WinnerSelectConfig"]
