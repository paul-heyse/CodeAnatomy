"""Execution surface policies for plan materialization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from arrowdsl.core.context import DeterminismTier

WriterStrategy = Literal["arrow", "datafusion"]


@dataclass(frozen=True)
class ExecutionSurfacePolicy:
    """Policy describing how plan outputs should be materialized."""

    prefer_streaming: bool = True
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT
    writer_strategy: WriterStrategy = "arrow"


__all__ = ["ExecutionSurfacePolicy", "WriterStrategy"]
