"""Execution surface policies for plan materialization."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from core.config_base import FingerprintableConfig, config_fingerprint
from core_types import DeterminismTier

WriterStrategy = Literal["arrow", "datafusion"]


@dataclass(frozen=True)
class ExecutionSurfacePolicy(FingerprintableConfig):
    """Policy describing how plan outputs should be materialized."""

    prefer_streaming: bool = True
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT
    writer_strategy: WriterStrategy = "arrow"

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the execution surface policy.

        Returns
        -------
        Mapping[str, object]
            Payload describing execution surface policy settings.
        """
        return {
            "prefer_streaming": self.prefer_streaming,
            "determinism_tier": self.determinism_tier.value,
            "writer_strategy": self.writer_strategy,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the execution surface policy.

        Returns
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


__all__ = ["ExecutionSurfacePolicy", "WriterStrategy"]
