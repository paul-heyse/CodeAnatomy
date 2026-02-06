"""Materialization policies for view output handling."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from core.config_base import FingerprintableConfig, config_fingerprint
from core_types import DeterminismTier

WriterStrategy = Literal["arrow", "datafusion"]


@dataclass(frozen=True)
class MaterializationPolicy(FingerprintableConfig):
    """Policy controlling how view outputs are materialized.

    Controls streaming preferences, determinism tier, and writer strategy for
    output handling.
    """

    prefer_streaming: bool = True
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT
    writer_strategy: WriterStrategy = "arrow"

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the materialization policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing materialization policy settings.
        """
        return {
            "prefer_streaming": self.prefer_streaming,
            "determinism_tier": self.determinism_tier.value,
            "writer_strategy": self.writer_strategy,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the materialization policy.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


__all__ = ["MaterializationPolicy", "WriterStrategy"]
