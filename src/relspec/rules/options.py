"""Execution-time option helpers for centralized rules."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class RuleExecutionOptions:
    """Execution options used for stage gating."""

    module_allowlist: tuple[str, ...] = ()
    feature_flags: Mapping[str, bool] = field(default_factory=dict)
    metadata_defaults: Mapping[str, object] = field(default_factory=dict)

    def as_mapping(self) -> Mapping[str, object]:
        """Return a merged mapping for stage gating.

        Returns
        -------
        Mapping[str, object]
            Combined options used for rule stage gating.
        """
        return {
            "module_allowlist": self.module_allowlist,
            **self.metadata_defaults,
            **self.feature_flags,
        }


__all__ = ["RuleExecutionOptions"]
