"""Configuration defaults for the CPG pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from arrowdsl.core.context import DeterminismTier, RuntimeProfile, runtime_profile_factory
from datafusion_engine.runtime import AdapterExecutionPolicy
from ibis_engine.config import IbisBackendConfig


@dataclass(frozen=True)
class AdapterMode:
    """Adapter selection flags for plan execution."""

    use_ibis_bridge: bool = True
    use_datafusion_bridge: bool = False


@dataclass(frozen=True)
class CodeIntelCPGConfig:
    """Define top-level configuration for building CPG artifacts."""

    repo_root: Path
    out_dir: Path

    runtime_profile: str = "default"
    determinism: DeterminismTier | None = None
    provenance: bool = False

    mode: Literal["strict", "tolerant"] = "tolerant"
    adapter_mode: AdapterMode = field(default_factory=AdapterMode)
    execution_policy: AdapterExecutionPolicy = field(default_factory=AdapterExecutionPolicy)

    def resolved_runtime(self) -> RuntimeProfile:
        """Resolve the runtime profile with overrides applied.

        Returns
        -------
        RuntimeProfile
            Resolved runtime profile.

        """
        profile = runtime_profile_factory(self.runtime_profile)
        if self.determinism is None:
            return profile
        return profile.with_determinism(self.determinism)


@dataclass(frozen=True)
class IbisExecutionConfig:
    """Configuration for Ibis-backed execution."""

    backend: IbisBackendConfig = field(default_factory=IbisBackendConfig)


ARROWDSL_PLAN_LANE_DECOMMISSION_CHECKLIST: tuple[str, ...] = (
    "All plan builders have Ibis bridge coverage.",
    "Ibis bridge enabled across pipeline entry points.",
    "DataFusion registry bridge validated for dataset formats.",
    "Kernel fallbacks verified against Arrow outputs.",
    "Legacy ArrowDSL plan helpers removed or deprecated.",
)
