"""Configuration defaults for the CPG pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from arrowdsl.core.context import DeterminismTier, RuntimeProfile, ScanProfile
from ibis_engine.config import IbisBackendConfig


@dataclass(frozen=True)
class CodeIntelCPGConfig:
    """Define top-level configuration for building CPG artifacts."""

    repo_root: Path
    out_dir: Path

    runtime_profile: str = "DEV_FAST"
    determinism: DeterminismTier | None = None
    provenance: bool = False

    mode: Literal["strict", "tolerant"] = "tolerant"

    def resolved_runtime(self) -> RuntimeProfile:
        """Resolve the runtime profile with overrides applied.

        Returns
        -------
        RuntimeProfile
            Resolved runtime profile.

        Raises
        ------
        KeyError
            Raised when the runtime_profile name is unknown.
        """
        profile = DEFAULT_RUNTIME_PROFILES.get(self.runtime_profile)
        if profile is None:
            msg = f"Unknown runtime_profile={self.runtime_profile!r}."
            raise KeyError(msg)
        if self.determinism is None:
            return profile
        return profile.with_determinism(self.determinism)


DEFAULT_SCAN_PROFILES: Mapping[str, ScanProfile] = {
    "DEV_FAST": ScanProfile(
        name="DEV_FAST",
        batch_size=64_000,
        batch_readahead=16,
        fragment_readahead=4,
        use_threads=True,
        require_sequenced_output=False,
        implicit_ordering=False,
    ),
    "DEV_DETERMINISTIC": ScanProfile(
        name="DEV_DETERMINISTIC",
        batch_size=32_000,
        batch_readahead=2,
        fragment_readahead=1,
        use_threads=False,
        require_sequenced_output=True,
        implicit_ordering=True,
    ),
    "CI_STABLE": ScanProfile(
        name="CI_STABLE",
        batch_size=32_000,
        batch_readahead=2,
        fragment_readahead=1,
        use_threads=False,
        require_sequenced_output=True,
        implicit_ordering=True,
    ),
    "PROD_THROUGHPUT": ScanProfile(
        name="PROD_THROUGHPUT",
        batch_size=131_072,
        batch_readahead=32,
        fragment_readahead=8,
        use_threads=True,
        require_sequenced_output=False,
        implicit_ordering=False,
    ),
}

DEFAULT_RUNTIME_PROFILES: Mapping[str, RuntimeProfile] = {
    "DEV_FAST": RuntimeProfile(
        name="DEV_FAST",
        cpu_threads=None,
        io_threads=None,
        scan=DEFAULT_SCAN_PROFILES["DEV_FAST"],
        plan_use_threads=True,
        determinism=DeterminismTier.BEST_EFFORT,
    ),
    "DEV_DETERMINISTIC": RuntimeProfile(
        name="DEV_DETERMINISTIC",
        cpu_threads=1,
        io_threads=4,
        scan=DEFAULT_SCAN_PROFILES["DEV_DETERMINISTIC"],
        plan_use_threads=False,
        determinism=DeterminismTier.CANONICAL,
    ),
    "CI_STABLE": RuntimeProfile(
        name="CI_STABLE",
        cpu_threads=2,
        io_threads=4,
        scan=DEFAULT_SCAN_PROFILES["CI_STABLE"],
        plan_use_threads=False,
        determinism=DeterminismTier.CANONICAL,
    ),
    "PROD_THROUGHPUT": RuntimeProfile(
        name="PROD_THROUGHPUT",
        cpu_threads=None,
        io_threads=None,
        scan=DEFAULT_SCAN_PROFILES["PROD_THROUGHPUT"],
        plan_use_threads=True,
        determinism=DeterminismTier.STABLE_SET,
    ),
}


@dataclass(frozen=True)
class IbisExecutionConfig:
    """Configuration for Ibis-backed execution."""

    backend: IbisBackendConfig = field(default_factory=IbisBackendConfig)
