from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional

from .arrowdsl.runtime import DeterminismTier, RuntimeProfile, ScanProfile


@dataclass(frozen=True)
class CodeIntelCPGConfig:
    """Top-level configuration for building CPG artifacts.

    Keep this intentionally small:
      - runtime policy (threads, determinism, provenance)
      - IO locations
    Data semantics live in Contracts / QuerySpecs, not here.
    """

    repo_root: Path
    out_dir: Path

    # Execution policy
    runtime_profile: str = "DEV_FAST"
    determinism: Optional[DeterminismTier] = None  # override profile if set
    provenance: bool = False

    # Finalize behavior
    mode: str = "tolerant"  # "strict" | "tolerant"

    def resolved_runtime(self) -> RuntimeProfile:
        profile = DEFAULT_RUNTIME_PROFILES.get(self.runtime_profile)
        if profile is None:
            raise KeyError(f"Unknown runtime_profile={self.runtime_profile!r}")
        if self.determinism is None:
            return profile
        return profile.with_determinism(self.determinism)


# --------------------------
# Default policy registry
# --------------------------

DEFAULT_SCAN_PROFILES: Mapping[str, ScanProfile] = {
    # Fast local iteration, reasonable IO parallelism.
    "DEV_FAST": ScanProfile(
        name="DEV_FAST",
        batch_size=64_000,
        batch_readahead=16,
        fragment_readahead=4,
        use_threads=True,
        require_sequenced_output=False,
        implicit_ordering=False,
    ),
    # Debugging determinism: single-thread scanning, sequenced output.
    "DEV_DETERMINISTIC": ScanProfile(
        name="DEV_DETERMINISTIC",
        batch_size=32_000,
        batch_readahead=2,
        fragment_readahead=1,
        use_threads=False,
        require_sequenced_output=True,
        implicit_ordering=True,
    ),
    # CI: conservative, stable, reproducible.
    "CI_STABLE": ScanProfile(
        name="CI_STABLE",
        batch_size=32_000,
        batch_readahead=2,
        fragment_readahead=1,
        use_threads=False,
        require_sequenced_output=True,
        implicit_ordering=True,
    ),
    # Production: throughput.
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
