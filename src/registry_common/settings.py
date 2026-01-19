"""Shared settings models for registry configuration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass(frozen=True)
class ScipIndexSettings:
    """Settings for scip-python indexing."""

    enabled: bool = True
    index_path_override: str | None = None
    output_dir: str = "build/scip"
    env_json_path: str | None = None
    scip_python_bin: str = "scip-python"
    target_only: str | None = None
    node_max_old_space_mb: int | None = 8192
    timeout_s: int | None = None


@dataclass(frozen=True)
class IncrementalSettings:
    """Shared incremental pipeline settings."""

    enabled: bool = False
    state_dir: Path | None = None
    repo_id: str | None = None
    impact_strategy: Literal["hybrid", "symbol_closure", "import_closure"] = "hybrid"


__all__ = ["IncrementalSettings", "ScipIndexSettings"]
