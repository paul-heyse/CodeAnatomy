"""Incremental pipeline configuration types."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass(frozen=True)
class IncrementalSettings:
    """Shared incremental pipeline settings."""

    enabled: bool = False
    state_dir: Path | None = None
    repo_id: str | None = None
    impact_strategy: Literal["hybrid", "symbol_closure", "import_closure"] = "hybrid"


IncrementalConfig = IncrementalSettings


@dataclass(frozen=True)
class IncrementalImpact:
    """Impact scope derived from incremental diffs and closures."""

    changed_file_ids: tuple[str, ...] = ()
    deleted_file_ids: tuple[str, ...] = ()
    impacted_file_ids: tuple[str, ...] = ()
    full_refresh: bool = False


@dataclass(frozen=True)
class IncrementalFileChanges:
    """File change sets derived from Delta CDF updates."""

    changed_file_ids: tuple[str, ...] = ()
    deleted_file_ids: tuple[str, ...] = ()
    full_refresh: bool = False


__all__ = [
    "IncrementalConfig",
    "IncrementalFileChanges",
    "IncrementalImpact",
    "IncrementalSettings",
]
