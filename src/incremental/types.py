"""Incremental pipeline configuration types."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class IncrementalConfig:
    """Configuration flags for incremental pipeline runs."""

    enabled: bool = False
    state_dir: Path | None = None
    repo_id: str | None = None


@dataclass(frozen=True)
class IncrementalFileChanges:
    """File change sets derived from snapshot diffs."""

    changed_file_ids: tuple[str, ...] = ()
    deleted_file_ids: tuple[str, ...] = ()
    full_refresh: bool = False


__all__ = ["IncrementalConfig", "IncrementalFileChanges"]
