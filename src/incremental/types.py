"""Incremental pipeline configuration types."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from hamilton_pipeline.types.incremental import IncrementalRunConfig


@dataclass(frozen=True)
class IncrementalConfig:
    """Shared incremental pipeline settings."""

    enabled: bool = False
    state_dir: Path | None = None
    repo_id: str | None = None
    impact_strategy: Literal["hybrid", "symbol_closure", "import_closure"] = "hybrid"
    git_base_ref: str | None = None
    git_head_ref: str | None = None
    git_changed_only: bool = False

    def to_run_snapshot(self) -> IncrementalRunConfig:
        """Return a run snapshot for manifests and artifacts.

        Returns
        -------
        IncrementalRunConfig
            Snapshot used by manifests and artifacts.
        """
        from hamilton_pipeline.types.incremental import IncrementalRunConfig

        return IncrementalRunConfig(
            enabled=self.enabled,
            state_dir=str(self.state_dir) if self.state_dir is not None else None,
            repo_id=self.repo_id,
            git_base_ref=self.git_base_ref,
            git_head_ref=self.git_head_ref,
            git_changed_only=self.git_changed_only,
        )


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
]
