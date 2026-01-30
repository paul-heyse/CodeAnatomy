"""Incremental pipeline snapshot types for the Hamilton pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class IncrementalRunConfig:
    """Incremental run configuration snapshot for manifests."""

    enabled: bool
    state_dir: str | None
    repo_id: str | None
    git_base_ref: str | None = None
    git_head_ref: str | None = None
    git_changed_only: bool = False


@dataclass(frozen=True)
class IncrementalDatasetUpdates:
    """Bundle incremental dataset update paths."""

    extract_updates: Mapping[str, str] | None
    normalize_updates: Mapping[str, str] | None
    module_index_updates: Mapping[str, str] | None
    imports_resolved_updates: Mapping[str, str] | None
    exported_defs_updates: Mapping[str, str] | None


@dataclass(frozen=True)
class IncrementalImpactUpdates:
    """Bundle incremental impact update paths."""

    impacted_callers_updates: Mapping[str, str] | None
    impacted_importers_updates: Mapping[str, str] | None
    impacted_files_updates: Mapping[str, str] | None


__all__ = [
    "IncrementalDatasetUpdates",
    "IncrementalImpactUpdates",
    "IncrementalRunConfig",
]
