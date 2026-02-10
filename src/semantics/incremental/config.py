"""Incremental configuration for the semantic pipeline.

This module provides configuration for incremental processing in the semantic
CPG pipeline. It integrates with the existing CDF infrastructure and provides
semantic-specific defaults for incremental relationship updates.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from semantics.incremental.cdf_joins import CDFMergeStrategy
from semantics.incremental.cdf_types import CdfFilterPolicy


@dataclass(frozen=True)
class IncrementalRunSnapshot:
    """Incremental run configuration snapshot for manifests.

    This is a standalone type used for serializing incremental configuration
    to manifests and build artifacts. It contains only the essential fields
    needed for reproducibility tracking.
    """

    enabled: bool
    state_dir: str | None
    repo_id: str | None
    git_base_ref: str | None = None
    git_head_ref: str | None = None
    git_changed_only: bool = False


@dataclass(frozen=True)
class SemanticIncrementalConfig:
    """Configuration for incremental semantic processing.

    This configuration controls CDF-based incremental relationship recomputation
    in the semantic pipeline. It provides settings for cursor state management,
    change filtering, and merge strategies.

    Attributes:
    ----------
    enabled
        Whether incremental processing is enabled. When False, full refresh
        is performed for all relationship computations.
    state_dir
        Directory for storing incremental state (cursors, checkpoints).
        Required when ``enabled`` is True.
    cdf_filter_policy
        Policy for filtering CDF change types. Defaults to including inserts
        and updates but excluding deletes.
    default_merge_strategy
        Default strategy for merging incremental results with existing data.
        Defaults to UPSERT for typical relationship updates.
    impact_strategy
        Strategy for computing impacted files in incremental runs.
    repo_id
        Optional repository identifier for multi-repo deployments.
    git_base_ref
        Git reference for the base commit (e.g., ``main`` or a commit SHA).
        Used for git-based change detection.
    git_head_ref
        Git reference for the head commit. Used with ``git_base_ref`` to
        compute changed files.
    git_changed_only
        When True, only process files changed between git refs.
        Requires ``git_base_ref`` and ``git_head_ref`` to be set.

    Examples:
    --------
    Create a basic incremental config:

    >>> config = SemanticIncrementalConfig(
    ...     enabled=True,
    ...     state_dir=Path("/tmp/incremental_state"),
    ... )

    Create a CDF-enabled config using the factory method:

    >>> config = SemanticIncrementalConfig.with_cdf_enabled(
    ...     state_dir=Path("/tmp/incremental_state"),
    ... )

    Access the cursor store path:

    >>> config.cursor_store_path
    PosixPath('/tmp/incremental_state/cursors')
    """

    enabled: bool = False
    state_dir: Path | None = None
    cdf_filter_policy: CdfFilterPolicy = field(
        default_factory=CdfFilterPolicy.inserts_and_updates_only
    )
    default_merge_strategy: CDFMergeStrategy = CDFMergeStrategy.UPSERT
    impact_strategy: Literal["hybrid", "symbol_closure", "import_closure"] = "hybrid"
    repo_id: str | None = None
    git_base_ref: str | None = None
    git_head_ref: str | None = None
    git_changed_only: bool = False

    @property
    def cursor_store_path(self) -> Path | None:
        """Return the path to the cursor store directory.

        The cursor store holds version cursors for incremental CDF reads,
        allowing the pipeline to track which changes have been processed.

        Returns:
        -------
        Path | None
            Path to the cursors directory if ``state_dir`` is set, else None.
        """
        if self.state_dir is None:
            return None
        return self.state_dir / "cursors"

    @classmethod
    def with_cdf_enabled(
        cls,
        state_dir: Path,
        *,
        cdf_filter_policy: CdfFilterPolicy | None = None,
        default_merge_strategy: CDFMergeStrategy = CDFMergeStrategy.UPSERT,
        impact_strategy: Literal["hybrid", "symbol_closure", "import_closure"] = "hybrid",
    ) -> SemanticIncrementalConfig:
        """Create an incremental config with CDF processing enabled.

        Factory method for creating a properly configured incremental config
        with CDF support. This ensures that ``enabled`` is True and that a
        valid ``state_dir`` is provided.

        For git-based change detection, construct the config directly with
        ``git_base_ref``, ``git_head_ref``, and ``git_changed_only`` fields.

        Parameters
        ----------
        state_dir
            Directory for storing incremental state. Required.
        cdf_filter_policy
            Policy for filtering CDF change types. Defaults to including
            inserts and updates but excluding deletes.
        default_merge_strategy
            Default strategy for merging incremental results.
            Defaults to UPSERT.
        impact_strategy
            Strategy for computing incremental impact scope.

        Returns:
        -------
        SemanticIncrementalConfig
            Configured incremental config with CDF enabled.

        Examples:
        --------
        >>> config = SemanticIncrementalConfig.with_cdf_enabled(
        ...     state_dir=Path("/tmp/state"),
        ...     cdf_filter_policy=CdfFilterPolicy.include_all(),
        ... )
        >>> config.enabled
        True
        >>> config.cursor_store_path
        PosixPath('/tmp/state/cursors')
        """
        effective_policy = (
            cdf_filter_policy
            if cdf_filter_policy is not None
            else CdfFilterPolicy.inserts_and_updates_only()
        )
        return cls(
            enabled=True,
            state_dir=state_dir,
            cdf_filter_policy=effective_policy,
            default_merge_strategy=default_merge_strategy,
            impact_strategy=impact_strategy,
        )

    def to_run_snapshot(self) -> IncrementalRunSnapshot:
        """Return a run snapshot for manifests and artifacts.

        Convert the incremental config to a serializable run configuration
        snapshot that can be stored in manifests and build artifacts for
        reproducibility tracking.

        Returns:
        -------
        IncrementalRunSnapshot
            Snapshot suitable for manifest storage.
        """
        return IncrementalRunSnapshot(
            enabled=self.enabled,
            state_dir=str(self.state_dir) if self.state_dir is not None else None,
            repo_id=self.repo_id,
            git_base_ref=self.git_base_ref,
            git_head_ref=self.git_head_ref,
            git_changed_only=self.git_changed_only,
        )


__all__ = ["IncrementalRunSnapshot", "SemanticIncrementalConfig"]
