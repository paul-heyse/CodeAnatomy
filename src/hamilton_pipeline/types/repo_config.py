"""Repository and extraction configuration types for the Hamilton pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ScipIndexSettings:
    """Settings for scip-python indexing."""

    enabled: bool = True
    index_path_override: str | None = None
    output_dir: str = "build/scip"
    env_json_path: str | None = None
    generate_env_json: bool = False
    scip_python_bin: str = "scip-python"
    scip_cli_bin: str = "scip"
    target_only: str | None = None
    node_max_old_space_mb: int | None = 8192
    timeout_s: int | None = None
    extra_args: tuple[str, ...] = ()
    use_incremental_shards: bool = False
    shards_dir: str | None = None
    shards_manifest_path: str | None = None
    run_scip_print: bool = False
    scip_print_path: str | None = None
    run_scip_snapshot: bool = False
    scip_snapshot_dir: str | None = None
    scip_snapshot_comment_syntax: str = "#"
    run_scip_test: bool = False
    scip_test_args: tuple[str, ...] = ("--check-documents",)


ScipIndexConfig = ScipIndexSettings


@dataclass(frozen=True)
class RepoScopeConfig:
    """Configuration for repository scope rules."""

    include_untracked: bool = True
    python_extensions: tuple[str, ...] = ()
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    include_submodules: bool = False
    include_worktrees: bool = False
    follow_symlinks: bool = False
    external_interface_depth: Literal["metadata", "full"] = "metadata"


@dataclass(frozen=True)
class RepoScanConfig:
    """Configuration for repository scanning."""

    repo_root: str
    scope_config: RepoScopeConfig
    max_files: int
    diff_base_ref: str | None = None
    diff_head_ref: str | None = None
    changed_only: bool = False
    record_pathspec_trace: bool = False
    pathspec_trace_limit: int | None = 200
    pathspec_trace_pattern_limit: int | None = 50


@dataclass(frozen=True)
class RelspecConfig:
    """Configuration for relationship-spec execution."""

    relspec_mode: Literal["memory", "filesystem"]
    scip_index_path: str | None


@dataclass(frozen=True)
class ScipIdentityOverrides:
    """Optional overrides for SCIP project identity."""

    project_name_override: str | None
    project_version_override: str | None
    project_namespace_override: str | None


@dataclass(frozen=True)
class ScipIndexInputs:
    """Bundle inputs for SCIP indexing and identity resolution."""

    repo_root: str
    scip_identity_overrides: ScipIdentityOverrides
    scip_index_config: ScipIndexConfig


@dataclass(frozen=True)
class TreeSitterConfig:
    """Configuration for tree-sitter extraction."""

    enable_tree_sitter: bool


__all__ = [
    "RelspecConfig",
    "RepoScanConfig",
    "RepoScopeConfig",
    "ScipIdentityOverrides",
    "ScipIndexConfig",
    "ScipIndexInputs",
    "ScipIndexSettings",
    "TreeSitterConfig",
]
