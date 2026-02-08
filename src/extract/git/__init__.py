"""Git repository integration for extract layer.

This subpackage provides git-aware utilities for:
- Repository context discovery and navigation
- Blame/authorship tracking via mailmap
- Diff/delta detection between refs
- Blob access at specific refs
- Submodule handling
- Remote URL parsing
"""

from __future__ import annotations

from extract.git.blobs import (
    RepoBlobOptions,
    default_repo_blob_options,
    open_repo_for_path,
    read_blob_at_ref,
    repo_file_blobs_query,
    scan_repo_blobs,
    scan_repo_blobs_plan,
)
from extract.git.context import (
    GitContext,
    default_project_name,
    discover_repo_root,
    discover_repo_root_from_paths,
    github_name_with_owner,
    open_git_context,
)
from extract.git.history import (
    BlameHunk,
    GitDeltaPaths,
    blame_hunks,
    diff_paths,
    resolve_mailmap,
)
from extract.git.pygit2_scan import (
    iter_repo_candidate_paths,
    iter_repo_files_pygit2,
    repo_status_paths,
)
from extract.git.remotes import (
    RemoteAuthCallbacks,
    RemoteAuthSpec,
    RemoteFeatureSet,
    fetch_remote,
    remote_auth_from_env,
    remote_callbacks_from_env,
    remote_features,
)
from extract.git.settings import (
    GitSettingsSpec,
    apply_git_settings,
    apply_git_settings_once,
    git_settings_from_env,
)
from extract.git.submodules import (
    SubmoduleRoot,
    SubmoduleUpdateResult,
    WorktreeRoot,
    submodule_roots,
    update_submodules,
    worktree_roots,
)

__all__ = [
    "BlameHunk",
    "GitContext",
    "GitDeltaPaths",
    "GitSettingsSpec",
    "RemoteAuthCallbacks",
    "RemoteAuthSpec",
    "RemoteFeatureSet",
    "RepoBlobOptions",
    "SubmoduleRoot",
    "SubmoduleUpdateResult",
    "WorktreeRoot",
    "apply_git_settings",
    "apply_git_settings_once",
    "blame_hunks",
    "default_project_name",
    "default_repo_blob_options",
    "diff_paths",
    "discover_repo_root",
    "discover_repo_root_from_paths",
    "fetch_remote",
    "git_settings_from_env",
    "github_name_with_owner",
    "iter_repo_candidate_paths",
    "iter_repo_files_pygit2",
    "open_git_context",
    "open_repo_for_path",
    "read_blob_at_ref",
    "remote_auth_from_env",
    "remote_callbacks_from_env",
    "remote_features",
    "repo_file_blobs_query",
    "repo_status_paths",
    "resolve_mailmap",
    "scan_repo_blobs",
    "scan_repo_blobs_plan",
    "submodule_roots",
    "update_submodules",
    "worktree_roots",
]
