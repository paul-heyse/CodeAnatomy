"""Backward-compatible re-exports for pygit2 repo scanning helpers."""

from __future__ import annotations

from extract.git.pygit2_scan import (
    iter_repo_candidate_paths,
    iter_repo_files_pygit2,
    repo_status_paths,
)

__all__ = [
    "iter_repo_candidate_paths",
    "iter_repo_files_pygit2",
    "repo_status_paths",
]
