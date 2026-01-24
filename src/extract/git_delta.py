"""Git diff helpers for changed-file detection."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pygit2

from extract.git_context import open_git_context


@dataclass(frozen=True)
class GitDeltaPaths:
    """Changed and deleted paths between two refs."""

    changed_paths: frozenset[str]
    deleted_paths: frozenset[str]


def diff_paths(
    repo_root: Path,
    *,
    base_ref: str,
    head_ref: str,
) -> GitDeltaPaths | None:
    """Return changed/deleted paths between two refs, or None on failure.

    Returns
    -------
    GitDeltaPaths | None
        Delta path summary when available.
    """
    git_ctx = open_git_context(repo_root)
    if git_ctx is None:
        return None
    repo = git_ctx.repo
    base_commit = _resolve_commit(repo, base_ref)
    head_commit = _resolve_commit(repo, head_ref)
    if base_commit is None or head_commit is None:
        return None
    try:
        diff = repo.diff(base_commit, head_commit)
    except pygit2.GitError:
        return None
    changed: set[str] = set()
    deleted: set[str] = set()
    for delta in diff.deltas:
        status = delta.status
        if status == pygit2.GIT_DELTA_DELETED:
            deleted.add(delta.old_file.path)
            continue
        path = delta.new_file.path or delta.old_file.path
        if path:
            changed.add(path)
    return GitDeltaPaths(
        changed_paths=frozenset(changed),
        deleted_paths=frozenset(deleted),
    )


def _resolve_commit(repo: pygit2.Repository, ref: str) -> pygit2.Commit | None:
    try:
        obj = repo.revparse_single(ref)
        return obj.peel(pygit2.Commit)
    except (KeyError, ValueError, pygit2.GitError):
        return None


__all__ = ["GitDeltaPaths", "diff_paths"]
