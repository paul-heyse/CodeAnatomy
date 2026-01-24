"""Git-backed blob access helpers for repo blob extraction."""

from __future__ import annotations

from pathlib import Path

import pygit2

from extract.git_context import open_git_context


def open_repo_for_path(path: Path) -> pygit2.Repository | None:
    """Return a pygit2 repository for the given path when available.

    Returns
    -------
    pygit2.Repository | None
        Repository when available.
    """
    git_ctx = open_git_context(path)
    if git_ctx is None:
        return None
    return git_ctx.repo


def read_blob_at_ref(
    repo: pygit2.Repository,
    *,
    ref: str,
    path_posix: str,
) -> bytes | None:
    """Read blob bytes at a ref and path, returning None when missing.

    Returns
    -------
    bytes | None
        Blob bytes when available.
    """
    try:
        obj = repo.revparse_single(ref)
        commit = obj.peel(pygit2.Commit)
    except (KeyError, ValueError, pygit2.GitError):
        return None
    try:
        entry = commit.tree[path_posix]
    except KeyError:
        return None
    try:
        blob = repo[entry.id]
    except (KeyError, ValueError, pygit2.GitError):
        return None
    if not isinstance(blob, pygit2.Blob):
        return None
    return blob.data


__all__ = ["open_repo_for_path", "read_blob_at_ref"]
