"""pygit2-backed repository file listing."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pygit2

from extract.git_context import open_git_context
from extract.pathspec_filters import build_repo_scan_pathspec, should_include_repo_path


def iter_repo_files_pygit2(
    repo_root: Path,
    *,
    include_globs: Sequence[str],
    exclude_globs: Sequence[str],
    exclude_dirs: Sequence[str],
    follow_symlinks: bool,
) -> list[Path] | None:
    """Return pygit2-listed repo files, or None if git is unavailable.

    Returns
    -------
    list[pathlib.Path] | None
        Repo-relative paths when git is available.
    """
    git_ctx = open_git_context(repo_root)
    if git_ctx is None or git_ctx.repo.workdir is None:
        return None
    repo = git_ctx.repo
    repo_root = git_ctx.repo_root.resolve()
    filters = build_repo_scan_pathspec(
        repo_root,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        exclude_dirs=exclude_dirs,
    )
    try:
        status = repo.status()
    except pygit2.GitError:
        return None
    paths: set[str] = set(status)
    paths.update(entry.path for entry in repo.index)
    resolved = repo_root.resolve()
    results: list[Path] = []
    for item in paths:
        rel = Path(item)
        if rel.is_absolute():
            continue
        if not should_include_repo_path(rel, filters=filters, allow_ignored=True):
            continue
        abs_path = resolved / rel
        if abs_path.is_dir():
            continue
        if not follow_symlinks and abs_path.is_symlink():
            continue
        results.append(rel)
    return results


__all__ = ["iter_repo_files_pygit2"]
