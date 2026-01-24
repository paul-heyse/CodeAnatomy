"""Filesystem-backed repository file listing."""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

from extract.pathspec_filters import (
    RepoScanPathspec,
    build_repo_scan_pathspec,
    should_include_repo_path,
)


@dataclass(frozen=True)
class _RepoScanFilters:
    pathspec: RepoScanPathspec
    follow_symlinks: bool


def _eligible_repo_file(
    abs_path: Path,
    *,
    rel_path: Path,
    filters: _RepoScanFilters,
    seen: set[str],
) -> str | None:
    eligible = True
    if abs_path.is_dir() or (not filters.follow_symlinks and abs_path.is_symlink()):
        eligible = False
    if not eligible:
        return None
    if not should_include_repo_path(rel_path, filters=filters.pathspec, allow_ignored=False):
        return None
    rel_posix = rel_path.as_posix()
    if rel_posix in seen:
        return None
    seen.add(rel_posix)
    return rel_posix


def iter_repo_files_fs(
    repo_root: Path,
    *,
    include_globs: Sequence[str],
    exclude_globs: Sequence[str],
    exclude_dirs: Sequence[str],
    follow_symlinks: bool,
) -> Iterator[Path]:
    """Yield repo files from the filesystem with pruning.

    Parameters
    ----------
    repo_root : Path
        Repository root to walk.
    include_globs : Sequence[str]
        Glob patterns to include (empty means include all).
    exclude_globs : Sequence[str]
        Glob patterns to exclude.
    exclude_dirs : Sequence[str]
        Directory names to prune during traversal.
    follow_symlinks : bool
        Whether to follow symlinked directories and files.

    Yields
    ------
    Path
        Repo-relative file paths that match the filters.
    """
    repo_root = repo_root.resolve()
    seen: set[str] = set()
    pathspec = build_repo_scan_pathspec(
        repo_root,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        exclude_dirs=exclude_dirs,
    )
    filters = _RepoScanFilters(
        pathspec=pathspec,
        follow_symlinks=follow_symlinks,
    )
    for root, dirs, files in os.walk(repo_root, followlinks=follow_symlinks):
        root_path = Path(root)
        if exclude_dirs:
            dirs[:] = [name for name in dirs if name not in exclude_dirs]
        if not follow_symlinks:
            dirs[:] = [name for name in dirs if not (root_path / name).is_symlink()]
        for filename in files:
            abs_path = root_path / filename
            rel = abs_path.relative_to(repo_root)
            rel_posix = _eligible_repo_file(
                abs_path,
                rel_path=rel,
                filters=filters,
                seen=seen,
            )
            if rel_posix is not None:
                yield rel
