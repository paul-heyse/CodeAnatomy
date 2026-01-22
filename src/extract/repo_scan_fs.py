"""Filesystem-backed repository file listing."""

from __future__ import annotations

import fnmatch
import os
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path


def _matches_any_glob(path_posix: str, globs: Sequence[str]) -> bool:
    return any(fnmatch.fnmatch(path_posix, glob) for glob in globs)


def _is_excluded_dir(rel_path: Path, exclude_dirs: Sequence[str]) -> bool:
    parts = set(rel_path.parts)
    return any(name in parts for name in exclude_dirs)


@dataclass(frozen=True)
class _RepoScanFilters:
    include_globs: Sequence[str]
    exclude_globs: Sequence[str]
    exclude_dirs: Sequence[str]
    follow_symlinks: bool


def _eligible_repo_file(
    abs_path: Path,
    *,
    rel_path: Path,
    filters: _RepoScanFilters,
    seen: set[str],
) -> str | None:
    eligible = True
    if (
        abs_path.is_dir()
        or (not filters.follow_symlinks and abs_path.is_symlink())
        or (filters.exclude_dirs and _is_excluded_dir(rel_path, filters.exclude_dirs))
    ):
        eligible = False
    if not eligible:
        return None
    rel_posix = rel_path.as_posix()
    if filters.include_globs and not _matches_any_glob(rel_posix, filters.include_globs):
        return None
    if filters.exclude_globs and _matches_any_glob(rel_posix, filters.exclude_globs):
        return None
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
    filters = _RepoScanFilters(
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        exclude_dirs=exclude_dirs,
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
