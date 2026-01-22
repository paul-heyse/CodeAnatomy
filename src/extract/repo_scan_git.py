"""Git-backed repository file listing."""

from __future__ import annotations

import fnmatch
import logging
import subprocess
from collections.abc import Sequence
from pathlib import Path

LOGGER = logging.getLogger(__name__)


def _matches_any_glob(path_posix: str, globs: Sequence[str]) -> bool:
    return any(fnmatch.fnmatch(path_posix, glob) for glob in globs)


def _is_excluded_dir(rel_path: Path, exclude_dirs: Sequence[str]) -> bool:
    parts = set(rel_path.parts)
    return any(name in parts for name in exclude_dirs)


def _git_ls_files(repo_root: Path) -> list[str] | None:
    cmd = [
        "git",
        "-C",
        str(repo_root),
        "ls-files",
        "-z",
        "--cached",
        "--others",
        "--exclude-standard",
    ]
    try:
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
        )
    except OSError as exc:
        LOGGER.debug("git ls-files failed: %s", exc)
        return None
    if result.returncode != 0:
        err = result.stderr.decode("utf-8", errors="replace").strip()
        LOGGER.debug("git ls-files returned %s: %s", result.returncode, err)
        return None
    entries = result.stdout.split(b"\x00")
    return [entry.decode("utf-8", errors="replace") for entry in entries if entry]


def iter_repo_files_git(
    repo_root: Path,
    *,
    include_globs: Sequence[str],
    exclude_globs: Sequence[str],
    exclude_dirs: Sequence[str],
    follow_symlinks: bool,
) -> list[Path] | None:
    """Return git-listed repo files, or None if git is unavailable.

    Parameters
    ----------
    repo_root : Path
        Repository root to scan with git.
    include_globs : Sequence[str]
        Glob patterns to include (empty means include all).
    exclude_globs : Sequence[str]
        Glob patterns to exclude.
    exclude_dirs : Sequence[str]
        Directory names to prune during filtering.
    follow_symlinks : bool
        Whether to include symlinked files.

    Returns
    -------
    list[Path] | None
        Repo-relative file paths or None when git is unavailable.
    """
    items = _git_ls_files(repo_root)
    if items is None:
        return None
    resolved = repo_root.resolve()
    paths: list[Path] = []
    for item in items:
        rel = Path(item)
        if rel.is_absolute():
            continue
        if exclude_dirs and _is_excluded_dir(rel, exclude_dirs):
            continue
        rel_posix = rel.as_posix()
        if include_globs and not _matches_any_glob(rel_posix, include_globs):
            continue
        if exclude_globs and _matches_any_glob(rel_posix, exclude_globs):
            continue
        abs_path = resolved / rel
        if abs_path.is_dir():
            continue
        if not follow_symlinks and abs_path.is_symlink():
            continue
        paths.append(rel)
    return paths
