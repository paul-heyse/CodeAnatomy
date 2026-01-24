"""Pathspec-backed include/exclude filters for repository scans."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from pathspec import GitIgnoreSpec, PathSpec


@dataclass(frozen=True)
class RepoScanPathspec:
    """Compiled pathspec filters for repo scanning."""

    include_spec: PathSpec | None
    exclude_spec: PathSpec | None
    ignore_spec: GitIgnoreSpec | None
    exclude_dirs: frozenset[str]


def build_repo_scan_pathspec(
    repo_root: Path,
    *,
    include_globs: Sequence[str],
    exclude_globs: Sequence[str],
    exclude_dirs: Sequence[str],
) -> RepoScanPathspec:
    """Compile pathspec filters for repo scanning."""
    include_spec = PathSpec.from_lines("gitwildmatch", include_globs) if include_globs else None
    exclude_spec = PathSpec.from_lines("gitwildmatch", exclude_globs) if exclude_globs else None
    ignore_spec = _gitignore_spec(repo_root)
    return RepoScanPathspec(
        include_spec=include_spec,
        exclude_spec=exclude_spec,
        ignore_spec=ignore_spec,
        exclude_dirs=frozenset(exclude_dirs),
    )


def should_include_repo_path(
    rel_path: Path,
    *,
    filters: RepoScanPathspec,
    allow_ignored: bool,
) -> bool:
    """Return True when a repo-relative path passes pathspec filters."""
    if _is_excluded_dir(rel_path, filters.exclude_dirs):
        return False
    rel_posix = rel_path.as_posix()
    if filters.include_spec is not None and not filters.include_spec.match_file(rel_posix):
        return False
    if filters.exclude_spec is not None and filters.exclude_spec.match_file(rel_posix):
        return False
    if not allow_ignored and filters.ignore_spec is not None:
        if filters.ignore_spec.match_file(rel_posix):
            return False
    return True


def _is_excluded_dir(rel_path: Path, exclude_dirs: Iterable[str]) -> bool:
    if not exclude_dirs:
        return False
    parts = set(rel_path.parts)
    return any(name in parts for name in exclude_dirs)


def _gitignore_spec(repo_root: Path) -> GitIgnoreSpec | None:
    lines = _gitignore_lines(repo_root)
    if not lines:
        return None
    return GitIgnoreSpec.from_lines(lines)


def _gitignore_lines(repo_root: Path) -> list[str]:
    lines: list[str] = []
    root_ignore = repo_root / ".gitignore"
    if root_ignore.is_file():
        lines.extend(_read_lines(root_ignore))
    git_dir = _resolve_git_dir(repo_root)
    if git_dir is not None:
        info_ignore = git_dir / "info" / "exclude"
        if info_ignore.is_file():
            lines.extend(_read_lines(info_ignore))
    return lines


def _resolve_git_dir(repo_root: Path) -> Path | None:
    git_entry = repo_root / ".git"
    if git_entry.is_dir():
        return git_entry
    if not git_entry.is_file():
        return None
    try:
        content = git_entry.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("gitdir:"):
            raw_path = line[len("gitdir:") :].strip()
            git_dir = Path(raw_path)
            if not git_dir.is_absolute():
                git_dir = (repo_root / git_dir).resolve()
            return git_dir
    return None


def _read_lines(path: Path) -> list[str]:
    try:
        return path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []


__all__ = [
    "RepoScanPathspec",
    "build_repo_scan_pathspec",
    "should_include_repo_path",
]
