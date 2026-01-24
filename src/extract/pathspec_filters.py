"""Pathspec-backed include/exclude filters for repository scans."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

from pathspec import GitIgnoreSpec, PathSpec


@dataclass(frozen=True)
class RepoScanPathspec:
    """Compiled pathspec filters for repo scanning."""

    include_spec: PathSpec | None
    exclude_spec: PathSpec | None
    ignore_spec: GitIgnoreSpec | None
    exclude_dirs: frozenset[str]


@dataclass(frozen=True)
class PathspecCheck:
    """Decision and indices for pathspec filtering."""

    include: bool
    excluded_by_dir: bool
    include_index: int | None
    exclude_index: int | None
    ignore_index: int | None
    ignored: bool


class _CheckResult(Protocol):
    include: bool | None
    index: int | None


def build_repo_scan_pathspec(
    repo_root: Path,
    *,
    include_globs: Sequence[str],
    exclude_globs: Sequence[str],
    exclude_dirs: Sequence[str],
) -> RepoScanPathspec:
    """Compile pathspec filters for repo scanning.

    Returns
    -------
    RepoScanPathspec
        Compiled pathspec filters.
    """
    include_lines = list(include_globs)
    exclude_lines = list(exclude_globs)
    from_lines = cast("Callable[[str, Iterable[str]], PathSpec]", PathSpec.from_lines)
    include_spec = from_lines("gitwildmatch", include_lines) if include_lines else None
    exclude_spec = from_lines("gitwildmatch", exclude_lines) if exclude_lines else None
    ignore_spec = _gitignore_spec(repo_root)
    return RepoScanPathspec(
        include_spec=include_spec,
        exclude_spec=exclude_spec,
        ignore_spec=ignore_spec,
        exclude_dirs=frozenset(exclude_dirs),
    )


def check_repo_path(
    rel_path: Path,
    *,
    filters: RepoScanPathspec,
    allow_ignored: bool,
) -> PathspecCheck:
    """Return a pathspec decision with match indices when available.

    Returns
    -------
    PathspecCheck
        Decision payload for the path.
    """
    if _is_excluded_dir(rel_path, filters.exclude_dirs):
        return PathspecCheck(
            include=False,
            excluded_by_dir=True,
            include_index=None,
            exclude_index=None,
            ignore_index=None,
            ignored=False,
        )
    rel_posix = rel_path.as_posix()
    include_result = _check_spec(filters.include_spec, rel_posix)
    if (
        filters.include_spec is not None
        and include_result is not None
        and include_result.include is not True
    ):
        return PathspecCheck(
            include=False,
            excluded_by_dir=False,
            include_index=include_result.index,
            exclude_index=None,
            ignore_index=None,
            ignored=False,
        )
    exclude_result = _check_spec(filters.exclude_spec, rel_posix)
    if (
        filters.exclude_spec is not None
        and exclude_result is not None
        and exclude_result.include is True
    ):
        return PathspecCheck(
            include=False,
            excluded_by_dir=False,
            include_index=include_result.index if include_result is not None else None,
            exclude_index=exclude_result.index,
            ignore_index=None,
            ignored=False,
        )
    ignore_result = _check_spec(filters.ignore_spec, rel_posix)
    ignored = ignore_result is not None and ignore_result.include is True
    if ignore_result is not None and ignore_result.include is True and not allow_ignored:
        return PathspecCheck(
            include=False,
            excluded_by_dir=False,
            include_index=include_result.index if include_result is not None else None,
            exclude_index=exclude_result.index if exclude_result is not None else None,
            ignore_index=ignore_result.index,
            ignored=True,
        )
    return PathspecCheck(
        include=True,
        excluded_by_dir=False,
        include_index=include_result.index if include_result is not None else None,
        exclude_index=exclude_result.index if exclude_result is not None else None,
        ignore_index=ignore_result.index if ignore_result is not None else None,
        ignored=ignored,
    )


def should_include_repo_path(
    rel_path: Path,
    *,
    filters: RepoScanPathspec,
    allow_ignored: bool,
) -> bool:
    """Return True when a repo-relative path passes pathspec filters.

    Returns
    -------
    bool
        ``True`` when the path should be included.
    """
    return check_repo_path(rel_path, filters=filters, allow_ignored=allow_ignored).include


def _is_excluded_dir(rel_path: Path, exclude_dirs: Iterable[str]) -> bool:
    if not exclude_dirs:
        return False
    parts = set(rel_path.parts)
    return any(name in parts for name in exclude_dirs)


def _gitignore_spec(repo_root: Path) -> GitIgnoreSpec | None:
    lines = _gitignore_lines(repo_root)
    if not lines:
        return None
    ignore_lines = list(lines)
    ignore_from_lines = cast("Callable[[Iterable[str]], GitIgnoreSpec]", GitIgnoreSpec.from_lines)
    return ignore_from_lines(ignore_lines)


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
    for raw_line in content.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("gitdir:"):
            raw_path = stripped[len("gitdir:") :].strip()
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


def _check_spec(spec: PathSpec | GitIgnoreSpec | None, path: str) -> _CheckResult | None:
    if spec is None:
        return None
    result = spec.check_file(path)
    return cast("_CheckResult", result)


__all__ = [
    "PathspecCheck",
    "RepoScanPathspec",
    "build_repo_scan_pathspec",
    "check_repo_path",
    "should_include_repo_path",
]
