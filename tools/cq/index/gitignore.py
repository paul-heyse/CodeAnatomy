"""Gitignore aggregation utilities for CQ file tabulation."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pygit2
from pathspec import GitIgnoreSpec


@dataclass(frozen=True)
class GitIgnoreInputs:
    """Collected gitignore input patterns."""

    patterns: list[str]
    sources: list[str]


def load_gitignore_spec(
    repo_root: Path,
    git_dir: Path | None,
    repo: pygit2.Repository | None,
) -> GitIgnoreSpec:
    """Load GitIgnoreSpec from repository ignore sources.

    Returns
    -------
    GitIgnoreSpec
        Compiled ignore spec from repository sources.
    """
    inputs = collect_gitignore_inputs(repo_root, git_dir, repo)
    return cast(
        "GitIgnoreSpec",
        cast("Any", GitIgnoreSpec).from_lines("gitignore", inputs.patterns),
    )


def collect_gitignore_inputs(
    repo_root: Path,
    git_dir: Path | None,
    repo: pygit2.Repository | None,
) -> GitIgnoreInputs:
    """Collect gitignore patterns with Git-like semantics.

    Returns
    -------
    GitIgnoreInputs
        Collected patterns and their sources.
    """
    patterns: list[str] = []
    sources: list[str] = []

    for ignore_path in iter_gitignore_files(repo_root):
        rel_dir = ignore_path.parent.relative_to(repo_root).as_posix()
        if rel_dir == ".":
            rel_dir = ""
        for line in _read_ignore_lines(ignore_path):
            prefixed = _prefix_pattern(line, rel_dir)
            if prefixed is None:
                continue
            patterns.append(prefixed)
        sources.append(str(ignore_path))

    info_exclude = _load_info_exclude(git_dir)
    if info_exclude:
        patterns.extend(info_exclude)
        sources.append(str(_info_exclude_path(git_dir)))

    global_excludes = _load_global_excludes(repo, repo_root)
    if global_excludes:
        patterns.extend(global_excludes)
        sources.append("global_excludes")

    return GitIgnoreInputs(patterns=patterns, sources=sources)


def iter_gitignore_files(repo_root: Path) -> Iterable[Path]:
    """Yield all .gitignore files under the repository root.

    Yields
    ------
    Path
        Paths to .gitignore files.
    """
    for path in repo_root.rglob(".gitignore"):
        if ".git" in path.parts:
            continue
        yield path


def _read_ignore_lines(path: Path) -> list[str]:
    """Read ignore file lines, returning empty list on I/O failure.

    Returns
    -------
    list[str]
        Lines from the ignore file.
    """
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []


def _prefix_pattern(line: str, rel_dir: str) -> str | None:
    """Prefix gitignore patterns with their directory scope.

    Returns
    -------
    str | None
        Normalized pattern or None if the line should be ignored.
    """
    if not line.strip():
        return ""
    stripped = line.lstrip()
    if stripped.startswith("#"):
        return line

    pattern = line
    negated = False
    if pattern.startswith("!"):
        negated = True
        pattern = pattern[1:]

    if pattern.startswith("/"):
        pattern = pattern[1:]

    if rel_dir:
        pattern = f"{rel_dir}/{pattern}" if pattern else rel_dir

    if negated:
        pattern = f"!{pattern}"

    return pattern


def _info_exclude_path(git_dir: Path | None) -> Path | None:
    if git_dir is None:
        return None
    return git_dir / "info" / "exclude"


def _load_info_exclude(git_dir: Path | None) -> list[str]:
    exclude_path = _info_exclude_path(git_dir)
    if exclude_path is None or not exclude_path.exists():
        return []
    return _read_ignore_lines(exclude_path)


def _load_global_excludes(
    repo: pygit2.Repository | None,
    repo_root: Path,
) -> list[str]:
    excludes_file = _resolve_global_excludes_file(repo, repo_root)
    if excludes_file is None or not excludes_file.exists():
        return []
    return _read_ignore_lines(excludes_file)


def _resolve_global_excludes_file(
    repo: pygit2.Repository | None,
    repo_root: Path,
) -> Path | None:
    config: pygit2.Config | None = None
    if repo is not None:
        config = repo.config
    if config is None:
        try:
            config = pygit2.Config.get_global_config()
        except pygit2.GitError:
            config = None

    excludes_value: str | None = None
    if config is not None:
        excludes_value = _read_config_value(config, "core.excludesfile")

    if excludes_value:
        excludes_path = Path(excludes_value).expanduser()
        if excludes_path.is_absolute():
            return excludes_path
        return (repo_root / excludes_path).resolve()

    xdg_config = Path.home() / ".config" / "git" / "ignore"
    if xdg_config.exists():
        return xdg_config
    legacy = Path.home() / ".gitignore_global"
    if legacy.exists():
        return legacy
    return None


def _read_config_value(config: pygit2.Config, key: str) -> str | None:
    config_any = cast("Any", config)
    try:
        value = config_any.get(key) if hasattr(config_any, "get") else None
    except (KeyError, TypeError):
        value = None
    if value is None:
        try:
            value = config_any[key]
        except (KeyError, TypeError):
            value = None
    if value is None:
        return None
    value_attr = getattr(value, "value", None)
    if value_attr is not None:
        return str(value_attr)
    return str(value)
