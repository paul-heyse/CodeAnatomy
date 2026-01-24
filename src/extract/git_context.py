"""Git repository context helpers backed by pygit2."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import pygit2

from core_types import PathLike, ensure_path
from extract.git_settings import apply_git_settings_once


@dataclass(frozen=True)
class GitContext:
    """Resolved Git repository context."""

    repo: pygit2.Repository
    repo_root: Path
    head_sha: str | None
    origin_url: str | None


def open_git_context(path: PathLike) -> GitContext | None:
    """Return a GitContext if the path belongs to a git repository.

    Returns
    -------
    GitContext | None
        Git context when discovery succeeds.
    """
    apply_git_settings_once()
    repo_path = _discover_repo_path(path)
    if repo_path is None:
        return None
    repo = pygit2.Repository(str(repo_path))
    repo_root = Path(repo.workdir) if repo.workdir else Path(repo.path)
    return GitContext(
        repo=repo,
        repo_root=repo_root,
        head_sha=_head_sha(repo),
        origin_url=_origin_url(repo),
    )


def discover_repo_root(path: PathLike) -> Path | None:
    """Return the repository root for a path when available.

    Returns
    -------
    pathlib.Path | None
        Repository root when available.
    """
    repo_path = _discover_repo_path(path)
    if repo_path is None:
        return None
    repo = pygit2.Repository(str(repo_path))
    return Path(repo.workdir) if repo.workdir else Path(repo.path)


def discover_repo_root_from_paths(paths: Iterable[PathLike]) -> Path | None:
    """Return the first repository root discovered from input paths.

    Returns
    -------
    pathlib.Path | None
        First repository root found, if any.
    """
    for path in paths:
        root = discover_repo_root(path)
        if root is not None:
            return root
    return None


def github_name_with_owner(origin_url: str | None) -> str | None:
    """Return a nameWithOwner slug for GitHub origins when possible.

    Returns
    -------
    str | None
        GitHub nameWithOwner slug when derivable.
    """
    if not origin_url:
        return None
    parsed = urlparse(origin_url)
    if parsed.scheme and parsed.hostname:
        if parsed.hostname.lower() != "github.com":
            return None
        path = parsed.path.lstrip("/")
        return _strip_dot_git(path)
    if ":" in origin_url:
        user_host, path = origin_url.split(":", 1)
        host = user_host.split("@")[-1]
        if host.lower() != "github.com":
            return None
        return _strip_dot_git(path.lstrip("/"))
    return None


def default_project_name(repo_root: Path) -> str:
    """Return a default project name derived from a repo root.

    Returns
    -------
    str
        Project name derived from the repo root.
    """
    return repo_root.name or str(repo_root)


def _discover_repo_path(path: PathLike) -> Path | None:
    try:
        repo_path = pygit2.discover_repository(str(ensure_path(path)))
    except (KeyError, ValueError, pygit2.GitError):
        return None
    if repo_path is None:
        return None
    return Path(repo_path)


def _origin_url(repo: pygit2.Repository) -> str | None:
    try:
        remote = repo.remotes["origin"]
    except KeyError:
        return None
    return remote.url


def _head_sha(repo: pygit2.Repository) -> str | None:
    if repo.head_is_unborn:
        return None
    try:
        head = repo.head.peel(pygit2.Commit)
    except (KeyError, ValueError, pygit2.GitError):
        return None
    return str(head.id)


def _strip_dot_git(path: str) -> str:
    if path.endswith(".git"):
        return path[: -len(".git")]
    return path


__all__ = [
    "GitContext",
    "default_project_name",
    "discover_repo_root",
    "discover_repo_root_from_paths",
    "github_name_with_owner",
    "open_git_context",
]
