"""Repository discovery utilities for CQ."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pygit2


@dataclass(frozen=True)
class RepoContext:
    """Resolved git repository context.

    Attributes:
    ----------
    repo_root
        Resolved repository root (workdir for non-bare repos).
    workdir
        Repository workdir (same as repo_root for non-bare repos).
    is_bare
        True if repository is bare.
    git_dir
        Path to the .git directory, if available.
    """

    repo_root: Path
    workdir: Path
    is_bare: bool
    git_dir: Path | None


def resolve_repo_context(start: Path | None = None) -> RepoContext:
    """Resolve repository context using pygit2 discovery.

    Parameters
    ----------
    start
        Starting path for repository discovery. Defaults to CWD.

    Returns:
    -------
    RepoContext
        Resolved repository context.
    """
    if start is None:
        start = Path.cwd()

    discovered: str | None
    try:
        discovered = pygit2.discover_repository(str(start))
    except (pygit2.GitError, OSError, ValueError):
        discovered = None

    if not discovered:
        repo_root = start.resolve()
        return RepoContext(
            repo_root=repo_root,
            workdir=repo_root,
            is_bare=False,
            git_dir=None,
        )

    repo = pygit2.Repository(discovered)
    workdir = Path(repo.workdir) if repo.workdir else Path(repo.path).parent
    repo_root = workdir.resolve()
    git_dir = Path(repo.path).resolve()
    return RepoContext(
        repo_root=repo_root,
        workdir=workdir.resolve(),
        is_bare=repo.is_bare,
        git_dir=git_dir,
    )


def open_repo(repo_context: RepoContext) -> pygit2.Repository | None:
    """Open a pygit2 repository if git_dir is available.

    Returns:
    -------
    pygit2.Repository | None
        Repository handle when available.
    """
    if repo_context.git_dir is None:
        return None
    try:
        return pygit2.Repository(str(repo_context.git_dir))
    except pygit2.GitError:
        return None
