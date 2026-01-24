"""Git submodule and worktree discovery helpers."""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

import pygit2

from extract.git_context import open_git_context


@dataclass(frozen=True)
class SubmoduleRoot:
    """Submodule root with repo-relative prefix."""

    repo_root: Path
    prefix: Path


@dataclass(frozen=True)
class WorktreeRoot:
    """Worktree root with identifier name."""

    repo_root: Path
    name: str


@dataclass(frozen=True)
class SubmoduleUpdateResult:
    """Summary of submodule update attempts."""

    updated: tuple[str, ...]
    failed: tuple[str, ...]


def submodule_roots(repo_root: Path) -> tuple[SubmoduleRoot, ...]:
    """Return submodule roots for a repository when available.

    Returns
    -------
    tuple[SubmoduleRoot, ...]
        Submodule roots discovered for the repo.
    """
    git_ctx = open_git_context(repo_root)
    if git_ctx is None:
        return ()
    repo = git_ctx.repo
    cache_all = getattr(repo.submodules, "cache_all", None)
    if callable(cache_all):
        with suppress(pygit2.GitError):
            cache_all()
    roots: list[SubmoduleRoot] = []
    for name in repo.listall_submodules():
        with suppress(pygit2.GitError):
            submodule = repo.submodules.get(name)
            if submodule is None:
                continue
            rel_path = Path(submodule.path)
            abs_path = (git_ctx.repo_root / rel_path).resolve()
            if abs_path.is_dir():
                roots.append(SubmoduleRoot(repo_root=abs_path, prefix=rel_path))
    return tuple(roots)


def update_submodules(
    repo_root: Path,
    *,
    init: bool,
    depth: int | None,
    submodules: tuple[str, ...] | None = None,
    callbacks: pygit2.RemoteCallbacks | None = None,
) -> SubmoduleUpdateResult:
    """Update submodules to the commits recorded in the superproject.

    Returns
    -------
    SubmoduleUpdateResult
        Summary of updated and failed submodules.
    """
    git_ctx = open_git_context(repo_root)
    if git_ctx is None:
        return SubmoduleUpdateResult(updated=(), failed=())
    repo = git_ctx.repo
    names = submodules or tuple(repo.listall_submodules())
    if not names:
        return SubmoduleUpdateResult(updated=(), failed=())
    try:
        repo.submodules.update(
            submodules=names,
            init=init,
            callbacks=callbacks,
            depth=depth or 0,
        )
    except pygit2.GitError:
        return SubmoduleUpdateResult(updated=(), failed=names)
    reload_submodules = getattr(repo.submodules, "reload", None)
    if callable(reload_submodules):
        with suppress(pygit2.GitError):
            reload_submodules()
    return SubmoduleUpdateResult(updated=names, failed=())


def worktree_roots(repo_root: Path) -> tuple[WorktreeRoot, ...]:
    """Return worktree roots for a repository when available.

    Returns
    -------
    tuple[WorktreeRoot, ...]
        Worktree roots discovered for the repo.
    """
    git_ctx = open_git_context(repo_root)
    if git_ctx is None:
        return ()
    repo = git_ctx.repo
    list_worktrees = getattr(repo, "list_worktrees", None)
    lookup_worktree = getattr(repo, "lookup_worktree", None)
    if list_worktrees is None or lookup_worktree is None:
        return ()
    try:
        worktree_names = list_worktrees()
    except pygit2.GitError:
        return ()
    roots: list[WorktreeRoot] = []
    for name in worktree_names:
        with suppress(pygit2.GitError):
            worktree = lookup_worktree(name)
            path = getattr(worktree, "path", None)
            if not path:
                continue
            root = Path(path).resolve()
            if not root.is_dir():
                continue
            roots.append(WorktreeRoot(repo_root=root, name=name))
    return tuple(roots)


__all__ = [
    "SubmoduleRoot",
    "SubmoduleUpdateResult",
    "WorktreeRoot",
    "submodule_roots",
    "update_submodules",
    "worktree_roots",
]
