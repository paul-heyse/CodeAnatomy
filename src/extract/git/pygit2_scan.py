"""pygit2-backed repository file listing."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pygit2

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from extract.scanning.repo_scope import RepoScope


def repo_status_paths(scope: RepoScope) -> Mapping[str, int]:
    """Return repository status flags keyed by path.

    Returns
    -------
    Mapping[str, int]
        Mapping of path to pygit2 status flags.

    Raises
    ------
    ValueError
        Raised when repository status cannot be read.
    """
    if scope.repo.workdir is None:
        msg = "Repository workdir is required for status-based scans."
        raise ValueError(msg)
    flags = int(getattr(pygit2, "GIT_STATUS_OPT_DISABLE_PATHSPEC_MATCH", 0))
    if scope.include_untracked:
        flags |= int(getattr(pygit2, "GIT_STATUS_OPT_INCLUDE_UNTRACKED", 0))
        flags |= int(getattr(pygit2, "GIT_STATUS_OPT_RECURSE_UNTRACKED_DIRS", 0))
    status_options_type = getattr(pygit2, "StatusOptions", None)
    options = None
    if status_options_type is not None:
        show_flag = int(getattr(pygit2, "GIT_STATUS_SHOW_INDEX_AND_WORKDIR", 0))
        options = status_options_type(show=show_flag, flags=flags)
    try:
        status_fn = cast("Callable[..., Mapping[str, int]]", scope.repo.status)
        return status_fn(options) if options is not None else status_fn()
    except pygit2.GitError as exc:
        msg = "Failed to read repository status via pygit2."
        raise ValueError(msg) from exc


def iter_repo_candidate_paths(scope: RepoScope) -> list[str]:
    """Return candidate repo paths from status and index.

    Returns
    -------
    list[str]
        Sorted repo-relative paths.
    """
    status = repo_status_paths(scope)
    paths: set[str] = set(status)
    paths.update(entry.path for entry in scope.repo.index)
    return sorted(paths)


def iter_repo_files_pygit2(scope: RepoScope) -> list[str]:
    """Return sorted repo-relative paths using pygit2 status.

    Returns
    -------
    list[str]
        Sorted repo-relative paths.
    """
    return iter_repo_candidate_paths(scope)


__all__ = ["iter_repo_candidate_paths", "iter_repo_files_pygit2", "repo_status_paths"]
