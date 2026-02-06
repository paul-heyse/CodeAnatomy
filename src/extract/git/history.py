"""Git history helpers: blame, mailmap, and diff tracking.

This module combines git authorship (blame/mailmap) and delta (diff) utilities
for git-aware diagnostics and change detection.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pygit2

from extract.git.context import open_git_context

# ---------------------------------------------------------------------------
# Blame / Mailmap helpers (from git_authorship.py)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BlameHunk:
    """Summarized blame hunk metadata."""

    path: str
    start_line: int
    lines: int
    commit_id: str
    author_name: str
    author_email: str


def resolve_mailmap(repo: pygit2.Repository) -> pygit2.Mailmap | None:
    """Return the repository mailmap when available.

    Returns:
    -------
    pygit2.Mailmap | None
        Mailmap when available.
    """
    try:
        return pygit2.Mailmap.from_repository(repo)
    except pygit2.GitError:
        return None


def resolve_signature(
    repo: pygit2.Repository,
    signature: pygit2.Signature,
) -> pygit2.Signature:
    """Resolve a signature using the repository mailmap.

    Returns:
    -------
    pygit2.Signature
        Resolved signature.
    """
    mailmap = resolve_mailmap(repo)
    if mailmap is None:
        return signature
    return mailmap.resolve_signature(signature)


def blame_hunks(
    repo: pygit2.Repository,
    *,
    path_posix: str,
    ref: str | None = None,
) -> tuple[BlameHunk, ...]:
    """Return simplified blame hunks for a file path.

    Returns:
    -------
    tuple[BlameHunk, ...]
        Blame hunks for the file path.
    """
    try:
        blame = repo.blame(path_posix, newest_commit=ref) if ref else repo.blame(path_posix)
    except pygit2.GitError:
        return ()
    mailmap = resolve_mailmap(repo)
    hunks: list[BlameHunk] = []
    for hunk in blame:
        signature = getattr(hunk, "final_signature", None)
        if not isinstance(signature, pygit2.Signature):
            signature = getattr(hunk, "orig_signature", None)
        if not isinstance(signature, pygit2.Signature):
            signature = pygit2.Signature("unknown", "unknown")
        author = signature
        if mailmap is not None:
            author = mailmap.resolve_signature(author)
        hunks.append(
            BlameHunk(
                path=path_posix,
                start_line=hunk.final_start_line_number,
                lines=hunk.lines_in_hunk,
                commit_id=str(hunk.final_commit_id),
                author_name=author.name,
                author_email=author.email,
            )
        )
    return tuple(hunks)


# ---------------------------------------------------------------------------
# Diff / Delta helpers (from git_delta.py)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GitDeltaPaths:
    """Changed and deleted paths between two refs."""

    changed_paths: frozenset[str]
    deleted_paths: frozenset[str]


def diff_paths(
    repo_root: Path,
    *,
    base_ref: str,
    head_ref: str,
) -> GitDeltaPaths | None:
    """Return changed/deleted paths between two refs, or None on failure.

    Returns:
    -------
    GitDeltaPaths | None
        Delta path summary when available.
    """
    git_ctx = open_git_context(repo_root)
    if git_ctx is None:
        return None
    repo = git_ctx.repo
    base_commit = _resolve_commit(repo, base_ref)
    head_commit = _resolve_commit(repo, head_ref)
    if base_commit is None or head_commit is None:
        return None
    try:
        diff = repo.diff(base_commit, head_commit)
    except pygit2.GitError:
        return None
    changed: set[str] = set()
    deleted: set[str] = set()
    for delta in diff.deltas:
        status = delta.status
        if status == pygit2.GIT_DELTA_DELETED:
            deleted.add(delta.old_file.path)
            continue
        path = delta.new_file.path or delta.old_file.path
        if path:
            changed.add(path)
    return GitDeltaPaths(
        changed_paths=frozenset(changed),
        deleted_paths=frozenset(deleted),
    )


def _resolve_commit(repo: pygit2.Repository, ref: str) -> pygit2.Commit | None:
    try:
        obj = repo.revparse_single(ref)
        return obj.peel(pygit2.Commit)
    except (KeyError, ValueError, pygit2.GitError):
        return None


__all__ = [
    "BlameHunk",
    "GitDeltaPaths",
    "blame_hunks",
    "diff_paths",
    "resolve_mailmap",
    "resolve_signature",
]
