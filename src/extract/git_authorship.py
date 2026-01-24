"""Mailmap and blame helpers for git-aware diagnostics."""

from __future__ import annotations

from dataclasses import dataclass

import pygit2


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

    Returns
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

    Returns
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

    Returns
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


__all__ = ["BlameHunk", "blame_hunks", "resolve_mailmap", "resolve_signature"]
