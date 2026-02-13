"""Shared scope filtering helpers for macro file enumeration."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from tools.cq.index.files import build_repo_file_index, tabulate_files
from tools.cq.index.repo import resolve_repo_context


def scope_filter_applied(
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
) -> bool:
    """Return whether scope include/exclude filters are configured."""
    return bool(include or exclude)


def resolve_macro_files(
    *,
    root: Path,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    extensions: tuple[str, ...],
) -> list[Path]:
    """Resolve macro scan files using shared include/exclude semantics.

    Returns:
        Files selected for the macro scan.
    """
    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    globs: list[str] = []
    if include:
        globs.extend(str(pattern) for pattern in include if str(pattern).strip())
    if exclude:
        globs.extend(
            f"!{pattern}" for pattern in (str(value).strip() for value in exclude) if pattern
        )
    result = tabulate_files(
        repo_index,
        [repo_context.repo_root],
        globs or None,
        extensions=extensions,
    )
    return result.files


__all__ = ["resolve_macro_files", "scope_filter_applied"]
