"""Shared file-scan helpers for CQ macro implementations."""

from __future__ import annotations

from pathlib import Path

from tools.cq.macros.scope_filters import resolve_macro_files


def iter_files(
    *,
    root: Path,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    extensions: tuple[str, ...] = (".py",),
    max_files: int | None = None,
) -> list[Path]:
    """Resolve files for macro scans with optional include/exclude globs."""
    rows = resolve_macro_files(
        root=root,
        include=include,
        exclude=exclude,
        extensions=extensions,
    )
    if max_files is None:
        return rows
    return rows[: max(0, int(max_files))]


__all__ = ["iter_files"]
