"""Path helper utilities for CLI commands."""

from __future__ import annotations

from pathlib import Path


def resolve_path(repo_root: Path, value: Path | str | None) -> Path | None:
    """Resolve a path relative to repo_root when needed.

    Parameters
    ----------
    repo_root
        Repository root path.
    value
        Input path or string.

    Returns:
    -------
    Path | None
        Resolved absolute path or None when input is None.
    """
    if value is None:
        return None
    path = Path(value)
    return path if path.is_absolute() else repo_root / path


__all__ = ["resolve_path"]
