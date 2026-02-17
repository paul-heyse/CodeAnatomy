"""Path helper utilities shared by core modules."""

from __future__ import annotations

from pathlib import Path


def safe_rel_path(*, root: Path, path: Path) -> str:
    """Return repo-relative POSIX path when possible, else absolute POSIX path."""
    resolved_root = root.resolve()
    resolved_path = path.resolve()
    try:
        return resolved_path.relative_to(resolved_root).as_posix()
    except ValueError:
        return resolved_path.as_posix()


__all__ = ["safe_rel_path"]
