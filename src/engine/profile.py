"""Environment class detection for the Rust engine session factory."""

from __future__ import annotations

import os
from pathlib import Path

_SMALL_THRESHOLD = 50
_MEDIUM_THRESHOLD = 500


def detect_environment_class(repo_root: str | Path) -> str:
    """Detect the environment class based on repository size.

    Args:
        repo_root: Path to the repository root.

    Returns:
        One of "small", "medium", "large".
    """
    repo_root = Path(repo_root)
    if not repo_root.is_dir():
        return "small"

    # Count Python files as a proxy for repository size
    py_files = list(repo_root.rglob("*.py"))
    file_count = len(py_files)

    if file_count < _SMALL_THRESHOLD:
        return "small"
    if file_count < _MEDIUM_THRESHOLD:
        return "medium"
    return "large"


def environment_class_from_env() -> str:
    """Read environment class from CODEANATOMY_ENV_CLASS env var.

    Returns:
        One of "small", "medium", "large". Defaults to "medium".
    """
    return os.environ.get("CODEANATOMY_ENV_CLASS", "medium")
