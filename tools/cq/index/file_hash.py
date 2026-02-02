"""File hashing utilities for content-based change detection.

Provides fast, reliable hashing and modification time tracking for Python files,
with batch processing support for efficient pipeline execution.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


def compute_file_hash(path: Path) -> str | None:
    """Compute SHA-256 hash of file contents.

    Parameters
    ----------
    path : Path
        Path to file to hash.

    Returns
    -------
    str | None
        Hex digest of SHA-256 hash, or None if file cannot be read.

    Examples
    --------
    >>> hash_val = compute_file_hash(Path("script.py"))
    >>> isinstance(hash_val, str) and len(hash_val) == 64
    True
    """
    try:
        content = path.read_bytes()
        return hashlib.sha256(content).hexdigest()
    except (OSError, IOError):
        return None


def compute_file_mtime(path: Path) -> float | None:
    """Compute modification time of file.

    Parameters
    ----------
    path : Path
        Path to file.

    Returns
    -------
    float | None
        Modification time as POSIX timestamp, or None if file cannot be stat'd.

    Examples
    --------
    >>> mtime = compute_file_mtime(Path("script.py"))
    >>> isinstance(mtime, float)
    True
    """
    try:
        return path.stat().st_mtime
    except (OSError, IOError):
        return None


def hash_files_batch(paths: Sequence[Path]) -> dict[Path, tuple[str, float]]:
    """Compute hashes and modification times for multiple files.

    Parameters
    ----------
    paths : Sequence[Path]
        Paths to files to process.

    Returns
    -------
    dict[Path, tuple[str, float]]
        Mapping from path to (hash, mtime) tuple. Files that cannot be
        processed are excluded from the result.

    Examples
    --------
    >>> results = hash_files_batch([Path("a.py"), Path("b.py")])
    >>> all(isinstance(h, str) and isinstance(m, float) for h, m in results.values())
    True
    """
    results: dict[Path, tuple[str, float]] = {}
    for path in paths:
        file_hash = compute_file_hash(path)
        file_mtime = compute_file_mtime(path)
        if file_hash is not None and file_mtime is not None:
            results[path] = (file_hash, file_mtime)
    return results


def files_changed_since(
    paths: Sequence[Path],
    cached_hashes: dict[str, str],
) -> list[Path]:
    """Identify files that have changed compared to cached hashes.

    Parameters
    ----------
    paths : Sequence[Path]
        Paths to check.
    cached_hashes : dict[str, str]
        Mapping from file path (as string) to cached hash value.

    Returns
    -------
    list[Path]
        Paths of files that are new, modified, or cannot be read.
        Files that cannot be read are included to trigger reprocessing.

    Examples
    --------
    >>> cached = {"/path/to/a.py": "abc123"}
    >>> changed = files_changed_since([Path("/path/to/a.py")], cached)
    >>> isinstance(changed, list)
    True
    """
    changed: list[Path] = []
    for path in paths:
        path_str = str(path)
        cached_hash = cached_hashes.get(path_str)

        # If no cached hash, file is new
        if cached_hash is None:
            changed.append(path)
            continue

        # Compute current hash
        current_hash = compute_file_hash(path)

        # If cannot read or hash differs, mark as changed
        if current_hash is None or current_hash != cached_hash:
            changed.append(path)

    return changed
