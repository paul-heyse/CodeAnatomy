"""Canonical path normalization and glob matching utilities for CQ tool.

This module consolidates duplicated path handling logic from query execution,
query parsing, batch selection, and index file discovery.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path, PurePosixPath
from typing import cast

from pathspec import PathSpec


def normalize_repo_relative_path(file_path: str | Path, *, root: Path) -> str:
    """Normalize a file path to repo-relative POSIX form when possible.

    Parameters
    ----------
    file_path : str | Path
        File path to normalize (absolute or relative).
    root : Path
        Repository root path for relative resolution.

    Returns:
    -------
    str
        Repository-relative POSIX path if file is under root, otherwise
        the original path as POSIX string.

    Examples:
    --------
    >>> root = Path("/repo")
    >>> normalize_repo_relative_path("/repo/src/foo.py", root=root)
    'src/foo.py'
    >>> normalize_repo_relative_path("/other/bar.py", root=root)
    '/other/bar.py'
    >>> normalize_repo_relative_path("src/foo.py", root=root)
    'src/foo.py'
    """
    path = Path(file_path)
    if path.is_absolute():
        try:
            return path.relative_to(root).as_posix()
        except ValueError:
            # Path is not under root, return as-is
            return path.as_posix()
    return path.as_posix()


def is_relative_to(path: Path, root: Path) -> bool:
    """Return whether ``path`` is under ``root`` without raising ValueError.

    Parameters
    ----------
    path : Path
        Path to check.
    root : Path
        Root path to test against.

    Returns:
    -------
    bool
        True if path is under root, False otherwise.

    Examples:
    --------
    >>> root = Path("/repo")
    >>> is_relative_to(Path("/repo/src/foo.py"), root)
    True
    >>> is_relative_to(Path("/other/bar.py"), root)
    False
    """
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def match_ordered_globs(rel_path: str, globs: Sequence[str]) -> bool:
    """Apply CQ's ordered include/negated-include glob semantics.

    CQ glob semantics:
    - Empty globs match all paths
    - If no positive globs exist, default is to include
    - If positive globs exist, default is to exclude
    - Process globs in order; last matching glob wins
    - Negated globs (prefix "!") exclude matches

    Parameters
    ----------
    rel_path : str
        Repository-relative POSIX path to match.
    globs : Sequence[str]
        Ordered list of glob patterns (may include negated patterns with "!" prefix).

    Returns:
    -------
    bool
        True if path matches the glob policy, False otherwise.

    Examples:
    --------
    >>> match_ordered_globs("src/foo.py", [])
    True
    >>> match_ordered_globs("src/foo.py", ["src/**"])
    True
    >>> match_ordered_globs("tests/bar.py", ["src/**"])
    False
    >>> match_ordered_globs("src/foo.py", ["src/**", "!src/foo.py"])
    False
    """
    if not globs:
        return True
    has_includes = any(not glob.startswith("!") for glob in globs)
    include = not has_includes
    for glob in globs:
        negated = glob.startswith("!")
        pattern = glob[1:] if negated else glob
        if PurePosixPath(rel_path).match(pattern):
            include = not negated
    return include


def compile_gitwildmatch(patterns: Sequence[str]) -> PathSpec:
    """Compile reusable gitwildmatch spec for high-volume match loops.

    Parameters
    ----------
    patterns : Sequence[str]
        List of gitwildmatch patterns.

    Returns:
    -------
    PathSpec
        Compiled pathspec for efficient matching.

    Examples:
    --------
    >>> spec = compile_gitwildmatch(["*.py", "!test_*.py"])
    >>> spec.match_file("foo.py")
    True
    >>> spec.match_file("test_foo.py")
    False
    """
    compiled_patterns = tuple(str(pattern) for pattern in patterns if pattern)
    pathspec_patterns = list(compiled_patterns)
    from_lines = cast("Callable[[str, object], PathSpec]", PathSpec.from_lines)
    return from_lines("gitwildmatch", pathspec_patterns)
