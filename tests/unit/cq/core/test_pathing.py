"""Tests for tools.cq.core.pathing canonical path/glob utilities."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.pathing import (
    compile_gitwildmatch,
    is_relative_to,
    match_ordered_globs,
    normalize_repo_relative_path,
)


def test_normalize_repo_relative_path_absolute_under_root(tmp_path: Path) -> None:
    """Test normalization of absolute path under root."""
    root = tmp_path
    file_path = root / "src" / "foo.py"
    result = normalize_repo_relative_path(file_path, root=root)
    assert result == "src/foo.py"


def test_normalize_repo_relative_path_absolute_not_under_root(tmp_path: Path) -> None:
    """Test normalization of absolute path not under root."""
    root = tmp_path / "repo"
    file_path = tmp_path / "other" / "bar.py"
    result = normalize_repo_relative_path(file_path, root=root)
    # Should return the absolute path as POSIX string
    assert result == file_path.as_posix()


def test_normalize_repo_relative_path_relative_path() -> None:
    """Test normalization of relative path."""
    root = Path("/repo")
    file_path = "src/foo.py"
    result = normalize_repo_relative_path(file_path, root=root)
    assert result == "src/foo.py"


def test_is_relative_to_path_under_root(tmp_path: Path) -> None:
    """Test is_relative_to with path under root returns True."""
    root = tmp_path
    path = root / "src" / "foo.py"
    assert is_relative_to(path, root) is True


def test_is_relative_to_path_not_under_root(tmp_path: Path) -> None:
    """Test is_relative_to with path not under root returns False."""
    root = tmp_path / "repo"
    path = tmp_path / "other" / "bar.py"
    assert is_relative_to(path, root) is False


def test_match_ordered_globs_empty_globs() -> None:
    """Test match_ordered_globs with empty globs returns True."""
    assert match_ordered_globs("src/foo.py", []) is True


def test_match_ordered_globs_matching_include_glob() -> None:
    """Test match_ordered_globs with matching include glob."""
    assert match_ordered_globs("src/foo.py", ["src/**"]) is True
    assert match_ordered_globs("tests/bar.py", ["src/**"]) is False


def test_match_ordered_globs_negated_glob() -> None:
    """Test match_ordered_globs with negated glob."""
    # Include src/** but exclude src/foo.py
    globs = ["src/**", "!src/foo.py"]
    assert match_ordered_globs("src/bar.py", globs) is True
    assert match_ordered_globs("src/foo.py", globs) is False


def test_match_ordered_globs_negated_only() -> None:
    """Test match_ordered_globs with only negated globs includes everything except negated."""
    globs = ["!tests/**"]
    assert match_ordered_globs("src/foo.py", globs) is True
    assert match_ordered_globs("tests/bar.py", globs) is False


def test_match_ordered_globs_last_match_wins() -> None:
    """Test match_ordered_globs where last matching glob wins."""
    globs = ["src/**", "!src/foo.py", "src/foo.py"]
    assert match_ordered_globs("src/foo.py", globs) is True


def test_compile_gitwildmatch_returns_pathspec() -> None:
    """Test compile_gitwildmatch returns a PathSpec that matches."""
    patterns = ["*.py", "!test_*.py"]
    spec = compile_gitwildmatch(patterns)
    assert spec.match_file("foo.py") is True
    assert spec.match_file("test_foo.py") is False
