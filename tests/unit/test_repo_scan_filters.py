"""Tests for repo scan filtering and git-backed listings."""

from __future__ import annotations

from pathlib import Path

import pytest

from extract.pathspec_filters import build_repo_scan_pathspec, check_repo_path
from extract.repo_scan_fs import iter_repo_files_fs
from extract.repo_scan_pygit2 import iter_repo_files_pygit2

pygit2 = pytest.importorskip("pygit2")


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _init_repo(tmp_path: Path) -> object:
    repo = pygit2.init_repository(tmp_path, bare=False)
    _write_file(tmp_path / ".gitignore", "ignored.py\nignored_dir/\n")
    _write_file(tmp_path / "tracked.py", 'print("tracked")\n')
    _write_file(tmp_path / "untracked.py", 'print("untracked")\n')
    _write_file(tmp_path / "ignored.py", 'print("ignored")\n')
    _write_file(tmp_path / "nested" / "keep.py", 'print("nested")\n')
    _write_file(tmp_path / "ignored_dir" / "skip.py", 'print("skip")\n')
    repo.index.add("tracked.py")
    repo.index.add("nested/keep.py")
    repo.index.write()
    return repo


def test_repo_scan_pygit2_listing(tmp_path: Path) -> None:
    """Ensure pygit2 listing respects gitignore and includes untracked."""
    _init_repo(tmp_path)
    paths = iter_repo_files_pygit2(
        tmp_path,
        include_globs=("**/*.py",),
        exclude_globs=(),
        exclude_dirs=(),
        follow_symlinks=False,
    )
    assert paths is not None
    found = {path.as_posix() for path in paths}
    assert found == {"tracked.py", "untracked.py", "nested/keep.py"}


def test_repo_scan_fs_listing(tmp_path: Path) -> None:
    """Ensure filesystem listing respects gitignore when available."""
    _init_repo(tmp_path)
    paths = list(
        iter_repo_files_fs(
            tmp_path,
            include_globs=("**/*.py",),
            exclude_globs=(),
            exclude_dirs=(),
            follow_symlinks=False,
        )
    )
    found = {path.as_posix() for path in paths}
    assert found == {"tracked.py", "untracked.py", "nested/keep.py"}


def test_pathspec_check_records_ignore_index(tmp_path: Path) -> None:
    """Ensure ignore checks surface a matching index for ignored paths."""
    _init_repo(tmp_path)
    filters = build_repo_scan_pathspec(
        tmp_path,
        include_globs=("**/*.py",),
        exclude_globs=(),
        exclude_dirs=(),
    )
    result = check_repo_path(Path("ignored.py"), filters=filters, allow_ignored=False)
    assert not result.include
    assert result.ignored
    assert result.ignore_index is not None
