"""Tests for repo scan filtering and git-backed listings."""

from __future__ import annotations

from pathlib import Path

import pytest

from extract.repo_scan_pygit2 import iter_repo_candidate_paths, repo_status_paths
from extract.repo_scope import RepoScopeOptions, resolve_repo_scope, scope_rule_lines
from extract.scope_manifest import ScopeManifestOptions, build_scope_manifest
from extract.scope_rules import build_scope_rules, check_scope_path

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
    options = RepoScopeOptions(
        include_globs=("**/*.py",),
        exclude_globs=(),
        follow_symlinks=False,
    )
    scope = resolve_repo_scope(tmp_path, options)
    include_lines, exclude_lines = scope_rule_lines(scope, options)
    rules = build_scope_rules(include_lines=include_lines, exclude_lines=exclude_lines)
    status_flags = repo_status_paths(scope)
    manifest = build_scope_manifest(
        iter_repo_candidate_paths(scope),
        options=ScopeManifestOptions(
            repo=scope.repo,
            rules=rules,
            scope_kind="repo",
            repo_root=scope.repo_root,
            status_flags=status_flags,
        ),
    )
    found = {entry.path for entry in manifest.entries if entry.included}
    assert found == {"tracked.py", "untracked.py", "nested/keep.py"}


def test_repo_scan_fs_listing(tmp_path: Path) -> None:
    """Ensure scope rules exclude ignored globs."""
    _init_repo(tmp_path)
    rules = build_scope_rules(
        include_lines=["**/*.py"],
        exclude_lines=["ignored.py", "ignored_dir/**"],
    )
    ignored = check_scope_path("ignored.py", rules)
    assert not ignored.include


def test_pathspec_check_records_ignore_index(tmp_path: Path) -> None:
    """Ensure ignore checks surface a matching index for ignored paths."""
    _init_repo(tmp_path)
    rules = build_scope_rules(
        include_lines=["**/*.py"],
        exclude_lines=["ignored.py", "ignored_dir/**"],
    )
    result = check_scope_path("ignored.py", rules)
    assert not result.include
    assert result.exclude_index is not None
