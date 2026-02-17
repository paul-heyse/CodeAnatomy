"""Tests for run scope merge helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.query.parser import parse_query
from tools.cq.run.scope import apply_in_dir_scope, apply_run_scope, merge_excludes


def test_merge_excludes_deduplicates_with_stable_order() -> None:
    """Merged excludes should preserve first-seen order with deduplication."""
    assert merge_excludes(("a", "b"), ("b", "c")) == ["a", "b", "c"]


def test_apply_run_scope_merges_plan_scope_into_query_scope() -> None:
    """Run-level scope overlays should compose onto parsed query scope."""
    query = parse_query("entity=function in=src")
    scoped = apply_run_scope(query, "tools", ("**/tests/**",))

    assert scoped.scope.in_dir == "tools/src"
    assert scoped.scope.exclude == ("**/tests/**",)


def test_apply_in_dir_scope_returns_recursive_glob_for_directory() -> None:
    """Directory scopes resolve to recursive glob patterns."""
    assert apply_in_dir_scope("tools/cq", Path()) == ["tools/cq/**"]


def test_apply_in_dir_scope_preserves_explicit_file_path() -> None:
    """File scopes are preserved without converting to directory globs."""
    assert apply_in_dir_scope("tools/cq/run/scope.py", Path()) == ["tools/cq/run/scope.py"]
