"""Tests for run scope merge helpers."""

from __future__ import annotations

from tools.cq.query.parser import parse_query
from tools.cq.run.scope import apply_run_scope, merge_excludes


def test_merge_excludes_deduplicates_with_stable_order() -> None:
    """Merged excludes should preserve first-seen order with deduplication."""
    assert merge_excludes(("a", "b"), ("b", "c")) == ["a", "b", "c"]


def test_apply_run_scope_merges_plan_scope_into_query_scope() -> None:
    """Run-level scope overlays should compose onto parsed query scope."""
    query = parse_query("entity=function in=src")
    scoped = apply_run_scope(query, "tools", ("**/tests/**",))

    assert scoped.scope.in_dir == "tools/src"
    assert scoped.scope.exclude == ("**/tests/**",)
