"""Tests for tree-sitter query planner wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.query import planner as planner_module


def test_query_planner_exports_functions() -> None:
    assert callable(planner_module.build_pack_plan)
    assert callable(planner_module.build_pattern_plan)
    assert callable(planner_module.sort_pack_plans)
