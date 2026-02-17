"""Tests for side-effect-analysis visitor behavior."""

from __future__ import annotations

import ast

from tools.cq.analysis.visitors.side_effect_visitor import SideEffectVisitor


def _side_effect(**kwargs: object) -> dict[str, object]:
    return dict(kwargs)


def _is_main_guard(node: ast.If) -> bool:
    return isinstance(node.test, ast.Constant) and node.test.value is True


def test_side_effect_visitor_collects_top_level_call() -> None:
    """Verify visitor records an unsafe top-level call as a side effect."""
    tree = ast.parse("print('x')\n")
    visitor = SideEffectVisitor(
        "a.py",
        make_side_effect=_side_effect,
        safe_unparse=ast.unparse,
        safe_top_level=set(),
        ambient_patterns={},
        is_main_guard=_is_main_guard,
    )
    visitor.visit(tree)
    assert visitor.effects
    assert visitor.effects[0]["kind"] == "top_level_call"
