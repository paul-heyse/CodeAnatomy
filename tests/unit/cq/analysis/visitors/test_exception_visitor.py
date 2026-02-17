"""Tests for exception-analysis visitor behavior."""

from __future__ import annotations

import ast

from tools.cq.analysis.visitors.exception_visitor import ExceptionVisitor


def _raise_site(**kwargs: object) -> dict[str, object]:
    return dict(kwargs)


def _catch_site(**kwargs: object) -> dict[str, object]:
    return dict(kwargs)


def test_exception_visitor_collects_raise_and_catch() -> None:
    """Verify visitor records both raise sites and catch sites from a try block."""
    tree = ast.parse(
        """
def f():
    try:
        raise ValueError('bad')
    except ValueError:
        pass
"""
    )
    visitor = ExceptionVisitor(
        "a.py",
        make_raise_site=_raise_site,
        make_catch_site=_catch_site,
        safe_unparse=ast.unparse,
    )
    visitor.visit(tree)

    assert visitor.raises
    assert visitor.catches
