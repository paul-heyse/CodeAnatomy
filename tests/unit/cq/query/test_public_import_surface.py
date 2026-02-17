"""Tests for query package public import surface."""

from __future__ import annotations

import tools.cq.query as query_pkg


def test_query_package_exposes_core_ir_and_parser_symbols() -> None:
    """Verify public query package exports core parser interfaces."""
    assert hasattr(query_pkg, "Query")
    assert callable(query_pkg.parse_query)


def test_query_package_does_not_export_runtime_facade_modules() -> None:
    """Verify runtime facade modules remain internal-only imports."""
    assert not hasattr(query_pkg, "executor_runtime")
    assert not hasattr(query_pkg, "executor_entity")
    assert not hasattr(query_pkg, "executor_pattern")
