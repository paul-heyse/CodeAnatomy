"""Tests for query package public import surface."""

from __future__ import annotations

import tools.cq.query as query_pkg


def test_query_package_exposes_core_ir_and_parser_symbols() -> None:
    """Verify public query package exports core parser interfaces."""
    assert hasattr(query_pkg, "Query")
    assert callable(query_pkg.parse_query)


def test_query_package_does_not_export_runtime_facade_modules() -> None:
    """Verify runtime facade modules remain outside the explicit public API."""
    public_api = set(getattr(query_pkg, "__all__", ()))
    assert "executor_runtime" not in public_api
    assert "executor_entity" not in public_api
    assert "executor_pattern" not in public_api
