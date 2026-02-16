"""Tests for semantic diagnostics wrappers."""

from __future__ import annotations

from tools.cq.search.semantic import diagnostics as diagnostics_module


def test_capability_diagnostics_wrapper_returns_list() -> None:
    """Test capability diagnostics wrapper returns list."""
    rows = diagnostics_module.capability_diagnostics(features=["search"], lang_scope="auto")
    assert isinstance(rows, list)
