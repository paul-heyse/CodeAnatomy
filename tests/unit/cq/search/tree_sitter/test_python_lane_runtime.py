"""Tests for python lane runtime wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.python_lane import runtime as runtime_module


def test_python_lane_runtime_availability_symbol_present() -> None:
    """Test python lane runtime availability symbol present."""
    assert hasattr(runtime_module, "is_tree_sitter_python_available")
