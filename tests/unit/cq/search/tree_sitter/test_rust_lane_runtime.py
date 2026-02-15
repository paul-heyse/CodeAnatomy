"""Tests for rust lane runtime wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.rust_lane import runtime as runtime_module


def test_rust_lane_runtime_availability_symbol_present() -> None:
    assert hasattr(runtime_module, "is_tree_sitter_rust_available")
