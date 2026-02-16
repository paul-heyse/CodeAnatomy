"""Tests for rust lane runtime wrappers."""

from __future__ import annotations

import pytest


def test_rust_lane_runtime_availability_symbol_present() -> None:
    """Test rust lane runtime availability symbol present."""
    try:
        from tools.cq.search.tree_sitter.rust_lane import runtime as runtime_module
    except ImportError as exc:
        pytest.skip(f"rust runtime import unavailable in isolated scope: {exc}")

    assert hasattr(runtime_module, "is_tree_sitter_rust_available")
