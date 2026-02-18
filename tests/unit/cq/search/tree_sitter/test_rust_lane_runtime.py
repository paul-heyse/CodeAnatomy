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


def test_rust_lane_required_payload_keys_guard() -> None:
    """Required payload-key guard accepts complete payload and rejects missing keys."""
    try:
        from tools.cq.search.tree_sitter.rust_lane import runtime_engine as runtime_module
    except ImportError as exc:
        pytest.skip(f"rust runtime import unavailable in isolated scope: {exc}")

    assert_required_payload_keys = runtime_module.__dict__["_assert_required_payload_keys"]
    assert_required_payload_keys(
        {
            "language": "rust",
            "enrichment_status": "applied",
            "enrichment_sources": ["tree_sitter"],
        }
    )
    with pytest.raises(ValueError, match="missing required keys"):
        assert_required_payload_keys({"language": "rust"})
