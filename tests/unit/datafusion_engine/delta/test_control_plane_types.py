"""Tests for Delta control-plane type exports."""

from __future__ import annotations

from datafusion_engine.delta import control_plane_types


def test_control_plane_types_exports() -> None:
    """Module should expose control-plane request/response contracts."""
    assert hasattr(control_plane_types, "DeltaWriteRequest")
    assert hasattr(control_plane_types, "DeltaMergeRequest")
