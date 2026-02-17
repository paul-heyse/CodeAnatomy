"""Tests for Delta control-plane core invariants."""

from __future__ import annotations

from datafusion_engine.delta import control_plane_core


def test_control_plane_core_exports_provider_request() -> None:
    """Control-plane core exports Delta provider request contract."""
    assert hasattr(control_plane_core, "DeltaProviderRequest")
