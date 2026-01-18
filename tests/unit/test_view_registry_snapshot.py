"""Tests for DataFusion view registry snapshots."""

from __future__ import annotations

from datafusion_engine.runtime import DataFusionRuntimeProfile


def test_view_registry_snapshot_stable_for_repeated_registration() -> None:
    """Keep view registry snapshots stable across repeated registrations."""
    profile = DataFusionRuntimeProfile()
    registry = profile.view_registry
    assert registry is not None
    registry.record(name="alpha_view", sql="SELECT 1")
    registry.record(name="beta_view", sql="SELECT 2")
    first_snapshot = registry.snapshot()
    registry.record(name="alpha_view", sql="SELECT 1")
    second_snapshot = registry.snapshot()
    assert first_snapshot == second_snapshot
