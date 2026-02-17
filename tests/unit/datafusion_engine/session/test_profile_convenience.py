# ruff: noqa: D103
"""Tests for DataFusion profile/location convenience accessors."""

from __future__ import annotations

from datafusion_engine.dataset.registry import DatasetLocation


def test_dataset_location_delta_convenience_defaults() -> None:
    location = DatasetLocation(path="/tmp/delta", format="delta")
    assert location.delta_scan is None
    assert location.delta_write_policy is None
    assert location.delta_schema_policy is None
