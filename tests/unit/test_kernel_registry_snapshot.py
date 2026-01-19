"""Tests for Arrow kernel registry snapshots."""

from __future__ import annotations

from engine.pyarrow_registry import pyarrow_registry_snapshot


def test_registry_snapshot_payload_sorted() -> None:
    """Return sorted kernel/udf lists for repro bundles."""
    snapshot = pyarrow_registry_snapshot()
    assert snapshot["version"] == 1
    kernels = snapshot["available_kernels"]
    registered_udfs = snapshot["registered_udfs"]
    registered_kernels = snapshot["registered_kernels"]
    assert isinstance(kernels, list)
    assert isinstance(registered_udfs, list)
    assert isinstance(registered_kernels, list)
    assert kernels == sorted(kernels)
    assert registered_udfs == sorted(registered_udfs)
    assert registered_kernels == sorted(registered_kernels)
