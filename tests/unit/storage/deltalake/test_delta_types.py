"""Tests for DeltaLake type export surface."""

from __future__ import annotations

from storage.deltalake import delta_types


def test_delta_types_exports() -> None:
    """Delta type module should expose expected request/response contracts."""
    assert hasattr(delta_types, "DeltaWriteResult")
    assert hasattr(delta_types, "DeltaReadRequest")
