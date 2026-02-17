"""Tests for runtime snapshot type normalization helpers."""

from __future__ import annotations

from datafusion_engine.udf.runtime_snapshot_types import (
    RuntimeInstallSnapshot,
    normalize_runtime_install_snapshot,
)

EXPECTED_CONTRACT_VERSION = 3


def test_normalize_runtime_install_snapshot_defaults() -> None:
    """Normalization should populate default runtime snapshot values."""
    snapshot = normalize_runtime_install_snapshot({"snapshot": {"scalar": []}})
    assert isinstance(snapshot, RuntimeInstallSnapshot)
    assert snapshot.contract_version == EXPECTED_CONTRACT_VERSION
    assert snapshot.runtime_install_mode == "unified"
    assert snapshot.snapshot == {"scalar": []}
