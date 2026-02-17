"""Unit tests for scan_policy module."""

from __future__ import annotations

from schema_spec.scan_options import DataFusionScanOptions
from schema_spec.scan_policy import ScanPolicyConfig, apply_scan_policy


def test_apply_scan_policy_returns_options() -> None:
    """Applying default scan policy returns a scan options object."""
    policy = ScanPolicyConfig()
    options = apply_scan_policy(
        options=DataFusionScanOptions(),
        policy=policy,
        dataset_format="parquet",
    )
    assert options is not None
