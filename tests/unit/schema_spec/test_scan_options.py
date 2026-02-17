"""Unit tests for scan_options module."""

from __future__ import annotations

from schema_spec.scan_options import DataFusionScanOptions


def test_datafusion_scan_options_defaults() -> None:
    """Scan options can be constructed with default values."""
    options = DataFusionScanOptions()
    assert options is not None
