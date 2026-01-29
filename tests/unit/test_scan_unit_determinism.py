"""Tests for deterministic scan-unit ordering in dynamic execution."""

from __future__ import annotations

import pyarrow as pa

from hamilton_pipeline.modules.task_execution import scan_unit_results_by_key__dynamic


def test_scan_unit_results_by_key_sorted() -> None:
    """Ensure scan-unit mappings are deterministically ordered by key."""
    table = pa.table({"col": [1]})
    items = [("scan_b", table), ("scan_a", table)]
    mapping = scan_unit_results_by_key__dynamic(items)
    assert list(mapping.keys()) == ["scan_a", "scan_b"]
