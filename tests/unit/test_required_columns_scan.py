"""Unit tests for required column scan behavior."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from arrowdsl.core.scan_telemetry import ScanTelemetryOptions, fragment_telemetry
from sqlglot_tools.lineage import required_columns_by_table

ibis = pytest.importorskip("ibis")


def test_required_columns_include_filter_fields() -> None:
    """Include predicate-only columns in required column mapping."""
    backend = ibis.duckdb.connect()
    expr = ibis.table(schema={"a": "int64", "b": "int64"}, name="events")
    filtered = expr.filter(expr.b > 0).select(expr.a)
    required = required_columns_by_table(filtered, backend=backend)
    assert set(required.get("events", ())) == {"a", "b"}


def test_scan_telemetry_records_required_vs_scanned() -> None:
    """Record required vs scanned columns for scan telemetry."""
    table = pa.table({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
    dataset = ds.dataset(table)
    scan_columns = ("a",)
    scanner = dataset.scanner(columns=scan_columns)
    telemetry = fragment_telemetry(
        dataset,
        scanner=scanner,
        options=ScanTelemetryOptions(
            required_columns=("a",),
            scan_columns=scan_columns,
        ),
    )
    assert telemetry.required_columns == ("a",)
    assert telemetry.scan_columns == scan_columns
