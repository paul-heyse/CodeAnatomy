"""Unit tests for required column scan behavior."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from arrowdsl.core.context import execution_context_factory
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.plan.scan_builder import ScanBuildSpec
from arrowdsl.plan.scan_telemetry import ScanTelemetryOptions, fragment_telemetry
from ibis_engine.lineage import required_columns_by_table

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
    ctx = execution_context_factory("default")
    table = pa.table({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
    dataset = ds.dataset(table)
    query = QuerySpec(projection=ProjectionSpec(base=("a", "b", "c")))
    scan_spec = ScanBuildSpec(
        dataset=dataset,
        query=query,
        ctx=ctx,
        required_columns=("a",),
    )
    scan_columns = scan_spec.scan_columns()
    assert list(scan_columns) == ["a"]
    telemetry = fragment_telemetry(
        dataset,
        scanner=scan_spec.scanner(),
        options=ScanTelemetryOptions(
            required_columns=("a",),
            scan_columns=("a",),
        ),
    )
    assert telemetry.required_columns == ("a",)
    assert telemetry.scan_columns == ("a",)
