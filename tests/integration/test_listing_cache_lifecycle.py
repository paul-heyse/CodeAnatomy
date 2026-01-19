"""Integration tests for listing cache lifecycle behavior."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datafusion_engine.registry_bridge import register_dataset_df
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.registry import DatasetLocation
from obs.diagnostics import DiagnosticsCollector
from schema_spec.system import DataFusionScanOptions

pytest.importorskip("datafusion")


def _write_parquet(path: Path) -> None:
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    pq.write_table(table, path)


@pytest.mark.integration
def test_listing_refresh_records_event(tmp_path: Path) -> None:
    """Record refresh events when listing tables are mutable."""
    parquet_path = tmp_path / "events.parquet"
    _write_parquet(parquet_path)
    sink = DiagnosticsCollector()
    profile = DataFusionRuntimeProfile(diagnostics_sink=sink)
    ctx = profile.session_context()
    scan = DataFusionScanOptions(listing_mutable=True, list_files_cache_ttl="1s")
    location = DatasetLocation(
        path=str(parquet_path),
        format="parquet",
        datafusion_scan=scan,
        datafusion_provider="listing",
    )
    register_dataset_df(
        ctx,
        name="events",
        location=location,
        runtime_profile=profile,
    )
    register_dataset_df(
        ctx,
        name="events",
        location=location,
        runtime_profile=profile,
    )
    refreshes = sink.artifacts_snapshot().get("datafusion_listing_refresh_v1", [])
    assert refreshes
    assert refreshes[-1].get("name") == "events"
