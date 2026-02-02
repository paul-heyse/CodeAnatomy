"""Integration tests for listing cache lifecycle behavior."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datafusion_engine.dataset.registration import (
    DatasetRegistrationOptions,
    register_dataset_df,
)
from datafusion_engine.dataset.registry import DatasetLocation
from schema_spec.system import DataFusionScanOptions
from tests.test_helpers.diagnostics import diagnostic_profile
from tests.test_helpers.optional_deps import require_datafusion_udfs

require_datafusion_udfs()


def _write_parquet(path: Path) -> None:
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    pq.write_table(table, path)


@pytest.mark.integration
def test_listing_refresh_records_event(tmp_path: Path) -> None:
    """Record refresh events when listing tables are mutable."""
    parquet_path = tmp_path / "events.parquet"
    _write_parquet(parquet_path)
    profile, sink = diagnostic_profile()
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
        options=DatasetRegistrationOptions(runtime_profile=profile),
    )
    register_dataset_df(
        ctx,
        name="events",
        location=location,
        options=DatasetRegistrationOptions(runtime_profile=profile),
    )
    refreshes = sink.artifacts_snapshot().get("datafusion_listing_refresh_v1", [])
    assert refreshes
    assert refreshes[-1].get("name") == "events"
