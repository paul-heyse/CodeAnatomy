"""Integration tests for unbounded table registrations."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datafusion_engine.dataset_registry import DatasetLocation
from datafusion_engine.registry_bridge import register_dataset_df
from datafusion_engine.runtime import DataFusionRuntimeProfile
from schema_spec.system import DataFusionScanOptions

pytest.importorskip("datafusion")

EXPECTED_ROWS = 2


def _write_parquet(path: Path) -> None:
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    pq.write_table(table, path)


@pytest.mark.integration
def test_unbounded_external_table_read(tmp_path: Path) -> None:
    """Register and read from unbounded external tables when supported.

    Raises
    ------
    ValueError
        Raised when unbounded external tables are unsupported.
    """
    parquet_path = tmp_path / "events.parquet"
    _write_parquet(parquet_path)
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    scan = DataFusionScanOptions(unbounded=True)
    location = DatasetLocation(
        path=str(parquet_path),
        format="parquet",
        datafusion_scan=scan,
        datafusion_provider="listing",
    )
    try:
        df = register_dataset_df(
            ctx,
            name="events_unbounded",
            location=location,
            runtime_profile=profile,
        )
    except ValueError as exc:
        message = str(exc).lower()
        if "unbounded" in message or "not supported" in message:
            pytest.skip("Unbounded external tables are not supported.")
        raise
    result = df.to_arrow_table()
    assert result.num_rows == EXPECTED_ROWS
