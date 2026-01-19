"""Integration tests for DataFusion table provider registry snapshots."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.registry_bridge import register_dataset_df
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.registry import DatasetLocation
from obs.diagnostics import DiagnosticsCollector

pytest.importorskip("datafusion")
deltalake = pytest.importorskip("deltalake")

EXPECTED_ROW_COUNT = 2


@pytest.mark.integration
def test_table_provider_registry_records_delta_capsule(tmp_path: Path) -> None:
    """Record table provider capabilities for Delta-backed tables."""
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    delta_path = tmp_path / "delta_table"
    deltalake.write_deltalake(str(delta_path), table)

    sink = DiagnosticsCollector()
    profile = DataFusionRuntimeProfile(diagnostics_sink=sink)
    ctx = profile.session_context()
    register_dataset_df(
        ctx,
        name="delta_tbl",
        location=DatasetLocation(path=str(delta_path), format="delta"),
        runtime_profile=profile,
    )
    df = ctx.sql("SELECT COUNT(*) AS row_count FROM delta_tbl")
    result = df.to_arrow_table()
    assert result.column("row_count")[0].as_py() == EXPECTED_ROW_COUNT
    artifacts = sink.artifacts_snapshot().get("datafusion_table_providers_v1", [])
    assert artifacts
    assert any(entry.get("name") == "delta_tbl" for entry in artifacts)
