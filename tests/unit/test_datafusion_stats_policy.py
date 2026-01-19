"""Unit tests for DataFusion stats policies."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datafusion_engine.registry_bridge import register_dataset_df
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.registry import DatasetLocation
from schema_spec.system import DataFusionScanOptions

pytest.importorskip("datafusion")


def _write_parquet(path: Path) -> None:
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    pq.write_table(table, path)


def test_scan_policy_applies_stats_settings(tmp_path: Path) -> None:
    """Apply stats settings before registering external datasets."""
    parquet_path = tmp_path / "events.parquet"
    _write_parquet(parquet_path)
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    scan = DataFusionScanOptions(
        collect_statistics=False,
        meta_fetch_concurrency=8,
        listing_mutable=True,
    )
    register_dataset_df(
        ctx,
        name="events",
        location=DatasetLocation(
            path=str(parquet_path),
            format="parquet",
            datafusion_scan=scan,
            datafusion_provider="listing",
        ),
        runtime_profile=profile,
    )
    settings = profile.settings_snapshot(ctx).to_pydict()
    values = dict(zip(settings.get("name", []), settings.get("value", []), strict=False))
    assert values.get("datafusion.execution.collect_statistics") == "false"
    assert values.get("datafusion.execution.meta_fetch_concurrency") == "8"
