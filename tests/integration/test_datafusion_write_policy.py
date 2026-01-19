"""Integration tests for DataFusion write policy handling."""

from __future__ import annotations

from pathlib import Path

import pytest

from datafusion_engine.bridge import datafusion_write_options, datafusion_write_parquet
from datafusion_engine.runtime import DataFusionRuntimeProfile
from schema_spec.policies import DataFusionWritePolicy

datafusion = pytest.importorskip("datafusion")


@pytest.mark.integration
def test_datafusion_write_policy_defaults() -> None:
    """Construct DataFusion write options from defaults."""
    policy = DataFusionWritePolicy()
    write_options, parquet_options = datafusion_write_options(policy)
    assert isinstance(write_options, datafusion.DataFrameWriteOptions)
    assert isinstance(parquet_options, datafusion.ParquetWriterOptions)
    assert policy.payload()["partition_by"] == []


@pytest.mark.integration
def test_datafusion_write_parquet_payload(tmp_path: Path) -> None:
    """Write DataFusion outputs using the write policy."""
    ctx = DataFusionRuntimeProfile().session_context()
    df = ctx.sql("SELECT 1 AS id")
    policy = DataFusionWritePolicy(
        partition_by=("id",),
        single_file_output=False,
        parquet_compression="zstd(5)",
        parquet_statistics_enabled="page",
    )
    out_dir = tmp_path / "out_dir"
    payload = datafusion_write_parquet(df, path=str(out_dir), policy=policy)
    assert out_dir.exists()
    assert payload["write_policy"] == policy.payload()
