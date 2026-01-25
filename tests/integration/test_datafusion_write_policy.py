"""Integration tests for DataFusion write policy handling."""

from __future__ import annotations

from pathlib import Path

import pytest

from datafusion_engine.bridge import datafusion_write_options
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.sql_policy_engine import SQLPolicyProfile
from datafusion_engine.write_pipeline import (
    WriteFormat,
    WriteMode,
    WritePipeline,
    WriteRequest,
    parquet_policy_from_datafusion,
)
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
    policy = DataFusionWritePolicy(
        partition_by=("id",),
        single_file_output=False,
        parquet_compression="zstd",
        parquet_statistics_enabled="page",
    )
    parquet_policy = parquet_policy_from_datafusion(policy)
    assert parquet_policy is not None
    out_dir = tmp_path / "out_dir"
    request = WriteRequest(
        source="SELECT 1 AS id",
        destination=str(out_dir),
        format=WriteFormat.PARQUET,
        mode=WriteMode.OVERWRITE,
        partition_by=tuple(policy.partition_by),
        parquet_policy=parquet_policy,
        single_file_output=policy.single_file_output,
    )
    pipeline = WritePipeline(ctx, SQLPolicyProfile())
    result = pipeline.write(request, prefer_streaming=True)
    assert out_dir.exists()
    assert result.request.parquet_policy is parquet_policy
    assert parquet_policy.compression == "zstd"
    assert parquet_policy.statistics_enabled == "page"
