"""Streaming write checks for Ibis and DataFusion paths."""

from __future__ import annotations

from pathlib import Path

import duckdb
import ibis
import pyarrow as pa
import pytest

from datafusion_engine.bridge import datafusion_to_reader
from datafusion_engine.runtime import DataFusionRuntimeProfile, read_delta_table_from_path
from ibis_engine.io_bridge import IbisDatasetWriteOptions, write_ibis_dataset_delta
from ibis_engine.plan import IbisPlan

EXPECTED_ROW_COUNT = 2


@pytest.mark.integration
def test_ibis_streaming_dataset_write(tmp_path: Path) -> None:
    """Stream Ibis batches into a dataset write."""
    data = pa.table({"entity_id": [1, 2], "name": ["alpha", "beta"]})
    backend = ibis.duckdb.from_connection(duckdb.connect(database=":memory:"))
    backend.create_table("input_table", data, overwrite=True)
    plan = IbisPlan(expr=backend.table("input_table"))
    output_dir = tmp_path / "ibis_stream"
    result = write_ibis_dataset_delta(
        plan,
        output_dir,
        options=IbisDatasetWriteOptions(
            execution=None,
            prefer_reader=True,
        ),
    )
    table = read_delta_table_from_path(result.path)
    assert table.num_rows == EXPECTED_ROW_COUNT


@pytest.mark.integration
def test_datafusion_streaming_dataset_write(tmp_path: Path) -> None:
    """Stream DataFusion batches into a dataset write."""
    ctx = DataFusionRuntimeProfile().session_context()
    table = pa.table({"entity_id": [2, 1], "name": ["beta", "alpha"]})
    ctx.register_record_batches("input_table", [list(table.to_batches())])
    df = ctx.sql("select * from input_table")
    reader = datafusion_to_reader(df)
    output_dir = tmp_path / "df_stream"
    result = write_ibis_dataset_delta(
        reader,
        output_dir,
        options=IbisDatasetWriteOptions(prefer_reader=True),
    )
    table = read_delta_table_from_path(result.path)
    assert table.num_rows == EXPECTED_ROW_COUNT
