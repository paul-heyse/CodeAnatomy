"""Streaming write checks for Ibis and DataFusion paths."""

from __future__ import annotations

from pathlib import Path

import duckdb
import ibis
import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from datafusion import SessionContext

from datafusion_engine.bridge import datafusion_to_reader
from ibis_engine.io_bridge import IbisDatasetWriteOptions, write_ibis_dataset_parquet
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
    path = write_ibis_dataset_parquet(
        plan,
        output_dir,
        options=IbisDatasetWriteOptions(
            execution=None,
            prefer_reader=True,
        ),
    )
    dataset = ds.dataset(path, format="parquet")
    assert dataset.count_rows() == EXPECTED_ROW_COUNT


@pytest.mark.integration
def test_datafusion_streaming_dataset_write(tmp_path: Path) -> None:
    """Stream DataFusion batches into a dataset write."""
    ctx = SessionContext()
    table = pa.table({"entity_id": [2, 1], "name": ["beta", "alpha"]})
    ctx.register_record_batches("input_table", [table.to_batches()])
    df = ctx.sql("select * from input_table")
    reader = datafusion_to_reader(df)
    output_dir = tmp_path / "df_stream"
    path = write_ibis_dataset_parquet(
        reader,
        output_dir,
        options=IbisDatasetWriteOptions(prefer_reader=True),
    )
    dataset = ds.dataset(path, format="parquet")
    assert dataset.count_rows() == EXPECTED_ROW_COUNT
