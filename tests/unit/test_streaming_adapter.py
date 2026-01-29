"""Unit tests for the streaming adapter."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from arrow_utils.core.streaming import to_reader
from datafusion_engine.ingest import datafusion_from_arrow
from datafusion_engine.runtime import DataFusionRuntimeProfile

datafusion = pytest.importorskip("datafusion")


class _StreamWrapper:
    def __init__(self, reader: pa.RecordBatchReader) -> None:
        self._reader = reader

    def __getattr__(self, name: str) -> object:
        if name == "__arrow_c_stream__":
            return self._reader.__arrow_c_stream__
        raise AttributeError(name)


def _simple_table() -> pa.Table:
    return pa.table({"id": [1, 2], "value": ["a", "b"]})


def test_to_reader_datafusion_dataframe() -> None:
    """Convert DataFusion DataFrames into a RecordBatchReader."""
    ctx = DataFusionRuntimeProfile().session_context()
    table = _simple_table()
    datafusion_from_arrow(ctx, name="input_table", value=table)
    df = ctx.table("input_table")
    reader = to_reader(df)
    result = reader.read_all()
    assert result.num_rows == table.num_rows


def test_to_reader_scanner() -> None:
    """Convert dataset scanners into a RecordBatchReader."""
    table = _simple_table()
    scanner = ds.dataset(table).scanner()
    reader = to_reader(scanner)
    result = reader.read_all()
    assert result.num_rows == table.num_rows


def test_to_reader_arrow_c_stream_wrapper() -> None:
    """Convert Arrow C stream providers into a RecordBatchReader."""
    table = _simple_table()
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    wrapper = _StreamWrapper(reader)
    streamed = to_reader(wrapper, schema=table.schema)
    result = streamed.read_all()
    assert result.num_rows == table.num_rows


def test_to_reader_rejects_schema_for_batches() -> None:
    """Reject schema overrides for batch-backed sources."""
    table = _simple_table()
    other_schema = pa.schema([("id", pa.int64())])
    with pytest.raises(ValueError, match="Schema negotiation is not supported"):
        _ = to_reader(table, schema=other_schema)
