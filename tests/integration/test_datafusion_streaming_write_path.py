"""Integration checks for streaming-first Arrow boundaries in write paths."""

from __future__ import annotations

import pyarrow as pa
import pytest
from datafusion import SessionContext

from datafusion_engine.session.streaming import as_record_batch_reader


@pytest.mark.integration
def test_as_record_batch_reader_streams_dataframe_results() -> None:
    """DataFusion DataFrames should expose streaming Arrow readers by default."""
    ctx = SessionContext()
    df = ctx.from_arrow(pa.table({"id": [1, 2, 3]}), name="events")

    reader = as_record_batch_reader(df)

    assert isinstance(reader, pa.RecordBatchReader)
    table = reader.read_all()
    assert table.num_rows == 3
    assert table.column_names == ["id"]
