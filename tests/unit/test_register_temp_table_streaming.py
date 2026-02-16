"""Tests for test_register_temp_table_streaming."""

from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.session.helpers import deregister_table, register_temp_table


def test_register_temp_table_streaming_reader() -> None:
    """Register and deregister a streaming temporary table in one pass."""
    ctx = SessionContext()
    schema = pa.schema([("value", pa.int64())])
    batch = pa.record_batch([pa.array([1, 2, 3])], schema=schema)
    reader = pa.RecordBatchReader.from_batches(schema, [batch])

    name = register_temp_table(ctx, reader, prefix="__test_stream_")
    assert ctx.table_exist(name)
    deregister_table(ctx, name)
