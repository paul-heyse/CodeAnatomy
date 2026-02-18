"""Unit tests for Delta write IPC payload builders."""

from __future__ import annotations

import msgspec
import pyarrow as pa
import pytest

from datafusion_engine.delta.write_ipc_payload import (
    DeltaWriteRequestOptions,
    build_delta_write_request,
    reader_to_raw_ipc,
    table_to_ipc_stream_bytes,
)


def test_table_to_ipc_stream_bytes_roundtrip() -> None:
    """Round-trip IPC payloads back into equivalent Arrow tables."""
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    payload = table_to_ipc_stream_bytes(table)
    assert payload
    reader = pa.ipc.open_stream(payload)
    decoded = reader.read_all()
    assert decoded.equals(table)


def test_build_delta_write_request_accepts_reader_payload() -> None:
    """Reader payloads can build IPC requests without table materialization."""
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    request = build_delta_write_request(
        table_uri="/tmp/delta",
        reader=reader,
        options=DeltaWriteRequestOptions(mode="append", schema_mode="merge"),
    )
    assert request.table_uri == "/tmp/delta"
    assert request.mode == "append"
    assert request.schema_mode == "merge"
    assert request.data_ipc


def test_reader_to_raw_ipc_roundtrip() -> None:
    """Reader payload preserves Arrow rows through IPC stream encoding."""
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    raw = reader_to_raw_ipc(reader)
    ipc_bytes = msgspec.msgpack.decode(raw, type=bytes)
    decoded = pa.ipc.open_stream(ipc_bytes).read_all()
    assert decoded.equals(table)


def test_build_delta_write_request_requires_payload() -> None:
    """Builder raises when neither table nor reader payload is provided."""
    with pytest.raises(ValueError, match="requires either table or reader"):
        _ = build_delta_write_request(
            table_uri="/tmp/delta",
            options=DeltaWriteRequestOptions(mode="append", schema_mode=None),
        )
