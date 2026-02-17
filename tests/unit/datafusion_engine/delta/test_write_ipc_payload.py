"""Unit tests for Delta write IPC payload builders."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.delta.write_ipc_payload import table_to_ipc_stream_bytes


def test_table_to_ipc_stream_bytes_roundtrip() -> None:
    """Round-trip IPC payloads back into equivalent Arrow tables."""
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    payload = table_to_ipc_stream_bytes(table)
    assert payload
    reader = pa.ipc.open_stream(payload)
    decoded = reader.read_all()
    assert decoded.equals(table)
