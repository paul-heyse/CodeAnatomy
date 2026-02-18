"""Arrow IPC payload builders for Rust Delta write requests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import msgspec
import pyarrow as pa

from datafusion_engine.delta.control_plane_core import DeltaCommitOptions, DeltaWriteRequest


class DeltaWriteRequestOptions(msgspec.Struct, frozen=True):
    """Normalized options for building a Delta write request payload."""

    mode: str
    schema_mode: str | None
    storage_options: Mapping[str, str] | None = None
    partition_columns: Sequence[str] | None = None
    target_file_size: int | None = None
    extra_constraints: Sequence[str] | None = None
    commit_options: DeltaCommitOptions | None = None


def table_to_ipc_stream_bytes(table: pa.Table) -> bytes:
    """Encode an Arrow table as stream-format IPC bytes.

    Returns:
    -------
    bytes
        Arrow IPC stream bytes for the provided table.
    """
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def table_to_raw_ipc(table: pa.Table) -> msgspec.Raw:
    """Encode an Arrow table to msgspec.Raw IPC payload.

    Returns:
        msgspec.Raw: Arrow IPC stream payload.
    """
    # DeltaWriteRequest.data_ipc is msgpack-embedded raw data, so the IPC bytes
    # must be msgpack-encoded before wrapping in msgspec.Raw.
    return msgspec.Raw(msgspec.msgpack.encode(table_to_ipc_stream_bytes(table)))


def reader_to_raw_ipc(reader: pa.RecordBatchReader) -> msgspec.Raw:
    """Encode a RecordBatchReader to msgspec.Raw IPC payload.

    Returns:
        msgspec.Raw: Arrow IPC stream payload.
    """
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, reader.schema) as writer:
        for batch in reader:
            writer.write_batch(batch)
    return msgspec.Raw(msgspec.msgpack.encode(sink.getvalue().to_pybytes()))


def build_delta_write_request(
    *,
    table_uri: str,
    table: pa.Table | None = None,
    reader: pa.RecordBatchReader | None = None,
    options: DeltaWriteRequestOptions,
) -> DeltaWriteRequest:
    """Build a control-plane DeltaWriteRequest from an Arrow table payload.

    Returns:
        DeltaWriteRequest: Normalized Delta write request.
    """
    if table is None and reader is None:
        msg = "build_delta_write_request requires either table or reader payload."
        raise ValueError(msg)
    data_ipc = table_to_raw_ipc(table) if table is not None else reader_to_raw_ipc(reader)
    return DeltaWriteRequest(
        table_uri=table_uri,
        storage_options=options.storage_options,
        version=None,
        timestamp=None,
        data_ipc=data_ipc,
        mode=options.mode,
        schema_mode=options.schema_mode,
        partition_columns=options.partition_columns,
        target_file_size=options.target_file_size,
        extra_constraints=options.extra_constraints,
        commit_options=options.commit_options,
    )


__all__ = [
    "DeltaWriteRequestOptions",
    "build_delta_write_request",
    "reader_to_raw_ipc",
    "table_to_ipc_stream_bytes",
    "table_to_raw_ipc",
]
