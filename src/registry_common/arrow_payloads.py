"""Arrow IPC helpers for payload encoding and hashing."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping

import pyarrow as pa
from pyarrow import ipc


def payload_table(payload: Mapping[str, object], schema: pa.Schema) -> pa.Table:
    """Build a single-row table for a payload.

    Parameters
    ----------
    payload:
        Mapping payload to encode.
    schema:
        Explicit schema for the payload table.

    Returns
    -------
    pyarrow.Table
        Single-row table matching the provided schema.
    """
    return pa.Table.from_pylist([dict(payload)], schema=schema)


def ipc_bytes(table: pa.Table) -> bytes:
    """Return IPC stream bytes for a table.

    Parameters
    ----------
    table:
        Arrow table to serialize.

    Returns
    -------
    bytes
        IPC stream bytes for the table.
    """
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def ipc_hash(table: pa.Table) -> str:
    """Return SHA-256 hash of IPC stream bytes.

    Parameters
    ----------
    table:
        Arrow table to hash.

    Returns
    -------
    str
        SHA-256 hex digest of the IPC bytes.
    """
    return hashlib.sha256(ipc_bytes(table)).hexdigest()


def payload_ipc_bytes(payload: Mapping[str, object], schema: pa.Schema) -> bytes:
    """Return IPC stream bytes for a payload.

    Parameters
    ----------
    payload:
        Mapping payload to encode.
    schema:
        Explicit schema for the payload table.

    Returns
    -------
    bytes
        IPC stream bytes for the payload.
    """
    return ipc_bytes(payload_table(payload, schema))


def payload_hash(payload: Mapping[str, object], schema: pa.Schema) -> str:
    """Return SHA-256 hash for a payload.

    Parameters
    ----------
    payload:
        Mapping payload to encode.
    schema:
        Explicit schema for the payload table.

    Returns
    -------
    str
        SHA-256 hex digest for the payload IPC bytes.
    """
    return ipc_hash(payload_table(payload, schema))


def ipc_table(payload: bytes) -> pa.Table:
    """Decode IPC stream bytes into a table.

    Parameters
    ----------
    payload:
        IPC stream bytes.

    Returns
    -------
    pyarrow.Table
        Table decoded from the IPC stream.
    """
    reader = ipc.open_stream(payload)
    return reader.read_all()


__all__ = [
    "ipc_bytes",
    "ipc_hash",
    "ipc_table",
    "payload_hash",
    "payload_ipc_bytes",
    "payload_table",
]
