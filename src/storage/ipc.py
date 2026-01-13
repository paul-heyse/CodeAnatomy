"""Arrow IPC read/write helpers."""

from __future__ import annotations

from arrowdsl.io.ipc import read_table_ipc_file, write_table_ipc_file, write_table_ipc_stream

__all__ = [
    "read_table_ipc_file",
    "write_table_ipc_file",
    "write_table_ipc_stream",
]
