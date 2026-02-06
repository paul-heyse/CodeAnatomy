"""Arrow IPC and Delta Lake read/write helpers."""

from __future__ import annotations

from typing import Literal

import pyarrow as pa

from core_types import PathLike, ensure_path
from storage.deltalake import (
    DeltaCdfOptions,
    DeltaWriteResult,
    StorageOptions,
    cleanup_delta_log,
    coerce_delta_input,
    coerce_delta_table,
    create_delta_checkpoint,
    delta_commit_metadata,
    delta_history_snapshot,
    delta_protocol_snapshot,
    delta_table_features,
    delta_table_version,
    enable_delta_features,
    read_delta_cdf,
    vacuum_delta,
)
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy
from storage.ipc_utils import read_table_ipc_file, write_table_ipc_file, write_table_ipc_stream


def mmap_file(path: PathLike, *, mode: Literal["r", "r+", "w+", "a+"] = "r") -> pa.MemoryMappedFile:
    """Return a memory-mapped file handle for a path.

    Returns:
    -------
    pyarrow.MemoryMappedFile
        Memory-mapped file handle.
    """
    target = ensure_path(path)
    return pa.memory_map(str(target), mode)


def open_compressed_input(
    path: PathLike,
    *,
    compression: str,
) -> pa.CompressedInputStream:
    """Return a compressed input stream for a path.

    Returns:
    -------
    pyarrow.CompressedInputStream
        Compressed input stream.
    """
    target = ensure_path(path)
    return pa.CompressedInputStream(pa.input_stream(str(target)), compression)


def open_compressed_output(
    path: PathLike,
    *,
    compression: str,
) -> pa.CompressedOutputStream:
    """Return a compressed output stream for a path.

    Returns:
    -------
    pyarrow.CompressedOutputStream
        Compressed output stream.
    """
    target = ensure_path(path)
    return pa.CompressedOutputStream(pa.output_stream(str(target)), compression)


__all__ = [
    "DeltaCdfOptions",
    "DeltaSchemaPolicy",
    "DeltaWritePolicy",
    "DeltaWriteResult",
    "StorageOptions",
    "cleanup_delta_log",
    "coerce_delta_input",
    "coerce_delta_table",
    "create_delta_checkpoint",
    "delta_commit_metadata",
    "delta_history_snapshot",
    "delta_protocol_snapshot",
    "delta_table_features",
    "delta_table_version",
    "enable_delta_features",
    "mmap_file",
    "open_compressed_input",
    "open_compressed_output",
    "read_delta_cdf",
    "read_table_ipc_file",
    "vacuum_delta",
    "write_table_ipc_file",
    "write_table_ipc_stream",
]
