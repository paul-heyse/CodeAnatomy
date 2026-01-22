"""Arrow IPC, Delta Lake, and Parquet read helpers."""

from __future__ import annotations

from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq

from arrowdsl.io.ipc import read_table_ipc_file, write_table_ipc_file, write_table_ipc_stream
from core_types import PathLike, ensure_path
from ibis_engine.io_bridge import (
    IbisWriteInput,
    ibis_plan_to_reader,
    ibis_table_to_reader,
    ibis_to_table,
    write_ibis_dataset_delta,
    write_ibis_named_datasets_delta,
)
from storage.deltalake import (
    DeltaCdfOptions,
    DeltaWriteResult,
    StorageOptions,
    cleanup_delta_log,
    coerce_delta_table,
    create_delta_checkpoint,
    delta_commit_metadata,
    delta_history_snapshot,
    delta_protocol_snapshot,
    delta_table_features,
    delta_table_version,
    enable_delta_features,
    open_delta_table,
    read_delta_cdf,
    vacuum_delta,
)
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy


def mmap_file(path: PathLike, *, mode: Literal["r", "r+", "w+", "a+"] = "r") -> pa.MemoryMappedFile:
    """Return a memory-mapped file handle for a path.

    Returns
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

    Returns
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

    Returns
    -------
    pyarrow.CompressedOutputStream
        Compressed output stream.
    """
    target = ensure_path(path)
    return pa.CompressedOutputStream(pa.output_stream(str(target)), compression)


def read_table_parquet(path: PathLike) -> pa.Table:
    """Return a Parquet file as an Arrow table.

    Returns
    -------
    pyarrow.Table
        Table loaded from the Parquet file.
    """
    target = ensure_path(path)
    return pq.read_table(str(target))


__all__ = [
    "DeltaCdfOptions",
    "DeltaSchemaPolicy",
    "DeltaWritePolicy",
    "DeltaWriteResult",
    "IbisWriteInput",
    "StorageOptions",
    "cleanup_delta_log",
    "coerce_delta_table",
    "create_delta_checkpoint",
    "delta_commit_metadata",
    "delta_history_snapshot",
    "delta_protocol_snapshot",
    "delta_table_features",
    "delta_table_version",
    "enable_delta_features",
    "ibis_plan_to_reader",
    "ibis_table_to_reader",
    "ibis_to_table",
    "mmap_file",
    "open_compressed_input",
    "open_compressed_output",
    "open_delta_table",
    "read_delta_cdf",
    "read_table_ipc_file",
    "read_table_parquet",
    "vacuum_delta",
    "write_ibis_dataset_delta",
    "write_ibis_named_datasets_delta",
    "write_table_ipc_file",
    "write_table_ipc_stream",
]
