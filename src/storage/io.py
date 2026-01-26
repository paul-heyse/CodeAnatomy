"""Arrow IPC and Delta Lake read/write helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal, cast

import pyarrow as pa

from core_types import PathLike, ensure_path
from datafusion_engine.schema_contracts import SCHEMA_ABI_FINGERPRINT_META, SchemaContract
from ibis_engine.io_bridge import (
    IbisDatasetWriteOptions,
    IbisNamedDatasetWriteOptions,
    IbisWriteInput,
    ibis_plan_to_reader,
    ibis_table_to_reader,
    ibis_to_table,
)
from ibis_engine.io_bridge import (
    write_ibis_dataset_delta as _write_ibis_dataset_delta,
)
from ibis_engine.io_bridge import (
    write_ibis_named_datasets_delta as _write_ibis_named_datasets_delta,
)
from ibis_engine.plan import IbisPlan
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
from storage.ipc import read_table_ipc_file, write_table_ipc_file, write_table_ipc_stream


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


def _schema_from_write_input(value: IbisWriteInput) -> pa.Schema | None:
    candidate: object = value
    if isinstance(value, IbisPlan):
        candidate = ibis_plan_to_reader(value)
    if isinstance(candidate, pa.Table):
        table = cast("pa.Table", candidate)
        return table.schema
    if isinstance(candidate, pa.RecordBatchReader):
        reader = cast("pa.RecordBatchReader", candidate)
        return reader.schema
    schema = getattr(candidate, "schema", None)
    schema_value = schema() if callable(schema) else schema
    return _schema_from_value(schema_value)


def _schema_from_value(schema: object) -> pa.Schema | None:
    if isinstance(schema, pa.Schema):
        return schema
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    return None


def _require_schema_abi(schema: pa.Schema, *, table_name: str) -> None:
    metadata = schema.metadata or {}
    expected = SchemaContract.from_arrow_schema(table_name, schema).schema_metadata
    expected_abi = expected.get(SCHEMA_ABI_FINGERPRINT_META)
    actual_abi = metadata.get(SCHEMA_ABI_FINGERPRINT_META)
    if actual_abi is None:
        msg = f"Schema ABI fingerprint missing for {table_name!r}."
        raise ValueError(msg)
    if expected_abi is not None and actual_abi != expected_abi:
        msg = f"Schema ABI fingerprint mismatch for {table_name!r}."
        raise ValueError(msg)


def write_ibis_dataset_delta(
    data: IbisWriteInput,
    base_dir: PathLike,
    *,
    options: IbisDatasetWriteOptions | None = None,
    table_name: str | None = None,
) -> DeltaWriteResult:
    """Write an Ibis plan/table or Arrow input as a Delta table.

    Returns
    -------
    DeltaWriteResult
        Delta write result containing path and version metadata.

    Raises
    ------
    ValueError
        Raised when the table schema cannot be resolved.
    """
    if table_name is not None:
        schema = _schema_from_write_input(data)
        if schema is None:
            msg = f"Unable to resolve schema for {table_name!r} Delta write."
            raise ValueError(msg)
        _require_schema_abi(schema, table_name=table_name)
    return _write_ibis_dataset_delta(data, base_dir, options=options, table_name=table_name)


def write_ibis_named_datasets_delta(
    datasets: Mapping[str, IbisWriteInput],
    base_dir: PathLike,
    *,
    options: IbisNamedDatasetWriteOptions | None = None,
) -> dict[str, DeltaWriteResult]:
    """Write a mapping of Ibis/Arrow datasets to Delta tables.

    Returns
    -------
    dict[str, DeltaWriteResult]
        Mapping of dataset names to Delta write results.

    Raises
    ------
    ValueError
        Raised when a dataset schema cannot be resolved.
    """
    for name, value in datasets.items():
        schema = _schema_from_write_input(value)
        if schema is None:
            msg = f"Unable to resolve schema for {name!r} Delta write."
            raise ValueError(msg)
        _require_schema_abi(schema, table_name=name)
    return _write_ibis_named_datasets_delta(datasets, base_dir, options=options)


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
    "vacuum_delta",
    "write_ibis_dataset_delta",
    "write_ibis_named_datasets_delta",
    "write_table_ipc_file",
    "write_table_ipc_stream",
]
