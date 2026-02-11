"""Arrow IPC and payload hashing helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as pa_ipc

from core_types import JsonDict, PathLike, ensure_path
from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from planning_engine.plan_product import PlanProduct
from utils.hashing import hash_sha256_hex

type IpcWriteInput = TableLike | RecordBatchReaderLike | PlanProduct


@dataclass(frozen=True)
class IpcWriteConfig:
    """Configuration for IPC write options."""

    compression: str | None = "zstd"
    use_threads: bool = True
    unify_dictionaries: bool = True
    emit_dictionary_deltas: bool = False
    allow_64bit: bool = False
    metadata_version: pa_ipc.MetadataVersion = pa_ipc.MetadataVersion.V5


def ipc_write_options_factory(
    config: IpcWriteConfig | None = None,
) -> pa_ipc.IpcWriteOptions:
    """Return a shared IPC write options configuration.

    Returns:
    -------
    pyarrow.ipc.IpcWriteOptions
        Configured IPC write options.
    """
    resolved = config or IpcWriteConfig()
    return pa_ipc.IpcWriteOptions(
        metadata_version=resolved.metadata_version,
        allow_64bit=resolved.allow_64bit,
        compression=resolved.compression,
        use_threads=resolved.use_threads,
        emit_dictionary_deltas=resolved.emit_dictionary_deltas,
        unify_dictionaries=resolved.unify_dictionaries,
    )


def ipc_read_options_factory(
    *,
    ensure_native_endian: bool = True,
    ensure_alignment: pa_ipc.Alignment = pa_ipc.Alignment.Any,
    use_threads: bool = True,
    included_fields: Sequence[int] | None = None,
) -> pa_ipc.IpcReadOptions:
    """Return a shared IPC read options configuration.

    Returns:
    -------
    pyarrow.ipc.IpcReadOptions
        Configured IPC read options.
    """
    return pa_ipc.IpcReadOptions(
        ensure_native_endian,
        ensure_alignment=ensure_alignment,
        use_threads=use_threads,
        included_fields=list(included_fields) if included_fields is not None else None,
    )


def payload_table(payload: Mapping[str, object], schema: pa.Schema) -> pa.Table:
    """Build a single-row table for a payload.

    Parameters
    ----------
    payload:
        Mapping payload to encode.
    schema:
        Explicit schema for the payload table.

    Returns:
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

    Returns:
    -------
    bytes
        IPC stream bytes for the table.
    """
    sink = pa.BufferOutputStream()
    with pa_ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def ipc_hash(table: pa.Table) -> str:
    """Return SHA-256 hash of IPC stream bytes.

    Parameters
    ----------
    table:
        Arrow table to hash.

    Returns:
    -------
    str
        SHA-256 hex digest of the IPC bytes.
    """
    return hash_sha256_hex(ipc_bytes(table))


def _apply_schema_metadata(schema: pa.Schema, metadata: dict[bytes, bytes]) -> pa.Schema:
    from datafusion_engine.arrow.metadata import SchemaMetadataSpec

    return SchemaMetadataSpec(schema_metadata=metadata).apply(schema)


def payload_ipc_bytes(payload: Mapping[str, object], schema: pa.Schema) -> bytes:
    """Return IPC stream bytes for a payload.

    Parameters
    ----------
    payload:
        Mapping payload to encode.
    schema:
        Explicit schema for the payload table.

    Returns:
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

    Returns:
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

    Returns:
    -------
    pyarrow.Table
        Table decoded from the IPC stream.
    """
    reader = pa_ipc.open_stream(payload)
    return reader.read_all()


def _resolve_plan_product(value: IpcWriteInput) -> TableLike | RecordBatchReaderLike:
    if isinstance(value, PlanProduct):
        if value.writer_strategy != "arrow":
            msg = "IPC writes require an Arrow writer strategy."
            raise ValueError(msg)
        return value.value()
    return value


def _ensure_dir(path: Path) -> None:
    """Ensure the directory exists for a target path.

    Parameters
    ----------
    path
        Directory path to create if missing.
    """
    path.mkdir(exist_ok=True, parents=True)


def write_table_ipc_file(
    table: IpcWriteInput,
    path: PathLike,
    *,
    overwrite: bool = True,
    schema_metadata: dict[bytes, bytes] | None = None,
    config: IpcWriteConfig | None = None,
) -> str:
    """Write an Arrow IPC file (random-access) to a path.

    Returns:
    -------
    str
        Path to the written IPC file.
    """
    target = ensure_path(path)
    _ensure_dir(target.parent)
    if overwrite and target.exists():
        target.unlink()

    data = _resolve_plan_product(table)
    schema = data.schema
    if schema_metadata:
        schema = _apply_schema_metadata(schema, schema_metadata)

    options = ipc_write_options_factory(config)
    with (
        pa.OSFile(str(target), "wb") as sink,
        pa_ipc.new_file(sink, schema, options=options) as writer,
    ):
        if isinstance(data, RecordBatchReaderLike):
            for batch in data:
                writer.write_batch(batch)
        else:
            table_value = data.cast(schema, safe=False)
            writer.write_table(table_value)
    return str(target)


def write_table_ipc_stream(
    table: IpcWriteInput,
    path: PathLike,
    *,
    overwrite: bool = True,
    schema_metadata: dict[bytes, bytes] | None = None,
    config: IpcWriteConfig | None = None,
) -> str:
    """Write an Arrow IPC stream (sequential) to a path.

    Returns:
    -------
    str
        Path to the written IPC stream.
    """
    target = ensure_path(path)
    _ensure_dir(target.parent)
    if overwrite and target.exists():
        target.unlink()

    data = _resolve_plan_product(table)
    schema = data.schema
    if schema_metadata:
        schema = _apply_schema_metadata(schema, schema_metadata)

    options = ipc_write_options_factory(config)
    with (
        pa.OSFile(str(target), "wb") as sink,
        pa_ipc.new_stream(sink, schema, options=options) as writer,
    ):
        if isinstance(data, RecordBatchReaderLike):
            for batch in data:
                writer.write_batch(batch)
        else:
            table_value = data.cast(schema, safe=False)
            writer.write_table(table_value)
    return str(target)


def ipc_write_config_payload(config: IpcWriteConfig | None) -> JsonDict:
    """Return a JSON-friendly payload for IPC write options.

    Returns:
    -------
    JsonDict
        JSON-ready IPC write configuration payload.
    """
    resolved = config or IpcWriteConfig()
    return {
        "compression": resolved.compression,
        "use_threads": resolved.use_threads,
        "unify_dictionaries": resolved.unify_dictionaries,
        "emit_dictionary_deltas": resolved.emit_dictionary_deltas,
        "allow_64bit": resolved.allow_64bit,
        "metadata_version": str(resolved.metadata_version),
    }


def read_table_ipc_file(path: PathLike) -> TableLike:
    """Read an Arrow IPC file.

    Returns:
    -------
    TableLike
        Loaded table.
    """
    target = ensure_path(path)
    options = ipc_read_options_factory()
    with pa.memory_map(str(target), "r") as source:
        reader = pa_ipc.open_file(source, options=options)
        return reader.read_all()


__all__ = [
    "IpcWriteConfig",
    "ipc_bytes",
    "ipc_hash",
    "ipc_read_options_factory",
    "ipc_table",
    "ipc_write_config_payload",
    "ipc_write_options_factory",
    "payload_hash",
    "payload_ipc_bytes",
    "payload_table",
    "read_table_ipc_file",
    "write_table_ipc_file",
    "write_table_ipc_stream",
]
