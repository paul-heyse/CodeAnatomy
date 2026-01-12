"""Arrow IPC read/write helpers."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as pa_ipc

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.schema import SchemaMetadataSpec
from core_types import PathLike, ensure_path


def _ensure_dir(path: Path) -> None:
    path.mkdir(exist_ok=True, parents=True)


def write_table_ipc_file(
    table: TableLike,
    path: PathLike,
    *,
    overwrite: bool = True,
    schema_metadata: dict[bytes, bytes] | None = None,
) -> str:
    """Write an Arrow IPC file (random-access) to a path.

    Returns
    -------
    str
        Path to the written IPC file.
    """
    target = ensure_path(path)
    _ensure_dir(target.parent)
    if overwrite and target.exists():
        target.unlink()

    schema = table.schema
    if schema_metadata:
        schema = SchemaMetadataSpec(schema_metadata=schema_metadata).apply(schema)
        table = table.cast(schema, safe=False)

    with pa.OSFile(str(target), "wb") as sink, pa_ipc.new_file(sink, schema) as writer:
        writer.write_table(table)
    return str(target)


def write_table_ipc_stream(
    table: TableLike,
    path: PathLike,
    *,
    overwrite: bool = True,
    schema_metadata: dict[bytes, bytes] | None = None,
) -> str:
    """Write an Arrow IPC stream (sequential) to a path.

    Returns
    -------
    str
        Path to the written IPC stream.
    """
    target = ensure_path(path)
    _ensure_dir(target.parent)
    if overwrite and target.exists():
        target.unlink()

    schema = table.schema
    if schema_metadata:
        schema = SchemaMetadataSpec(schema_metadata=schema_metadata).apply(schema)
        table = table.cast(schema, safe=False)

    with pa.OSFile(str(target), "wb") as sink, pa_ipc.new_stream(sink, schema) as writer:
        writer.write_table(table)
    return str(target)


def read_table_ipc_file(path: PathLike) -> TableLike:
    """Read an Arrow IPC file.

    Returns
    -------
    TableLike
        Loaded table.
    """
    target = ensure_path(path)
    with pa.memory_map(str(target), "r") as source:
        reader = pa_ipc.open_file(source)
        return reader.read_all()
