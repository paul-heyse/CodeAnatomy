from __future__ import annotations

import os
import pathlib

import pyarrow as pa


def _ensure_dir(path: str) -> None:
    pathlib.Path(path).mkdir(exist_ok=True, parents=True)


def write_table_ipc_file(table: pa.Table, path: str, *, overwrite: bool = True) -> str:
    """
    Write an Arrow IPC *file* (random-access) to `path`.
    """
    _ensure_dir(os.path.dirname(path) or ".")
    if overwrite and pathlib.Path(path).exists():
        pathlib.Path(path).unlink()

    with pa.OSFile(path, "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return path


def write_table_ipc_stream(table: pa.Table, path: str, *, overwrite: bool = True) -> str:
    """
    Write an Arrow IPC *stream* (sequential) to `path`.
    """
    _ensure_dir(os.path.dirname(path) or ".")
    if overwrite and pathlib.Path(path).exists():
        pathlib.Path(path).unlink()

    with pa.OSFile(path, "wb") as sink, pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return path


def read_table_ipc_file(path: str) -> pa.Table:
    """
    Read an Arrow IPC file.
    """
    with pa.memory_map(path, "r") as source:
        reader = pa.ipc.open_file(source)
        return reader.read_all()
