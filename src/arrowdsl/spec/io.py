"""Spec table IO helpers."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from pyarrow import ipc

from arrowdsl.core.interop import SchemaLike
from arrowdsl.schema.build import table_from_rows as build_table_from_rows

if TYPE_CHECKING:
    from arrowdsl.spec.tables.base import SpecTableCodec


@dataclass(frozen=True)
class IpcWriteConfig:
    """Configuration for IPC write options."""

    compression: str | None = "zstd"
    use_threads: bool = True
    unify_dictionaries: bool = True
    emit_dictionary_deltas: bool = False
    allow_64bit: bool = False
    metadata_version: ipc.MetadataVersion = ipc.MetadataVersion.V5


def ipc_write_options_factory(
    config: IpcWriteConfig | None = None,
) -> ipc.IpcWriteOptions:
    """Return a shared IPC write options configuration.

    Returns
    -------
    ipc.IpcWriteOptions
        Configured IPC write options.
    """
    config = config or IpcWriteConfig()
    return ipc.IpcWriteOptions(
        metadata_version=config.metadata_version,
        allow_64bit=config.allow_64bit,
        compression=config.compression,
        use_threads=config.use_threads,
        emit_dictionary_deltas=config.emit_dictionary_deltas,
        unify_dictionaries=config.unify_dictionaries,
    )


def ipc_read_options_factory(
    *,
    ensure_native_endian: bool = True,
    ensure_alignment: ipc.Alignment = ipc.Alignment.Any,
    use_threads: bool = True,
    included_fields: Sequence[int] | None = None,
) -> ipc.IpcReadOptions:
    """Return a shared IPC read options configuration.

    Returns
    -------
    ipc.IpcReadOptions
        Configured IPC read options.
    """
    return ipc.IpcReadOptions(
        ensure_native_endian,
        ensure_alignment=ensure_alignment,
        use_threads=use_threads,
        included_fields=list(included_fields) if included_fields is not None else None,
    )


def write_spec_table(
    path: str | Path,
    table: pa.Table,
    *,
    options: ipc.IpcWriteOptions | None = None,
) -> None:
    """Write a spec table to an IPC file."""
    options = options or ipc_write_options_factory()
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with ipc.new_file(str(target), table.schema, options=options) as writer:
        writer.write_table(table)


def read_spec_table(
    path: str | Path,
    *,
    options: ipc.IpcReadOptions | None = None,
) -> pa.Table:
    """Read a spec table from an IPC file.

    Returns
    -------
    pa.Table
        Table loaded from disk.
    """
    options = options or ipc_read_options_factory()
    with ipc.open_file(str(path), options=options) as reader:
        return reader.read_all()


def write_spec_values[SpecT](
    codec: SpecTableCodec[SpecT],
    path: str | Path,
    values: Sequence[SpecT],
    *,
    options: ipc.IpcWriteOptions | None = None,
) -> None:
    """Write spec values to an IPC file."""
    write_spec_table(path, codec.to_table(values), options=options)


def read_spec_values[SpecT](
    codec: SpecTableCodec[SpecT],
    path: str | Path,
    *,
    options: ipc.IpcReadOptions | None = None,
) -> tuple[SpecT, ...]:
    """Read spec values from an IPC file.

    Returns
    -------
    tuple[SpecT, ...]
        Spec values decoded from the table.
    """
    return codec.from_table(read_spec_table(path, options=options))


def table_from_rows(
    schema: SchemaLike,
    rows: Iterable[Mapping[str, Any]],
) -> pa.Table:
    """Build a spec table from row dictionaries.

    Returns
    -------
    pa.Table
        Table built from rows with the provided schema.
    """
    return build_table_from_rows(schema, rows)


def rows_from_table(table: pa.Table) -> list[dict[str, Any]]:
    """Return rows from a spec table.

    Returns
    -------
    list[dict[str, Any]]
        Row dictionaries from the table.
    """
    return table.to_pylist()


def table_from_json(
    schema: SchemaLike,
    payload: list[dict[str, Any]],
) -> pa.Table:
    """Build a spec table from JSON records.

    Returns
    -------
    pa.Table
        Table built from JSON payload.
    """
    return table_from_rows(schema, payload)


def table_from_json_file(schema: SchemaLike, path: str | Path) -> pa.Table:
    """Build a spec table from a JSON file.

    Returns
    -------
    pa.Table
        Table built from JSON payload.

    Raises
    ------
    TypeError
        Raised when the JSON payload is not a list of objects.
    """
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        msg = "Spec JSON payload must be a list of objects."
        raise TypeError(msg)
    return table_from_json(schema, data)


def sort_spec_table(table: pa.Table, *, keys: Sequence[str]) -> pa.Table:
    """Return a deterministically sorted copy of a spec table.

    Returns
    -------
    pa.Table
        Sorted table copy.

    Raises
    ------
    ValueError
        Raised when required sort keys are missing.
    """
    if not keys:
        return table
    missing = [key for key in keys if key not in table.column_names]
    if missing:
        msg = f"Spec table is missing sort keys: {missing}."
        raise ValueError(msg)
    sort_keys = [(key, "ascending") for key in keys]
    return table.sort_by(sort_keys)


__all__ = [
    "IpcWriteConfig",
    "ipc_read_options_factory",
    "ipc_write_options_factory",
    "read_spec_table",
    "read_spec_values",
    "rows_from_table",
    "sort_spec_table",
    "table_from_json",
    "table_from_json_file",
    "table_from_rows",
    "write_spec_table",
    "write_spec_values",
]
