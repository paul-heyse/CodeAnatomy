"""Storage helpers for Arrow IPC and Delta Lake."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

__all__ = (
    "DeltaCdfOptions",
    "DeltaSchemaPolicy",
    "DeltaWritePolicy",
    "DeltaWriteResult",
    "cleanup_delta_log",
    "coerce_delta_table",
    "create_delta_checkpoint",
    "delta_commit_metadata",
    "delta_history_snapshot",
    "delta_protocol_snapshot",
    "delta_table_features",
    "delta_table_version",
    "enable_delta_features",
    "open_delta_table",
    "read_delta_cdf",
    "read_table_ipc_file",
    "vacuum_delta",
    "write_ibis_dataset_delta",
    "write_ibis_named_datasets_delta",
    "write_table_ipc_file",
    "write_table_ipc_stream",
)
_EXPORTS: tuple[str, ...] = __all__
_EXPORT_MAP: dict[str, tuple[str, str]] = {name: ("storage.io", name) for name in _EXPORTS}

if TYPE_CHECKING:
    from storage import io as _storage_io

    DeltaCdfOptions = _storage_io.DeltaCdfOptions
    DeltaSchemaPolicy = _storage_io.DeltaSchemaPolicy
    DeltaWritePolicy = _storage_io.DeltaWritePolicy
    DeltaWriteResult = _storage_io.DeltaWriteResult
    cleanup_delta_log = _storage_io.cleanup_delta_log
    coerce_delta_table = _storage_io.coerce_delta_table
    create_delta_checkpoint = _storage_io.create_delta_checkpoint
    delta_commit_metadata = _storage_io.delta_commit_metadata
    delta_history_snapshot = _storage_io.delta_history_snapshot
    delta_protocol_snapshot = _storage_io.delta_protocol_snapshot
    delta_table_features = _storage_io.delta_table_features
    delta_table_version = _storage_io.delta_table_version
    enable_delta_features = _storage_io.enable_delta_features
    open_delta_table = _storage_io.open_delta_table
    read_delta_cdf = _storage_io.read_delta_cdf
    read_table_ipc_file = _storage_io.read_table_ipc_file
    vacuum_delta = _storage_io.vacuum_delta
    write_ibis_dataset_delta = _storage_io.write_ibis_dataset_delta
    write_ibis_named_datasets_delta = _storage_io.write_ibis_named_datasets_delta
    write_table_ipc_file = _storage_io.write_table_ipc_file
    write_table_ipc_stream = _storage_io.write_table_ipc_stream


def __getattr__(name: str) -> object:
    export = _EXPORT_MAP.get(name)
    if export is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attr = export
    module = importlib.import_module(module_name)
    value = getattr(module, attr)
    globals()[name] = value
    return value
