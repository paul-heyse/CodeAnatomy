"""Storage helpers for Arrow IPC and Delta Lake."""

from __future__ import annotations

import importlib
import warnings
from typing import TYPE_CHECKING

__all__ = (
    "DeltaCdfOptions",
    "DeltaSchemaPolicy",
    "DeltaWritePolicy",
    "DeltaWriteResult",
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
    "read_delta_cdf",
    "read_table_ipc_file",
    "vacuum_delta",
    "write_table_ipc_file",
    "write_table_ipc_stream",
)
_EXPORTS: tuple[str, ...] = __all__
_EXPORT_MAP: dict[str, tuple[str, str]] = {name: ("storage.io", name) for name in _EXPORTS}
_DEPRECATED_EXPORTS: dict[str, str] = {
    "DeltaCdfOptions": (
        "storage.DeltaCdfOptions is deprecated; import from storage.deltalake instead."
    ),
    "DeltaSchemaPolicy": (
        "storage.DeltaSchemaPolicy is deprecated; import from storage.deltalake.config instead."
    ),
    "DeltaWritePolicy": (
        "storage.DeltaWritePolicy is deprecated; import from storage.deltalake.config instead."
    ),
    "DeltaWriteResult": (
        "storage.DeltaWriteResult is deprecated; import from storage.deltalake instead."
    ),
    "cleanup_delta_log": (
        "storage.cleanup_delta_log is deprecated; import from storage.deltalake instead."
    ),
    "coerce_delta_input": (
        "storage.coerce_delta_input is deprecated; import from storage.deltalake instead."
    ),
    "coerce_delta_table": (
        "storage.coerce_delta_table is deprecated; import from storage.deltalake instead."
    ),
    "create_delta_checkpoint": (
        "storage.create_delta_checkpoint is deprecated; import from storage.deltalake instead."
    ),
    "delta_commit_metadata": (
        "storage.delta_commit_metadata is deprecated; import from storage.deltalake instead."
    ),
    "delta_history_snapshot": (
        "storage.delta_history_snapshot is deprecated; import from storage.deltalake instead."
    ),
    "delta_protocol_snapshot": (
        "storage.delta_protocol_snapshot is deprecated; import from storage.deltalake instead."
    ),
    "delta_table_features": (
        "storage.delta_table_features is deprecated; import from storage.deltalake instead."
    ),
    "delta_table_version": (
        "storage.delta_table_version is deprecated; import from storage.deltalake instead."
    ),
    "enable_delta_features": (
        "storage.enable_delta_features is deprecated; import from storage.deltalake instead."
    ),
    "read_delta_cdf": (
        "storage.read_delta_cdf is deprecated; import from storage.deltalake instead."
    ),
    "vacuum_delta": ("storage.vacuum_delta is deprecated; import from storage.deltalake instead."),
}

if TYPE_CHECKING:
    from storage import io as _storage_io

    DeltaCdfOptions = _storage_io.DeltaCdfOptions
    DeltaSchemaPolicy = _storage_io.DeltaSchemaPolicy
    DeltaWritePolicy = _storage_io.DeltaWritePolicy
    DeltaWriteResult = _storage_io.DeltaWriteResult
    cleanup_delta_log = _storage_io.cleanup_delta_log
    coerce_delta_input = _storage_io.coerce_delta_input
    coerce_delta_table = _storage_io.coerce_delta_table
    create_delta_checkpoint = _storage_io.create_delta_checkpoint
    delta_commit_metadata = _storage_io.delta_commit_metadata
    delta_history_snapshot = _storage_io.delta_history_snapshot
    delta_protocol_snapshot = _storage_io.delta_protocol_snapshot
    delta_table_features = _storage_io.delta_table_features
    delta_table_version = _storage_io.delta_table_version
    enable_delta_features = _storage_io.enable_delta_features
    read_delta_cdf = _storage_io.read_delta_cdf
    read_table_ipc_file = _storage_io.read_table_ipc_file
    vacuum_delta = _storage_io.vacuum_delta
    write_table_ipc_file = _storage_io.write_table_ipc_file
    write_table_ipc_stream = _storage_io.write_table_ipc_stream


def __getattr__(name: str) -> object:
    export = _EXPORT_MAP.get(name)
    if export is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    if warning := _DEPRECATED_EXPORTS.get(name):
        warnings.warn(warning, DeprecationWarning, stacklevel=2)
    module_name, attr = export
    module = importlib.import_module(module_name)
    value = getattr(module, attr)
    globals()[name] = value
    return value
