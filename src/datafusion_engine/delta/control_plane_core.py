"""Rust-backed Delta control-plane adapters.

This module centralizes access to the Rust Delta control plane exposed via
``datafusion_ext``. It provides a typed, canonical surface for:
1. Snapshot and protocol-aware metadata.
2. Provider construction with session-derived scan configuration.
3. File-pruned provider construction for scan planning.
4. Maintenance operations (optimize/vacuum/checkpoint).
"""

from __future__ import annotations

import base64
import importlib
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, NoReturn, cast

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.arrow.abi import schema_to_dict
from datafusion_engine.delta.capabilities import (
    DeltaExtensionCompatibility,
    is_delta_extension_compatible,
    resolve_delta_extension_module,
)
from datafusion_engine.delta.control_plane_types import (
    DeltaAddConstraintsRequest,
    DeltaAddFeaturesRequest,
    DeltaCdfProviderBundle,
    DeltaCdfRequest,
    DeltaCheckpointRequest,
    DeltaDeleteRequest,
    DeltaDropConstraintsRequest,
    DeltaFeatureEnableRequest,
    DeltaMergeRequest,
    DeltaOptimizeRequest,
    DeltaProviderBundle,
    DeltaProviderRequest,
    DeltaRestoreRequest,
    DeltaSetPropertiesRequest,
    DeltaSnapshotRequest,
    DeltaTableRef,
    DeltaUpdateRequest,
    DeltaVacuumRequest,
    DeltaWriteRequest,
)
from datafusion_engine.delta.payload import (
    cdf_options_payload,
)
from datafusion_engine.delta.protocols import (
    InternalSessionContext,
    RustCdfOptionsHandle,
    RustDeltaEntrypoint,
    RustDeltaExtensionModule,
)
from datafusion_engine.delta.specs import (
    DeltaAppTransactionSpec as DeltaAppTransaction,
)
from datafusion_engine.delta.specs import (
    DeltaCdfOptionsSpec as DeltaCdfOptions,
)
from datafusion_engine.delta.specs import (
    DeltaCommitOptionsSpec as DeltaCommitOptions,
)
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.extensions.context_adaptation import select_context_candidate
from datafusion_engine.generated.delta_types import DeltaFeatureGate
from utils.value_coercion import coerce_mapping_list


def _resolve_extension_module(
    *,
    required_attr: str | None = None,
    entrypoint: str | None = None,
) -> RustDeltaExtensionModule:
    """Return the Delta extension module (datafusion_ext).

    Args:
        required_attr: Description.
        entrypoint: Description.

    Raises:
        DataFusionEngineError: If the operation cannot be completed.
    """
    resolved = resolve_delta_extension_module(
        required_attr=required_attr,
        entrypoint=entrypoint,
    )
    if resolved is not None:
        return cast("RustDeltaExtensionModule", resolved.module)
    msg = "Delta control-plane operations require datafusion_ext."
    raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN)


def _raise_engine_error(
    message: str,
    *,
    kind: ErrorKind,
    exc: Exception | None = None,
) -> NoReturn:
    error = DataFusionEngineError(message, kind=kind)
    if exc is None:
        raise error
    raise error from exc


def _internal_ctx(
    ctx: SessionContext,
    *,
    entrypoint: str | None = None,
) -> InternalSessionContext:
    """Return the internal session context required by Rust entrypoints.

    Returns:
    -------
    InternalSessionContext
        Internal DataFusion session context used by Rust entrypoints.
    """
    probe_entrypoint = entrypoint or "delta_scan_config_from_session"
    compatibility = is_delta_extension_compatible(
        ctx,
        entrypoint=probe_entrypoint,
        require_non_fallback=True,
    )
    if not compatibility.available:
        msg = _compatibility_message(
            "Delta control-plane extension module is unavailable.",
            compatibility=compatibility,
            entrypoint=probe_entrypoint,
        )
        _raise_engine_error(msg, kind=ErrorKind.PLUGIN)
    if not compatibility.compatible:
        msg = _compatibility_message(
            "Delta control-plane extension is incompatible for this SessionContext.",
            compatibility=compatibility,
            entrypoint=probe_entrypoint,
        )
        _raise_engine_error(msg, kind=ErrorKind.PLUGIN)
    resolved = _context_from_compatibility(
        ctx,
        ctx_kind=compatibility.ctx_kind,
    )
    if resolved is not None:
        return resolved
    msg = _compatibility_message(
        "Delta control-plane extension selected an unavailable context kind.",
        compatibility=compatibility,
        entrypoint=probe_entrypoint,
    )
    _raise_engine_error(msg, kind=ErrorKind.PLUGIN)


def _context_from_compatibility(
    ctx: SessionContext,
    *,
    ctx_kind: str | None,
) -> InternalSessionContext | None:
    _ = ctx_kind
    candidates = dict(select_context_candidate(ctx))
    selected = candidates.get("outer")
    if selected is None:
        return None
    return cast("InternalSessionContext", selected)


def _compatibility_message(
    prefix: str,
    *,
    compatibility: DeltaExtensionCompatibility,
    entrypoint: str,
) -> str:
    details: list[str] = [prefix, f"entrypoint={entrypoint}"]
    probe_result = compatibility.probe_result
    if probe_result:
        details.append(f"probe_result={probe_result}")
    module = compatibility.module
    if module is not None:
        details.append(f"module={module}")
    ctx_kind = compatibility.ctx_kind
    if ctx_kind is not None:
        details.append(f"ctx_kind={ctx_kind}")
    error = compatibility.error
    if error:
        details.append(f"error={error}")
    return " ".join(details)


def _require_internal_entrypoint(name: str) -> RustDeltaEntrypoint:
    module = _resolve_extension_module(entrypoint=name)
    entrypoint = getattr(module, name, None)
    if not callable(entrypoint):
        msg = f"Delta control-plane entrypoint {name} is unavailable."
        raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN)
    return cast("RustDeltaEntrypoint", entrypoint)


def _parse_add_actions(payload: object | None) -> Sequence[Mapping[str, object]] | None:
    return coerce_mapping_list(payload)


def _decode_schema_ipc(payload: bytes) -> pa.Schema:
    """Decode Arrow schema IPC bytes into a ``pyarrow.Schema``.

    Returns:
    -------
    pyarrow.Schema
        Decoded Arrow schema.

    """
    try:
        return pa.ipc.read_schema(pa.BufferReader(payload))
    except (pa.ArrowInvalid, TypeError, ValueError) as exc:
        msg = "Invalid Delta scan schema IPC payload."
        _raise_engine_error(msg, kind=ErrorKind.ARROW, exc=exc)


def _cdf_options_to_ext(
    module: RustDeltaExtensionModule,
    options: DeltaCdfOptions | None,
) -> RustCdfOptionsHandle:
    """Convert Python CDF options into the Rust extension options type.

    Returns:
    -------
    object
        Rust extension options value.

    """
    options_type = getattr(module, "DeltaCdfOptions", None)
    if options_type is None:
        msg = "Delta CDF options type is unavailable in the extension module."
        _raise_engine_error(msg, kind=ErrorKind.PLUGIN)
    ext_options = cast("RustCdfOptionsHandle", options_type())
    payload = cdf_options_payload(options)
    starting_version = payload.get("starting_version")
    ending_version = payload.get("ending_version")
    starting_timestamp = payload.get("starting_timestamp")
    ending_timestamp = payload.get("ending_timestamp")
    if starting_version is not None and not isinstance(starting_version, int):
        msg = "Delta CDF starting_version must be an int or None."
        _raise_engine_error(msg, kind=ErrorKind.PLUGIN)
    if ending_version is not None and not isinstance(ending_version, int):
        msg = "Delta CDF ending_version must be an int or None."
        _raise_engine_error(msg, kind=ErrorKind.PLUGIN)
    if starting_timestamp is not None and not isinstance(starting_timestamp, str):
        msg = "Delta CDF starting_timestamp must be a str or None."
        _raise_engine_error(msg, kind=ErrorKind.PLUGIN)
    if ending_timestamp is not None and not isinstance(ending_timestamp, str):
        msg = "Delta CDF ending_timestamp must be a str or None."
        _raise_engine_error(msg, kind=ErrorKind.PLUGIN)
    ext_options.starting_version = starting_version
    ext_options.ending_version = ending_version
    ext_options.starting_timestamp = starting_timestamp
    ext_options.ending_timestamp = ending_timestamp
    ext_options.allow_out_of_range = bool(payload["allow_out_of_range"])
    return ext_options


def _scan_effective_payload(payload: Mapping[str, object]) -> dict[str, object]:
    """Normalize scan-config payloads into an effective scan snapshot.

    Returns:
    -------
    dict[str, object]
        Normalized scan configuration payload.
    """
    schema_payload: Mapping[str, object] | None = None
    schema_ipc_b64: str | None = None
    schema_ipc = payload.get("schema_ipc")
    if isinstance(schema_ipc, (bytes, bytearray, memoryview)):
        schema_bytes = bytes(schema_ipc)
        schema_payload = schema_to_dict(_decode_schema_ipc(schema_bytes))
        schema_ipc_b64 = base64.b64encode(schema_bytes).decode("ascii")
    return {
        "file_column_name": payload.get("file_column_name"),
        "enable_parquet_pushdown": payload.get("enable_parquet_pushdown"),
        "schema_force_view_types": payload.get("schema_force_view_types"),
        "wrap_partition_values": payload.get("wrap_partition_values"),
        "schema": schema_payload,
        "schema_ipc": schema_ipc_b64,
        "has_schema": payload.get("has_schema"),
    }


_LAZY_EXPORT_MODULES: dict[str, str] = {
    "delta_provider_from_session": "datafusion_engine.delta.control_plane_provider",
    "delta_provider_with_files": "datafusion_engine.delta.control_plane_provider",
    "delta_cdf_provider": "datafusion_engine.delta.control_plane_provider",
    "delta_snapshot_info": "datafusion_engine.delta.control_plane_provider",
    "delta_add_actions": "datafusion_engine.delta.control_plane_provider",
    "delta_write_ipc": "datafusion_engine.delta.control_plane_mutation",
    "delta_delete": "datafusion_engine.delta.control_plane_mutation",
    "delta_update": "datafusion_engine.delta.control_plane_mutation",
    "delta_merge": "datafusion_engine.delta.control_plane_mutation",
    "delta_optimize_compact": "datafusion_engine.delta.control_plane_maintenance",
    "delta_vacuum": "datafusion_engine.delta.control_plane_maintenance",
    "delta_restore": "datafusion_engine.delta.control_plane_maintenance",
    "delta_set_properties": "datafusion_engine.delta.control_plane_maintenance",
    "delta_add_features": "datafusion_engine.delta.control_plane_maintenance",
    "delta_add_constraints": "datafusion_engine.delta.control_plane_maintenance",
    "delta_drop_constraints": "datafusion_engine.delta.control_plane_maintenance",
    "delta_create_checkpoint": "datafusion_engine.delta.control_plane_maintenance",
    "delta_cleanup_metadata": "datafusion_engine.delta.control_plane_maintenance",
    "delta_enable_column_mapping": "datafusion_engine.delta.control_plane_maintenance",
    "delta_enable_deletion_vectors": "datafusion_engine.delta.control_plane_maintenance",
    "delta_enable_row_tracking": "datafusion_engine.delta.control_plane_maintenance",
    "delta_enable_change_data_feed": "datafusion_engine.delta.control_plane_maintenance",
    "delta_enable_generated_columns": "datafusion_engine.delta.control_plane_maintenance",
    "delta_enable_invariants": "datafusion_engine.delta.control_plane_maintenance",
    "delta_enable_check_constraints": "datafusion_engine.delta.control_plane_maintenance",
    "delta_enable_in_commit_timestamps": "datafusion_engine.delta.control_plane_maintenance",
    "delta_enable_v2_checkpoints": "datafusion_engine.delta.control_plane_maintenance",
    "delta_enable_vacuum_protocol_check": "datafusion_engine.delta.control_plane_maintenance",
    "delta_enable_checkpoint_protection": "datafusion_engine.delta.control_plane_maintenance",
    "delta_disable_change_data_feed": "datafusion_engine.delta.control_plane_maintenance",
    "delta_disable_deletion_vectors": "datafusion_engine.delta.control_plane_maintenance",
    "delta_disable_row_tracking": "datafusion_engine.delta.control_plane_maintenance",
    "delta_disable_in_commit_timestamps": "datafusion_engine.delta.control_plane_maintenance",
    "delta_disable_vacuum_protocol_check": "datafusion_engine.delta.control_plane_maintenance",
    "delta_disable_checkpoint_protection": "datafusion_engine.delta.control_plane_maintenance",
    "delta_disable_column_mapping": "datafusion_engine.delta.control_plane_maintenance",
    "delta_disable_generated_columns": "datafusion_engine.delta.control_plane_maintenance",
    "delta_disable_invariants": "datafusion_engine.delta.control_plane_maintenance",
    "delta_disable_check_constraints": "datafusion_engine.delta.control_plane_maintenance",
    "delta_disable_v2_checkpoints": "datafusion_engine.delta.control_plane_maintenance",
}


def _resolve_lazy_export(name: str) -> object:
    module_name = _LAZY_EXPORT_MODULES.get(name)
    if module_name is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module = importlib.import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value


class _LazyExportProxy:
    def __init__(self, name: str) -> None:
        self._name = name

    def _resolve(self) -> object:
        return _resolve_lazy_export(self._name)

    def __call__(self, *args: object, **kwargs: object) -> object:
        target = self._resolve()
        if not callable(target):
            msg = f"Delta control-plane export {self._name!r} is not callable."
            raise TypeError(msg)
        return target(*args, **kwargs)

    def __getattr__(self, attr: str) -> object:
        return getattr(self._resolve(), attr)


if TYPE_CHECKING:
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_add_constraints,
        delta_add_features,
        delta_cleanup_metadata,
        delta_create_checkpoint,
        delta_disable_change_data_feed,
        delta_disable_check_constraints,
        delta_disable_checkpoint_protection,
        delta_disable_column_mapping,
        delta_disable_deletion_vectors,
        delta_disable_generated_columns,
        delta_disable_in_commit_timestamps,
        delta_disable_invariants,
        delta_disable_row_tracking,
        delta_disable_v2_checkpoints,
        delta_disable_vacuum_protocol_check,
        delta_drop_constraints,
        delta_enable_change_data_feed,
        delta_enable_check_constraints,
        delta_enable_checkpoint_protection,
        delta_enable_column_mapping,
        delta_enable_deletion_vectors,
        delta_enable_generated_columns,
        delta_enable_in_commit_timestamps,
        delta_enable_invariants,
        delta_enable_row_tracking,
        delta_enable_v2_checkpoints,
        delta_enable_vacuum_protocol_check,
        delta_optimize_compact,
        delta_restore,
        delta_set_properties,
        delta_vacuum,
    )
    from datafusion_engine.delta.control_plane_mutation import (
        delta_delete,
        delta_merge,
        delta_update,
        delta_write_ipc,
    )
    from datafusion_engine.delta.control_plane_provider import (
        delta_add_actions,
        delta_cdf_provider,
        delta_provider_from_session,
        delta_provider_with_files,
        delta_snapshot_info,
    )
else:
    delta_add_actions = _LazyExportProxy("delta_add_actions")
    delta_add_constraints = _LazyExportProxy("delta_add_constraints")
    delta_add_features = _LazyExportProxy("delta_add_features")
    delta_cdf_provider = _LazyExportProxy("delta_cdf_provider")
    delta_cleanup_metadata = _LazyExportProxy("delta_cleanup_metadata")
    delta_create_checkpoint = _LazyExportProxy("delta_create_checkpoint")
    delta_delete = _LazyExportProxy("delta_delete")
    delta_disable_change_data_feed = _LazyExportProxy("delta_disable_change_data_feed")
    delta_disable_check_constraints = _LazyExportProxy("delta_disable_check_constraints")
    delta_disable_checkpoint_protection = _LazyExportProxy("delta_disable_checkpoint_protection")
    delta_disable_column_mapping = _LazyExportProxy("delta_disable_column_mapping")
    delta_disable_deletion_vectors = _LazyExportProxy("delta_disable_deletion_vectors")
    delta_disable_generated_columns = _LazyExportProxy("delta_disable_generated_columns")
    delta_disable_in_commit_timestamps = _LazyExportProxy("delta_disable_in_commit_timestamps")
    delta_disable_invariants = _LazyExportProxy("delta_disable_invariants")
    delta_disable_row_tracking = _LazyExportProxy("delta_disable_row_tracking")
    delta_disable_v2_checkpoints = _LazyExportProxy("delta_disable_v2_checkpoints")
    delta_disable_vacuum_protocol_check = _LazyExportProxy("delta_disable_vacuum_protocol_check")
    delta_drop_constraints = _LazyExportProxy("delta_drop_constraints")
    delta_enable_change_data_feed = _LazyExportProxy("delta_enable_change_data_feed")
    delta_enable_check_constraints = _LazyExportProxy("delta_enable_check_constraints")
    delta_enable_checkpoint_protection = _LazyExportProxy("delta_enable_checkpoint_protection")
    delta_enable_column_mapping = _LazyExportProxy("delta_enable_column_mapping")
    delta_enable_deletion_vectors = _LazyExportProxy("delta_enable_deletion_vectors")
    delta_enable_generated_columns = _LazyExportProxy("delta_enable_generated_columns")
    delta_enable_in_commit_timestamps = _LazyExportProxy("delta_enable_in_commit_timestamps")
    delta_enable_invariants = _LazyExportProxy("delta_enable_invariants")
    delta_enable_row_tracking = _LazyExportProxy("delta_enable_row_tracking")
    delta_enable_v2_checkpoints = _LazyExportProxy("delta_enable_v2_checkpoints")
    delta_enable_vacuum_protocol_check = _LazyExportProxy("delta_enable_vacuum_protocol_check")
    delta_merge = _LazyExportProxy("delta_merge")
    delta_optimize_compact = _LazyExportProxy("delta_optimize_compact")
    delta_provider_from_session = _LazyExportProxy("delta_provider_from_session")
    delta_provider_with_files = _LazyExportProxy("delta_provider_with_files")
    delta_restore = _LazyExportProxy("delta_restore")
    delta_set_properties = _LazyExportProxy("delta_set_properties")
    delta_snapshot_info = _LazyExportProxy("delta_snapshot_info")
    delta_update = _LazyExportProxy("delta_update")
    delta_vacuum = _LazyExportProxy("delta_vacuum")
    delta_write_ipc = _LazyExportProxy("delta_write_ipc")


def __getattr__(name: str) -> object:
    return _resolve_lazy_export(name)


__all__ = [
    "DeltaAddConstraintsRequest",
    "DeltaAddFeaturesRequest",
    "DeltaAppTransaction",
    "DeltaCdfProviderBundle",
    "DeltaCdfRequest",
    "DeltaCheckpointRequest",
    "DeltaCommitOptions",
    "DeltaDeleteRequest",
    "DeltaDropConstraintsRequest",
    "DeltaFeatureEnableRequest",
    "DeltaFeatureGate",
    "DeltaMergeRequest",
    "DeltaOptimizeRequest",
    "DeltaProviderBundle",
    "DeltaProviderRequest",
    "DeltaRestoreRequest",
    "DeltaSetPropertiesRequest",
    "DeltaSnapshotRequest",
    "DeltaTableRef",
    "DeltaUpdateRequest",
    "DeltaVacuumRequest",
    "DeltaWriteRequest",
    "delta_add_actions",
    "delta_add_constraints",
    "delta_add_features",
    "delta_cdf_provider",
    "delta_cleanup_metadata",
    "delta_create_checkpoint",
    "delta_delete",
    "delta_disable_change_data_feed",
    "delta_disable_check_constraints",
    "delta_disable_checkpoint_protection",
    "delta_disable_column_mapping",
    "delta_disable_deletion_vectors",
    "delta_disable_generated_columns",
    "delta_disable_in_commit_timestamps",
    "delta_disable_invariants",
    "delta_disable_row_tracking",
    "delta_disable_v2_checkpoints",
    "delta_disable_vacuum_protocol_check",
    "delta_drop_constraints",
    "delta_enable_change_data_feed",
    "delta_enable_check_constraints",
    "delta_enable_checkpoint_protection",
    "delta_enable_column_mapping",
    "delta_enable_deletion_vectors",
    "delta_enable_generated_columns",
    "delta_enable_in_commit_timestamps",
    "delta_enable_invariants",
    "delta_enable_row_tracking",
    "delta_enable_v2_checkpoints",
    "delta_enable_vacuum_protocol_check",
    "delta_merge",
    "delta_optimize_compact",
    "delta_provider_from_session",
    "delta_provider_with_files",
    "delta_restore",
    "delta_set_properties",
    "delta_snapshot_info",
    "delta_update",
    "delta_vacuum",
    "delta_write_ipc",
]
