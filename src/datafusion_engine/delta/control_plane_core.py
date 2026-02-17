"""Rust-backed Delta control-plane adapters.

This module centralizes access to the Rust Delta control plane exposed via
``datafusion_ext``. It provides a typed, canonical surface for:
1. Snapshot and protocol-aware metadata.
2. Provider construction with session-derived scan configuration.
3. File-pruned provider construction for scan planning.
4. Maintenance operations (optimize/vacuum/checkpoint).
"""

# ruff: noqa: DOC201
# NOTE(size-exception): This module is temporarily >800 LOC during hard-cutover
# decomposition. Remaining extraction and contraction work is tracked in
# docs/plans/src_design_improvements_implementation_plan_v1_2026-02-16.md.

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from typing import NoReturn, cast

import msgspec
import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.arrow.abi import schema_to_dict
from datafusion_engine.delta.capabilities import (
    DeltaExtensionCompatibility,
    is_delta_extension_compatible,
    resolve_delta_extension_module,
)
from datafusion_engine.delta.commit_payload import commit_payload_parts
from datafusion_engine.delta.payload import (
    cdf_options_payload,
    commit_payload,
)
from datafusion_engine.delta.protocol import delta_feature_gate_rust_payload
from datafusion_engine.delta.protocols import (
    DeltaProviderHandle,
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
from datafusion_engine.generated.delta_types import (
    DeltaFeatureGate,
)
from schema_spec.dataset_spec import DeltaScanOptions
from serde_msgspec import StructBaseStrict, ensure_raw
from utils.validation import ensure_mapping
from utils.value_coercion import coerce_mapping_list


class DeltaTableRef(StructBaseStrict, frozen=True):
    """Reference to a Delta table with versioning context."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None


class DeltaProviderBundle(StructBaseStrict, frozen=True):
    """Provider response with canonical control-plane metadata."""

    provider: DeltaProviderHandle
    snapshot: Mapping[str, object]
    scan_config: Mapping[str, object]
    scan_effective: dict[str, object]
    add_actions: Sequence[Mapping[str, object]] | None = None
    predicate_error: str | None = None


class DeltaCdfProviderBundle(StructBaseStrict, frozen=True):
    """CDF provider response with canonical control-plane metadata."""

    provider: DeltaProviderHandle
    snapshot: Mapping[str, object]
    cdf_options: DeltaCdfOptions | None


class DeltaSnapshotRequest(StructBaseStrict, frozen=True):
    """Inputs required to resolve a Delta snapshot."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    gate: DeltaFeatureGate | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaProviderRequest(StructBaseStrict, frozen=True):
    """Inputs required to construct a Delta table provider."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    delta_scan: DeltaScanOptions | None
    predicate: str | None = None
    gate: DeltaFeatureGate | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaCdfRequest(StructBaseStrict, frozen=True):
    """Inputs required to construct a Delta CDF provider."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    options: DeltaCdfOptions | None
    gate: DeltaFeatureGate | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaWriteRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native Delta write."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    data_ipc: msgspec.Raw
    mode: str
    schema_mode: str | None
    partition_columns: Sequence[str] | None
    target_file_size: int | None
    extra_constraints: Sequence[str] | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    def __post_init__(self) -> None:
        """Normalize the IPC payload into msgspec.Raw."""
        object.__setattr__(self, "data_ipc", ensure_raw(self.data_ipc))

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaDeleteRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native Delta delete."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    predicate: str | None
    extra_constraints: Sequence[str] | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaUpdateRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native Delta update."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    predicate: str | None
    updates: Mapping[str, str]
    extra_constraints: Sequence[str] | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaMergeRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native Delta merge."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    source_table: str
    predicate: str
    source_alias: str | None
    target_alias: str | None
    matched_predicate: str | None
    matched_updates: Mapping[str, str]
    not_matched_predicate: str | None
    not_matched_inserts: Mapping[str, str]
    not_matched_by_source_predicate: str | None
    delete_not_matched_by_source: bool
    extra_constraints: Sequence[str] | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaOptimizeRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native optimize/compact."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    target_size: int | None
    z_order_cols: Sequence[str] | None = None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaVacuumRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native vacuum."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    retention_hours: int | None
    dry_run: bool
    enforce_retention_duration: bool
    require_vacuum_protocol_check: bool = False
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaRestoreRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native restore."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    restore_version: int | None
    restore_timestamp: str | None
    allow_unsafe_restore: bool = False
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaSetPropertiesRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native property update."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    properties: Mapping[str, str]
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaAddFeaturesRequest(StructBaseStrict, frozen=True):
    """Inputs required to run a Rust-native feature enablement."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    features: Sequence[str]
    allow_protocol_versions_increase: bool
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaFeatureEnableRequest(StructBaseStrict, frozen=True):
    """Inputs required to enable a Delta protocol feature set."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaAddConstraintsRequest(StructBaseStrict, frozen=True):
    """Inputs required to add Delta check constraints."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    constraints: Mapping[str, str]
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaDropConstraintsRequest(StructBaseStrict, frozen=True):
    """Inputs required to drop Delta check constraints."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    constraints: Sequence[str]
    raise_if_not_exists: bool = True
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


class DeltaCheckpointRequest(StructBaseStrict, frozen=True):
    """Inputs required to create a Delta checkpoint or cleanup metadata."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    gate: DeltaFeatureGate | None = None

    @property
    def table_ref(self) -> DeltaTableRef:
        """Return the request table reference."""
        return DeltaTableRef(
            table_uri=self.table_uri,
            storage_options=self.storage_options,
            version=self.version,
            timestamp=self.timestamp,
        )


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


def delta_provider_from_session(
    ctx: SessionContext,
    *,
    request: DeltaProviderRequest,
) -> DeltaProviderBundle:
    """Build a Delta provider with session-derived scan configuration.

    Returns:
    -------
    DeltaProviderBundle
        Provider bundle and control-plane metadata.
    """
    from datafusion_engine.delta.control_plane_provider import (
        delta_provider_from_session as _delta_provider_from_session,
    )

    return _delta_provider_from_session(ctx, request=request)


def delta_provider_with_files(
    ctx: SessionContext,
    *,
    files: Sequence[str],
    request: DeltaProviderRequest,
) -> DeltaProviderBundle:
    """Build a file-pruned Delta provider.

    Parameters
    ----------
    ctx
        DataFusion session context for provider construction.
    files
        File list restricting the provider scan.
    request
        Provider request describing scan configuration and Delta gates.

    Returns:
    -------
    DeltaProviderBundle
        Provider capsule and control-plane metadata.

    """
    from datafusion_engine.delta.control_plane_provider import (
        delta_provider_with_files as _delta_provider_with_files,
    )

    return _delta_provider_with_files(
        ctx,
        files=files,
        request=request,
    )


def delta_cdf_provider(
    *,
    request: DeltaCdfRequest,
) -> DeltaCdfProviderBundle:
    """Build a Delta CDF provider through the Rust control plane."""
    from datafusion_engine.delta.control_plane_provider import (
        delta_cdf_provider as _delta_cdf_provider,
    )

    return _delta_cdf_provider(request=request)


def delta_snapshot_info(
    request: DeltaSnapshotRequest,
) -> Mapping[str, object]:
    """Load protocol-aware Delta snapshot metadata."""
    from datafusion_engine.delta.control_plane_provider import (
        delta_snapshot_info as _delta_snapshot_info,
    )

    return _delta_snapshot_info(request)


def delta_add_actions(
    request: DeltaSnapshotRequest,
) -> Mapping[str, object]:
    """Return protocol-aware snapshot info and add actions."""
    from datafusion_engine.delta.control_plane_provider import (
        delta_add_actions as _delta_add_actions,
    )

    return _delta_add_actions(request)


def delta_write_ipc(
    ctx: SessionContext,
    *,
    request: DeltaWriteRequest,
) -> Mapping[str, object]:
    """Run a Rust-native Delta write over Arrow IPC bytes.

    Parameters
    ----------
    ctx
        DataFusion session context for the write.
    request
        Write request describing target table and commit options.

    Returns:
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    """
    write_fn = _require_internal_entrypoint("delta_write_ipc")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    commit_parts = commit_payload_parts(commit_payload(request.commit_options))
    constraints_payload = list(request.extra_constraints) if request.extra_constraints else None
    partitions_payload = list(request.partition_columns) if request.partition_columns else None
    response = write_fn(
        _internal_ctx(ctx, entrypoint="delta_write_ipc"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        bytes(request.data_ipc),
        request.mode,
        request.schema_mode,
        partitions_payload,
        request.target_file_size,
        constraints_payload,
        *delta_feature_gate_rust_payload(request.gate),
        commit_parts.metadata_payload,
        commit_parts.app_id,
        commit_parts.app_version,
        commit_parts.app_last_updated,
        commit_parts.max_retries,
        commit_parts.create_checkpoint,
    )
    return ensure_mapping(response, label="delta_write_ipc")


def delta_delete(
    ctx: SessionContext,
    *,
    request: DeltaDeleteRequest,
) -> Mapping[str, object]:
    """Run a Rust-native Delta delete.

    Parameters
    ----------
    ctx
        DataFusion session context for the delete.
    request
        Delete request describing target table and predicate.

    Returns:
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    """
    delete_fn = _require_internal_entrypoint("delta_delete")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    commit_parts = commit_payload_parts(commit_payload(request.commit_options))
    constraints_payload = list(request.extra_constraints) if request.extra_constraints else None
    response = delete_fn(
        _internal_ctx(ctx, entrypoint="delta_delete"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.predicate,
        constraints_payload,
        *delta_feature_gate_rust_payload(request.gate),
        commit_parts.metadata_payload,
        commit_parts.app_id,
        commit_parts.app_version,
        commit_parts.app_last_updated,
        commit_parts.max_retries,
        commit_parts.create_checkpoint,
    )
    return ensure_mapping(response, label="delta_delete")


def _validate_update_constraints(ctx: SessionContext, request: DeltaUpdateRequest) -> None:
    if not request.extra_constraints:
        return
    # Delta constraint enforcement is delegated to the Rust control-plane
    # mutation entrypoint via ``constraints_payload`` for update operations.
    # Python-surface delta_data_checker probes are intentionally removed.
    _ = ctx


def delta_update(
    ctx: SessionContext,
    *,
    request: DeltaUpdateRequest,
) -> Mapping[str, object]:
    """Run a Rust-native Delta update.

    Parameters
    ----------
    ctx
        DataFusion session context for the update.
    request
        Update request describing target table and assignments.

    Returns:
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    """
    if not request.updates:
        msg = "Delta update requires at least one column assignment."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)
    _validate_update_constraints(ctx, request)
    update_fn = _require_internal_entrypoint("delta_update")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    commit_parts = commit_payload_parts(commit_payload(request.commit_options))
    constraints_payload = list(request.extra_constraints) if request.extra_constraints else None
    updates_payload = sorted((str(key), str(value)) for key, value in request.updates.items())
    response = update_fn(
        _internal_ctx(ctx, entrypoint="delta_update"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.predicate,
        updates_payload,
        constraints_payload,
        *delta_feature_gate_rust_payload(request.gate),
        commit_parts.metadata_payload,
        commit_parts.app_id,
        commit_parts.app_version,
        commit_parts.app_last_updated,
        commit_parts.max_retries,
        commit_parts.create_checkpoint,
    )
    return ensure_mapping(response, label="delta_update")


def delta_merge(
    ctx: SessionContext,
    *,
    request: DeltaMergeRequest,
) -> Mapping[str, object]:
    """Run a Rust-native Delta merge.

    Parameters
    ----------
    ctx
        DataFusion session context for the merge.
    request
        Merge request describing source/target expressions and predicates.

    Returns:
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    """
    merge_fn = _require_internal_entrypoint("delta_merge")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    commit_parts = commit_payload_parts(commit_payload(request.commit_options))
    constraints_payload = list(request.extra_constraints) if request.extra_constraints else None
    matched_payload = sorted(
        (str(key), str(value)) for key, value in request.matched_updates.items()
    )
    inserts_payload = sorted(
        (str(key), str(value)) for key, value in request.not_matched_inserts.items()
    )
    response = merge_fn(
        _internal_ctx(
            ctx,
            entrypoint="delta_merge",
        ),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.source_table,
        request.predicate,
        request.source_alias,
        request.target_alias,
        request.matched_predicate,
        matched_payload,
        request.not_matched_predicate,
        inserts_payload,
        request.not_matched_by_source_predicate,
        request.delete_not_matched_by_source,
        constraints_payload,
        *delta_feature_gate_rust_payload(request.gate),
        commit_parts.metadata_payload,
        commit_parts.app_id,
        commit_parts.app_version,
        commit_parts.app_last_updated,
        commit_parts.max_retries,
        commit_parts.create_checkpoint,
    )
    return ensure_mapping(response, label="delta_merge")


def delta_optimize_compact(
    ctx: SessionContext,
    *,
    request: DeltaOptimizeRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta optimize/compact.

    Returns:
    -------
    Mapping[str, object]
        Control-plane optimize report payload.
    """
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_optimize_compact as _delta_optimize_compact,
    )

    return _delta_optimize_compact(ctx, request=request)


def delta_vacuum(
    ctx: SessionContext,
    *,
    request: DeltaVacuumRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta vacuum.

    Returns:
    -------
    Mapping[str, object]
        Control-plane vacuum report payload.
    """
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_vacuum as _delta_vacuum,
    )

    return _delta_vacuum(ctx, request=request)


def delta_restore(
    ctx: SessionContext,
    *,
    request: DeltaRestoreRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta restore."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_restore as _delta_restore,
    )

    return _delta_restore(ctx, request=request)


def delta_set_properties(
    ctx: SessionContext,
    *,
    request: DeltaSetPropertiesRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta property updates."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_set_properties as _delta_set_properties,
    )

    return _delta_set_properties(ctx, request=request)


def delta_add_features(
    ctx: SessionContext,
    *,
    request: DeltaAddFeaturesRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta feature enablement."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_add_features as _delta_add_features,
    )

    return _delta_add_features(ctx, request=request)


def delta_add_constraints(
    ctx: SessionContext,
    *,
    request: DeltaAddConstraintsRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta add-constraints."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_add_constraints as _delta_add_constraints,
    )

    return _delta_add_constraints(ctx, request=request)


def delta_drop_constraints(
    ctx: SessionContext,
    *,
    request: DeltaDropConstraintsRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta drop-constraints."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_drop_constraints as _delta_drop_constraints,
    )

    return _delta_drop_constraints(ctx, request=request)


def delta_create_checkpoint(
    ctx: SessionContext,
    *,
    request: DeltaCheckpointRequest,
) -> Mapping[str, object]:
    """Create a Delta checkpoint for the requested table.

    Returns:
    -------
    Mapping[str, object]
        Control-plane checkpoint report payload.
    """
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_create_checkpoint as _delta_create_checkpoint,
    )

    return _delta_create_checkpoint(ctx, request=request)


def delta_cleanup_metadata(
    ctx: SessionContext,
    *,
    request: DeltaCheckpointRequest,
) -> Mapping[str, object]:
    """Clean expired Delta log metadata for the requested table."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_cleanup_metadata as _delta_cleanup_metadata,
    )

    return _delta_cleanup_metadata(ctx, request=request)


def delta_enable_column_mapping(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    mode: str = "name",
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta column mapping with the desired mode."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_enable_column_mapping as _delta_enable_column_mapping,
    )

    return _delta_enable_column_mapping(
        ctx,
        request=request,
        mode=mode,
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_deletion_vectors(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta deletion vectors (table feature + property)."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_enable_deletion_vectors as _delta_enable_deletion_vectors,
    )

    return _delta_enable_deletion_vectors(
        ctx,
        request=request,
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_row_tracking(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta row tracking (table feature + property)."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_enable_row_tracking as _delta_enable_row_tracking,
    )

    return _delta_enable_row_tracking(
        ctx,
        request=request,
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_change_data_feed(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta Change Data Feed (table feature + property)."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_enable_change_data_feed as _delta_enable_change_data_feed,
    )

    return _delta_enable_change_data_feed(
        ctx,
        request=request,
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_generated_columns(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta generated columns feature."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_enable_generated_columns as _delta_enable_generated_columns,
    )

    return _delta_enable_generated_columns(
        ctx,
        request=request,
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_invariants(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta invariants feature."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_enable_invariants as _delta_enable_invariants,
    )

    return _delta_enable_invariants(
        ctx,
        request=request,
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_check_constraints(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta check constraints feature."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_enable_check_constraints as _delta_enable_check_constraints,
    )

    return _delta_enable_check_constraints(
        ctx,
        request=request,
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_in_commit_timestamps(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    enablement_version: int | None = None,
    enablement_timestamp: str | None = None,
) -> Mapping[str, object]:
    """Enable Delta in-commit timestamps via table properties."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_enable_in_commit_timestamps as _delta_enable_in_commit_timestamps,
    )

    return _delta_enable_in_commit_timestamps(
        ctx,
        request=request,
        enablement_version=enablement_version,
        enablement_timestamp=enablement_timestamp,
    )


def delta_enable_v2_checkpoints(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta v2 checkpoints via feature + checkpoint policy property."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_enable_v2_checkpoints as _delta_enable_v2_checkpoints,
    )

    return _delta_enable_v2_checkpoints(
        ctx,
        request=request,
        allow_protocol_versions_increase=allow_protocol_versions_increase,
    )


def delta_enable_vacuum_protocol_check(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Enable vacuum protocol checks via table properties (best-effort)."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_enable_vacuum_protocol_check as _delta_enable_vacuum_protocol_check,
    )

    return _delta_enable_vacuum_protocol_check(ctx, request=request)


def delta_enable_checkpoint_protection(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Enable checkpoint protection via table properties (best-effort)."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_enable_checkpoint_protection as _delta_enable_checkpoint_protection,
    )

    return _delta_enable_checkpoint_protection(ctx, request=request)


def delta_disable_change_data_feed(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta Change Data Feed via table properties (best-effort)."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_disable_change_data_feed as _delta_disable_change_data_feed,
    )

    return _delta_disable_change_data_feed(ctx, request=request)


def delta_disable_deletion_vectors(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta deletion vectors via table properties (best-effort)."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_disable_deletion_vectors as _delta_disable_deletion_vectors,
    )

    return _delta_disable_deletion_vectors(ctx, request=request)


def delta_disable_row_tracking(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta row tracking via table properties (best-effort)."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_disable_row_tracking as _delta_disable_row_tracking,
    )

    return _delta_disable_row_tracking(ctx, request=request)


def delta_disable_in_commit_timestamps(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta in-commit timestamps via table properties (best-effort)."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_disable_in_commit_timestamps as _delta_disable_in_commit_timestamps,
    )

    return _delta_disable_in_commit_timestamps(ctx, request=request)


def delta_disable_vacuum_protocol_check(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable vacuum protocol check flag via table properties (best-effort)."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_disable_vacuum_protocol_check as _delta_disable_vacuum_protocol_check,
    )

    return _delta_disable_vacuum_protocol_check(ctx, request=request)


def delta_disable_checkpoint_protection(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable checkpoint protection via table properties (best-effort)."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_disable_checkpoint_protection as _delta_disable_checkpoint_protection,
    )

    return _delta_disable_checkpoint_protection(ctx, request=request)


def delta_disable_column_mapping(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta column mapping cannot be disabled safely."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_disable_column_mapping as _delta_disable_column_mapping,
    )

    _delta_disable_column_mapping(*_args, **_kwargs)


def delta_disable_generated_columns(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta generated columns cannot be disabled safely."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_disable_generated_columns as _delta_disable_generated_columns,
    )

    _delta_disable_generated_columns(*_args, **_kwargs)


def delta_disable_invariants(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta invariants cannot be disabled safely."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_disable_invariants as _delta_disable_invariants,
    )

    _delta_disable_invariants(*_args, **_kwargs)


def delta_disable_check_constraints(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta check constraints must be dropped individually."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_disable_check_constraints as _delta_disable_check_constraints,
    )

    _delta_disable_check_constraints(*_args, **_kwargs)


def delta_disable_v2_checkpoints(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta v2 checkpoints cannot be disabled safely."""
    from datafusion_engine.delta.control_plane_maintenance import (
        delta_disable_v2_checkpoints as _delta_disable_v2_checkpoints,
    )

    _delta_disable_v2_checkpoints(*_args, **_kwargs)


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
