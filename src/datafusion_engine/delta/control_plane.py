"""Rust-backed Delta control-plane adapters.

This module centralizes access to the Rust Delta control plane exposed via
``datafusion._internal``. It provides a typed, canonical surface for:
1. Snapshot and protocol-aware metadata.
2. Provider construction with session-derived scan configuration.
3. File-pruned provider construction for scan planning.
4. Maintenance operations (optimize/vacuum/checkpoint).
"""

from __future__ import annotations

import base64
import importlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, NoReturn, Protocol, cast

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.arrow.abi import schema_to_dict
from datafusion_engine.delta.capabilities import is_delta_extension_compatible
from datafusion_engine.delta.payload import (
    cdf_options_payload,
    commit_payload,
    schema_ipc_payload,
)
from datafusion_engine.delta.protocol import delta_feature_gate_rust_payload
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.generated.delta_types import (
    DeltaAppTransaction,
    DeltaCommitOptions,
    DeltaFeatureGate,
)
from schema_spec.system import DeltaScanOptions
from utils.validation import ensure_mapping
from utils.value_coercion import coerce_mapping_list

if TYPE_CHECKING:
    from storage.deltalake.delta import DeltaCdfOptions

_DELTA_INTERNAL_CTX: dict[str, InternalSessionContext | None] = {"value": None}


class InternalSessionContext(Protocol):
    """Protocol representing the internal DataFusion session context."""


class _DeltaCdfExtension(Protocol):
    def delta_cdf_table_provider(self, *args: object, **kwargs: object) -> object: ...


@dataclass(frozen=True)
class DeltaProviderBundle:
    """Provider response with canonical control-plane metadata."""

    provider: object
    snapshot: Mapping[str, object]
    scan_config: Mapping[str, object]
    scan_effective: dict[str, object]
    add_actions: Sequence[Mapping[str, object]] | None = None
    predicate_error: str | None = None


@dataclass(frozen=True)
class DeltaCdfProviderBundle:
    """CDF provider response with canonical control-plane metadata."""

    provider: object
    snapshot: Mapping[str, object]
    cdf_options: DeltaCdfOptions | None


@dataclass(frozen=True)
class DeltaSnapshotRequest:
    """Inputs required to resolve a Delta snapshot."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    gate: DeltaFeatureGate | None = None


@dataclass(frozen=True)
class DeltaProviderRequest:
    """Inputs required to construct a Delta table provider."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    delta_scan: DeltaScanOptions | None
    predicate: str | None = None
    gate: DeltaFeatureGate | None = None


@dataclass(frozen=True)
class DeltaCdfRequest:
    """Inputs required to construct a Delta CDF provider."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    options: DeltaCdfOptions | None
    gate: DeltaFeatureGate | None = None


@dataclass(frozen=True)
class DeltaWriteRequest:
    """Inputs required to run a Rust-native Delta write."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    data_ipc: bytes
    mode: str
    schema_mode: str | None
    partition_columns: Sequence[str] | None
    target_file_size: int | None
    extra_constraints: Sequence[str] | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None


@dataclass(frozen=True)
class DeltaDeleteRequest:
    """Inputs required to run a Rust-native Delta delete."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    predicate: str | None
    extra_constraints: Sequence[str] | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None


@dataclass(frozen=True)
class DeltaUpdateRequest:
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


@dataclass(frozen=True)
class DeltaMergeRequest:
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


@dataclass(frozen=True)
class DeltaOptimizeRequest:
    """Inputs required to run a Rust-native optimize/compact."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    target_size: int | None
    z_order_cols: Sequence[str] | None = None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None


@dataclass(frozen=True)
class DeltaVacuumRequest:
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


@dataclass(frozen=True)
class DeltaRestoreRequest:
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


@dataclass(frozen=True)
class DeltaSetPropertiesRequest:
    """Inputs required to run a Rust-native property update."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    properties: Mapping[str, str]
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None


@dataclass(frozen=True)
class DeltaAddFeaturesRequest:
    """Inputs required to run a Rust-native feature enablement."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    features: Sequence[str]
    allow_protocol_versions_increase: bool
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None


@dataclass(frozen=True)
class DeltaFeatureEnableRequest:
    """Inputs required to enable a Delta protocol feature set."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None


@dataclass(frozen=True)
class DeltaAddConstraintsRequest:
    """Inputs required to add Delta check constraints."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    constraints: Mapping[str, str]
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None


@dataclass(frozen=True)
class DeltaDropConstraintsRequest:
    """Inputs required to drop Delta check constraints."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    constraints: Sequence[str]
    raise_if_not_exists: bool = True
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptions | None = None


@dataclass(frozen=True)
class DeltaCheckpointRequest:
    """Inputs required to create a Delta checkpoint or cleanup metadata."""

    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None
    gate: DeltaFeatureGate | None = None


def _resolve_extension_module(
    *,
    required_attr: str | None = None,
    entrypoint: str | None = None,
) -> object:
    """Return the Delta extension module (datafusion._internal or datafusion_ext).

    Raises
    ------
    DataFusionEngineError
        Raised when the required extension module is unavailable.

    Returns
    -------
    object
        Resolved extension module implementing the requested entrypoints.
    """
    for module_name in ("datafusion._internal", "datafusion_ext"):
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        if required_attr is not None and not hasattr(module, required_attr):
            continue
        if entrypoint is not None and not callable(getattr(module, entrypoint, None)):
            continue
        return module
    msg = "Delta control-plane operations require datafusion._internal or datafusion_ext."
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


def _internal_ctx(ctx: SessionContext) -> InternalSessionContext:
    """Return the internal session context required by Rust entrypoints.

    Returns
    -------
    InternalSessionContext
        Internal DataFusion session context used by Rust entrypoints.
    """
    internal_ctx = getattr(ctx, "ctx", None)
    if internal_ctx is None:
        msg = "Delta entrypoints require datafusion._internal.SessionContext (use ctx.ctx)."
        _raise_engine_error(msg, kind=ErrorKind.PLUGIN)
    compatibility = is_delta_extension_compatible(ctx)
    if not compatibility.available:
        msg = "Delta control-plane extension module is unavailable."
        _raise_engine_error(msg, kind=ErrorKind.PLUGIN)
    if not compatibility.compatible:
        alt_ctx = _fallback_delta_internal_ctx()
        if alt_ctx is not None:
            return alt_ctx
        details = f" {compatibility.error}" if compatibility.error else ""
        msg = f"Delta control-plane extension is incompatible.{details}"
        _raise_engine_error(msg, kind=ErrorKind.PLUGIN)
    return cast("InternalSessionContext", internal_ctx)


def _fallback_delta_internal_ctx() -> InternalSessionContext | None:
    cached = _DELTA_INTERNAL_CTX["value"]
    if cached is not None:
        return cached
    try:
        module = _resolve_extension_module(entrypoint="delta_session_context")
    except DataFusionEngineError:
        return None
    builder = getattr(module, "delta_session_context", None)
    if not callable(builder):
        return None
    try:
        delta_ctx = builder()
    except (TypeError, ValueError):
        try:
            delta_ctx = builder(None, None, None)
        except (TypeError, ValueError):
            return None
    internal_ctx = getattr(delta_ctx, "ctx", None)
    resolved = internal_ctx if internal_ctx is not None else delta_ctx
    _DELTA_INTERNAL_CTX["value"] = cast("InternalSessionContext", resolved)
    return _DELTA_INTERNAL_CTX["value"]


def _require_internal_entrypoint(name: str) -> Callable[..., object]:
    module = _resolve_extension_module(entrypoint=name)
    entrypoint = getattr(module, name, None)
    if not callable(entrypoint):
        msg = f"Delta control-plane entrypoint {name} is unavailable."
        raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN)
    return entrypoint


def _parse_add_actions(payload: object | None) -> Sequence[Mapping[str, object]] | None:
    return coerce_mapping_list(payload)


def _decode_schema_ipc(payload: bytes) -> pa.Schema:
    """Decode Arrow schema IPC bytes into a ``pyarrow.Schema``.

    Returns
    -------
    pyarrow.Schema
        Decoded Arrow schema.

    """
    try:
        return pa.ipc.read_schema(pa.BufferReader(payload))
    except (pa.ArrowInvalid, TypeError, ValueError) as exc:
        msg = "Invalid Delta scan schema IPC payload."
        _raise_engine_error(msg, kind=ErrorKind.ARROW, exc=exc)


def _cdf_options_to_ext(module: object, options: DeltaCdfOptions | None) -> object:
    """Convert Python CDF options into the Rust extension options type.

    Returns
    -------
    object
        Rust extension options value.

    """
    options_type = getattr(module, "DeltaCdfOptions", None)
    if options_type is None:
        msg = "Delta CDF options type is unavailable in the extension module."
        _raise_engine_error(msg, kind=ErrorKind.PLUGIN)
    ext_options = options_type()
    payload = cdf_options_payload(options)
    ext_options.starting_version = payload["starting_version"]
    ext_options.ending_version = payload["ending_version"]
    ext_options.starting_timestamp = payload["starting_timestamp"]
    ext_options.ending_timestamp = payload["ending_timestamp"]
    ext_options.allow_out_of_range = bool(payload["allow_out_of_range"])
    return ext_options


def _scan_effective_payload(payload: Mapping[str, object]) -> dict[str, object]:
    """Normalize scan-config payloads into an effective scan snapshot.

    Returns
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

    Parameters
    ----------
    ctx
        DataFusion session context for provider construction.
    request
        Provider request describing scan configuration and Delta gates.

    Returns
    -------
    DeltaProviderBundle
        Provider capsule and control-plane metadata.

    """
    provider_factory = _require_internal_entrypoint("delta_table_provider_from_session")
    schema_ipc = schema_ipc_payload(request.delta_scan.schema) if request.delta_scan else None
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    response = provider_factory(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.predicate,
        request.delta_scan.file_column_name if request.delta_scan else None,
        request.delta_scan.enable_parquet_pushdown if request.delta_scan else None,
        request.delta_scan.schema_force_view_types if request.delta_scan else None,
        request.delta_scan.wrap_partition_values if request.delta_scan else None,
        schema_ipc,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
    )
    payload = ensure_mapping(response, label="delta_table_provider_from_session")
    snapshot = ensure_mapping(payload.get("snapshot"), label="snapshot")
    scan_config = ensure_mapping(payload.get("scan_config"), label="scan_config")
    provider = payload.get("provider")
    if provider is None:
        msg = "Delta control-plane response missing provider capsule."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)
    return DeltaProviderBundle(
        provider=provider,
        snapshot=snapshot,
        scan_config=scan_config,
        scan_effective=_scan_effective_payload(scan_config),
        add_actions=_parse_add_actions(payload.get("add_actions")),
        predicate_error=cast("str | None", payload.get("predicate_error")),
    )


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

    Returns
    -------
    DeltaProviderBundle
        Provider capsule and control-plane metadata.

    """
    provider_factory = _require_internal_entrypoint("delta_table_provider_with_files")
    schema_ipc = schema_ipc_payload(request.delta_scan.schema) if request.delta_scan else None
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    response = provider_factory(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        list(files),
        request.delta_scan.file_column_name if request.delta_scan else None,
        request.delta_scan.enable_parquet_pushdown if request.delta_scan else None,
        request.delta_scan.schema_force_view_types if request.delta_scan else None,
        request.delta_scan.wrap_partition_values if request.delta_scan else None,
        schema_ipc,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
    )
    payload = ensure_mapping(response, label="delta_table_provider_with_files")
    snapshot = ensure_mapping(payload.get("snapshot"), label="snapshot")
    scan_config = ensure_mapping(payload.get("scan_config"), label="scan_config")
    provider = payload.get("provider")
    if provider is None:
        msg = "Delta control-plane response missing provider capsule."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)
    return DeltaProviderBundle(
        provider=provider,
        snapshot=snapshot,
        scan_config=scan_config,
        scan_effective=_scan_effective_payload(scan_config),
        add_actions=_parse_add_actions(payload.get("add_actions")),
    )


def delta_cdf_provider(
    *,
    request: DeltaCdfRequest,
) -> DeltaCdfProviderBundle:
    """Build a Delta CDF provider through the Rust control plane.

    Parameters
    ----------
    request
        CDF provider request describing snapshot pins and CDF options.

    Returns
    -------
    DeltaCdfProviderBundle
        Provider capsule plus snapshot metadata and CDF options payload.

    """
    module = _resolve_extension_module(
        required_attr="DeltaCdfOptions",
        entrypoint="delta_cdf_table_provider",
    )
    provider_factory = cast("_DeltaCdfExtension", module).delta_cdf_table_provider
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    ext_options = _cdf_options_to_ext(module, request.options)
    response = provider_factory(
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        ext_options,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
    )
    payload = ensure_mapping(response, label="delta_cdf_table_provider")
    snapshot = ensure_mapping(payload.get("snapshot"), label="snapshot")
    provider = payload.get("provider")
    if provider is None:
        msg = "Delta control-plane CDF response missing provider capsule."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)
    return DeltaCdfProviderBundle(
        provider=provider,
        snapshot=snapshot,
        cdf_options=request.options,
    )


def delta_snapshot_info(
    request: DeltaSnapshotRequest,
) -> Mapping[str, object]:
    """Load protocol-aware Delta snapshot metadata.

    Parameters
    ----------
    request
        Snapshot request describing protocol gates and snapshot pinning.

    Returns
    -------
    Mapping[str, object]
        Snapshot metadata payload from the control plane.

    """
    snapshot_factory = _require_internal_entrypoint("delta_snapshot_info")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    try:
        response = snapshot_factory(
            request.table_uri,
            storage_payload,
            request.version,
            request.timestamp,
            gate_payload[0],
            gate_payload[1],
            gate_payload[2],
            gate_payload[3],
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = "Delta snapshot retrieval failed."
        _raise_engine_error(msg, kind=ErrorKind.DELTA, exc=exc)
    return ensure_mapping(response, label="delta_snapshot_info")


def delta_add_actions(
    request: DeltaSnapshotRequest,
) -> Mapping[str, object]:
    """Return protocol-aware snapshot info and add actions.

    Parameters
    ----------
    request
        Snapshot request describing protocol gates and snapshot pinning.

    Returns
    -------
    Mapping[str, object]
        Snapshot metadata plus Add-action payload.

    """
    add_actions_factory = _require_internal_entrypoint("delta_add_actions")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    response = add_actions_factory(
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
    )
    return ensure_mapping(response, label="delta_add_actions")


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

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    """
    write_fn = _require_internal_entrypoint("delta_write_ipc")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload_values = commit_payload(request.commit_options)
    constraints_payload = list(request.extra_constraints) if request.extra_constraints else None
    partitions_payload = list(request.partition_columns) if request.partition_columns else None
    response = write_fn(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.data_ipc,
        request.mode,
        request.schema_mode,
        partitions_payload,
        request.target_file_size,
        constraints_payload,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload_values[0],
        commit_payload_values[1],
        commit_payload_values[2],
        commit_payload_values[3],
        commit_payload_values[4],
        commit_payload_values[5],
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

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    """
    delete_fn = _require_internal_entrypoint("delta_delete")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload_values = commit_payload(request.commit_options)
    constraints_payload = list(request.extra_constraints) if request.extra_constraints else None
    response = delete_fn(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.predicate,
        constraints_payload,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload_values[0],
        commit_payload_values[1],
        commit_payload_values[2],
        commit_payload_values[3],
        commit_payload_values[4],
        commit_payload_values[5],
    )
    return ensure_mapping(response, label="delta_delete")


def _validate_update_constraints(ctx: SessionContext, request: DeltaUpdateRequest) -> None:
    if not request.extra_constraints:
        return
    from datafusion import col

    from storage.deltalake import DeltaDataCheckRequest, delta_data_checker

    bundle = delta_provider_from_session(
        ctx,
        request=DeltaProviderRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            delta_scan=None,
            gate=request.gate,
        ),
    )
    df = ctx.read_table(bundle.provider)
    if request.predicate:
        try:
            predicate_expr = df.parse_sql_expr(request.predicate)
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = "Delta update predicate parse failed."
            _raise_engine_error(msg, kind=ErrorKind.DATAFUSION, exc=exc)
        df = df.filter(predicate_expr)
    schema = df.schema()
    exprs = []
    for name in schema.names:
        if name in request.updates:
            try:
                expr = df.parse_sql_expr(request.updates[name]).alias(name)
            except (RuntimeError, TypeError, ValueError) as exc:
                msg = "Delta update expression parse failed."
                _raise_engine_error(msg, kind=ErrorKind.DATAFUSION, exc=exc)
            exprs.append(expr)
        else:
            exprs.append(col(name))
    updated = df.select(*exprs)
    violations = delta_data_checker(
        DeltaDataCheckRequest(
            ctx=ctx,
            table_path=request.table_uri,
            data=updated.to_arrow_table(),
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            extra_constraints=request.extra_constraints,
            gate=request.gate,
        )
    )
    if violations:
        msg = "Delta update constraint check failed."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)


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

    Returns
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
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload_values = commit_payload(request.commit_options)
    constraints_payload = list(request.extra_constraints) if request.extra_constraints else None
    updates_payload = sorted((str(key), str(value)) for key, value in request.updates.items())
    response = update_fn(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.predicate,
        updates_payload,
        constraints_payload,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload_values[0],
        commit_payload_values[1],
        commit_payload_values[2],
        commit_payload_values[3],
        commit_payload_values[4],
        commit_payload_values[5],
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

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    """
    merge_fn = _require_internal_entrypoint("delta_merge")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload_values = commit_payload(request.commit_options)
    constraints_payload = list(request.extra_constraints) if request.extra_constraints else None
    matched_payload = sorted(
        (str(key), str(value)) for key, value in request.matched_updates.items()
    )
    inserts_payload = sorted(
        (str(key), str(value)) for key, value in request.not_matched_inserts.items()
    )
    response = merge_fn(
        _internal_ctx(ctx),
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
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload_values[0],
        commit_payload_values[1],
        commit_payload_values[2],
        commit_payload_values[3],
        commit_payload_values[4],
        commit_payload_values[5],
    )
    return ensure_mapping(response, label="delta_merge")


def delta_optimize_compact(
    ctx: SessionContext,
    *,
    request: DeltaOptimizeRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta optimize/compact.

    Parameters
    ----------
    ctx
        DataFusion session context for optimize.
    request
        Optimize request describing target table and compaction sizing.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    """
    optimize_fn = _require_internal_entrypoint("delta_optimize_compact")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload_values = commit_payload(request.commit_options)
    z_order_payload = list(request.z_order_cols) if request.z_order_cols else None
    response = optimize_fn(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.target_size,
        z_order_payload,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload_values[0],
        commit_payload_values[1],
        commit_payload_values[2],
        commit_payload_values[3],
        commit_payload_values[4],
        commit_payload_values[5],
    )
    return ensure_mapping(response, label="delta_optimize_compact")


def delta_vacuum(
    ctx: SessionContext,
    *,
    request: DeltaVacuumRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta vacuum.

    Parameters
    ----------
    ctx
        DataFusion session context for vacuum.
    request
        Vacuum request describing retention and enforcement settings.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    """
    vacuum_fn = _require_internal_entrypoint("delta_vacuum")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload_values = commit_payload(request.commit_options)
    response = vacuum_fn(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.retention_hours,
        request.dry_run,
        request.enforce_retention_duration,
        request.require_vacuum_protocol_check,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload_values[0],
        commit_payload_values[1],
        commit_payload_values[2],
        commit_payload_values[3],
        commit_payload_values[4],
        commit_payload_values[5],
    )
    return ensure_mapping(response, label="delta_vacuum")


def delta_restore(
    ctx: SessionContext,
    *,
    request: DeltaRestoreRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta restore.

    Parameters
    ----------
    ctx
        DataFusion session context for restore.
    request
        Restore request describing target and restore pins.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    """
    restore_fn = _require_internal_entrypoint("delta_restore")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload_values = commit_payload(request.commit_options)
    response = restore_fn(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.restore_version,
        request.restore_timestamp,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload_values[0],
        commit_payload_values[1],
        commit_payload_values[2],
        commit_payload_values[3],
        commit_payload_values[4],
        commit_payload_values[5],
    )
    return ensure_mapping(response, label="delta_restore")


def delta_set_properties(
    ctx: SessionContext,
    *,
    request: DeltaSetPropertiesRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta property updates.

    Parameters
    ----------
    ctx
        DataFusion session context for property updates.
    request
        Property update request describing target and properties.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    """
    if not request.properties:
        msg = "Delta property update requires at least one key/value pair."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)
    set_fn = _require_internal_entrypoint("delta_set_properties")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload_values = commit_payload(request.commit_options)
    properties_payload = sorted((str(key), str(value)) for key, value in request.properties.items())
    response = set_fn(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        properties_payload,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload_values[0],
        commit_payload_values[1],
        commit_payload_values[2],
        commit_payload_values[3],
        commit_payload_values[4],
        commit_payload_values[5],
    )
    return ensure_mapping(response, label="delta_set_properties")


def delta_add_features(
    ctx: SessionContext,
    *,
    request: DeltaAddFeaturesRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta feature enablement.

    Parameters
    ----------
    ctx
        DataFusion session context for feature enablement.
    request
        Feature-enable request describing target table and features.

    Returns
    -------
    Mapping[str, object]
        Control-plane mutation report payload.

    """
    if not request.features:
        msg = "Delta add-features requires at least one feature name."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)
    add_fn = _require_internal_entrypoint("delta_add_features")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload_values = commit_payload(request.commit_options)
    features_payload = [str(feature) for feature in request.features]
    response = add_fn(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        features_payload,
        request.allow_protocol_versions_increase,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload_values[0],
        commit_payload_values[1],
        commit_payload_values[2],
        commit_payload_values[3],
        commit_payload_values[4],
        commit_payload_values[5],
    )
    return ensure_mapping(response, label="delta_add_features")


def delta_add_constraints(
    ctx: SessionContext,
    *,
    request: DeltaAddConstraintsRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta add-constraints.

    Returns
    -------
    Mapping[str, object]
        Control-plane response payload for the constraint update.

    """
    if not request.constraints:
        msg = "Delta add-constraints requires at least one constraint."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)
    add_fn = _require_internal_entrypoint("delta_add_constraints")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload_values = commit_payload(request.commit_options)
    constraints_payload = sorted(
        (str(name), str(expr)) for name, expr in request.constraints.items()
    )
    response = add_fn(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        constraints_payload,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload_values[0],
        commit_payload_values[1],
        commit_payload_values[2],
        commit_payload_values[3],
        commit_payload_values[4],
        commit_payload_values[5],
    )
    return ensure_mapping(response, label="delta_add_constraints")


def delta_drop_constraints(
    ctx: SessionContext,
    *,
    request: DeltaDropConstraintsRequest,
) -> Mapping[str, object]:
    """Run Rust-native Delta drop-constraints.

    Returns
    -------
    Mapping[str, object]
        Control-plane response payload for the constraint drop.

    """
    if not request.constraints:
        msg = "Delta drop-constraints requires at least one constraint name."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)
    drop_fn = _require_internal_entrypoint("delta_drop_constraints")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload_values = commit_payload(request.commit_options)
    response = drop_fn(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        [str(name) for name in request.constraints],
        request.raise_if_not_exists,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload_values[0],
        commit_payload_values[1],
        commit_payload_values[2],
        commit_payload_values[3],
        commit_payload_values[4],
        commit_payload_values[5],
    )
    return ensure_mapping(response, label="delta_drop_constraints")


def delta_create_checkpoint(
    ctx: SessionContext,
    *,
    request: DeltaCheckpointRequest,
) -> Mapping[str, object]:
    """Create a Delta checkpoint for the requested table.

    Returns
    -------
    Mapping[str, object]
        Control-plane response payload for the checkpoint request.

    """
    checkpoint_fn = _require_internal_entrypoint("delta_create_checkpoint")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    response = checkpoint_fn(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
    )
    return ensure_mapping(response, label="delta_create_checkpoint")


def delta_cleanup_metadata(
    ctx: SessionContext,
    *,
    request: DeltaCheckpointRequest,
) -> Mapping[str, object]:
    """Clean expired Delta log metadata for the requested table.

    Returns
    -------
    Mapping[str, object]
        Control-plane response payload for the cleanup request.

    """
    cleanup_fn = _require_internal_entrypoint("delta_cleanup_metadata")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    response = cleanup_fn(
        _internal_ctx(ctx),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
    )
    return ensure_mapping(response, label="delta_cleanup_metadata")


def _feature_reports(
    *,
    properties_report: Mapping[str, object] | None,
    features_report: Mapping[str, object] | None,
) -> Mapping[str, object]:
    payload: dict[str, object] = {}
    if properties_report is not None:
        payload["properties"] = properties_report
    if features_report is not None:
        payload["features"] = features_report
    return payload


def delta_enable_column_mapping(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    mode: str = "name",
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta column mapping with the desired mode.

    Returns
    -------
    Mapping[str, object]
        Properties/features report payload.
    """
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties={
                "delta.columnMapping.mode": mode,
                "delta.minReaderVersion": "2",
                "delta.minWriterVersion": "5",
            },
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    features_report = delta_add_features(
        ctx,
        request=DeltaAddFeaturesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            features=["columnMapping"],
            allow_protocol_versions_increase=allow_protocol_versions_increase,
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=features_report,
    )


def delta_enable_deletion_vectors(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta deletion vectors (table feature + property).

    Returns
    -------
    Mapping[str, object]
        Properties/features report payload.
    """
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties={"delta.enableDeletionVectors": "true"},
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    features_report = delta_add_features(
        ctx,
        request=DeltaAddFeaturesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            features=["deletionVectors"],
            allow_protocol_versions_increase=allow_protocol_versions_increase,
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=features_report,
    )


def delta_enable_row_tracking(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta row tracking (table feature + property).

    Returns
    -------
    Mapping[str, object]
        Properties/features report payload.
    """
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties={"delta.enableRowTracking": "true"},
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    features_report = delta_add_features(
        ctx,
        request=DeltaAddFeaturesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            features=["rowTracking"],
            allow_protocol_versions_increase=allow_protocol_versions_increase,
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=features_report,
    )


def delta_enable_change_data_feed(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta Change Data Feed (table feature + property).

    Returns
    -------
    Mapping[str, object]
        Properties/features report payload.
    """
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties={"delta.enableChangeDataFeed": "true"},
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    features_report = delta_add_features(
        ctx,
        request=DeltaAddFeaturesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            features=["changeDataFeed"],
            allow_protocol_versions_increase=allow_protocol_versions_increase,
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=features_report,
    )


def delta_enable_generated_columns(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta generated columns feature.

    Returns
    -------
    Mapping[str, object]
        Properties/features report payload.
    """
    features_report = delta_add_features(
        ctx,
        request=DeltaAddFeaturesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            features=["generatedColumns"],
            allow_protocol_versions_increase=allow_protocol_versions_increase,
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=None,
        features_report=features_report,
    )


def delta_enable_invariants(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta invariants feature.

    Returns
    -------
    Mapping[str, object]
        Properties/features report payload.
    """
    features_report = delta_add_features(
        ctx,
        request=DeltaAddFeaturesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            features=["invariants"],
            allow_protocol_versions_increase=allow_protocol_versions_increase,
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=None,
        features_report=features_report,
    )


def delta_enable_check_constraints(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta check constraints feature.

    Returns
    -------
    Mapping[str, object]
        Properties/features report payload.
    """
    features_report = delta_add_features(
        ctx,
        request=DeltaAddFeaturesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            features=["checkConstraints"],
            allow_protocol_versions_increase=allow_protocol_versions_increase,
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=None,
        features_report=features_report,
    )


def delta_enable_in_commit_timestamps(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    enablement_version: int | None = None,
    enablement_timestamp: str | None = None,
) -> Mapping[str, object]:
    """Enable Delta in-commit timestamps via table properties.

    Returns
    -------
    Mapping[str, object]
        Properties report payload.
    """
    properties: dict[str, str] = {"delta.enableInCommitTimestamps": "true"}
    if enablement_version is not None:
        properties["delta.inCommitTimestampEnablementVersion"] = str(enablement_version)
    if enablement_timestamp is not None:
        properties["delta.inCommitTimestampEnablementTimestamp"] = enablement_timestamp
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties=properties,
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=None,
    )


def delta_enable_v2_checkpoints(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
    allow_protocol_versions_increase: bool = True,
) -> Mapping[str, object]:
    """Enable Delta v2 checkpoints via feature + checkpoint policy property.

    Returns
    -------
    Mapping[str, object]
        Properties/features report payload.
    """
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties={"delta.checkpointPolicy": "v2"},
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    features_report = delta_add_features(
        ctx,
        request=DeltaAddFeaturesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            features=["v2Checkpoint"],
            allow_protocol_versions_increase=allow_protocol_versions_increase,
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=features_report,
    )


def delta_enable_vacuum_protocol_check(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Enable vacuum protocol checks via table properties (best-effort).

    Returns
    -------
    Mapping[str, object]
        Properties report payload.
    """
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties={"delta.vacuumProtocolCheck": "true"},
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=None,
    )


def delta_enable_checkpoint_protection(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Enable checkpoint protection via table properties (best-effort).

    Returns
    -------
    Mapping[str, object]
        Properties report payload.
    """
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties={"delta.checkpointProtection": "true"},
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=None,
    )


def delta_disable_change_data_feed(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta Change Data Feed via table properties (best-effort).

    Returns
    -------
    Mapping[str, object]
        Properties report payload.
    """
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties={"delta.enableChangeDataFeed": "false"},
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=None,
    )


def delta_disable_deletion_vectors(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta deletion vectors via table properties (best-effort).

    Returns
    -------
    Mapping[str, object]
        Properties report payload.
    """
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties={"delta.enableDeletionVectors": "false"},
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=None,
    )


def delta_disable_row_tracking(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta row tracking via table properties (best-effort).

    Returns
    -------
    Mapping[str, object]
        Properties report payload.
    """
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties={"delta.enableRowTracking": "false"},
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=None,
    )


def delta_disable_in_commit_timestamps(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable Delta in-commit timestamps via table properties (best-effort).

    Returns
    -------
    Mapping[str, object]
        Properties report payload.
    """
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties={"delta.enableInCommitTimestamps": "false"},
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=None,
    )


def delta_disable_vacuum_protocol_check(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable vacuum protocol check flag via table properties (best-effort).

    Returns
    -------
    Mapping[str, object]
        Properties report payload.
    """
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties={"delta.vacuumProtocolCheck": "false"},
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=None,
    )


def delta_disable_checkpoint_protection(
    ctx: SessionContext,
    *,
    request: DeltaFeatureEnableRequest,
) -> Mapping[str, object]:
    """Disable checkpoint protection via table properties (best-effort).

    Returns
    -------
    Mapping[str, object]
        Properties report payload.
    """
    properties_report = delta_set_properties(
        ctx,
        request=DeltaSetPropertiesRequest(
            table_uri=request.table_uri,
            storage_options=request.storage_options,
            version=request.version,
            timestamp=request.timestamp,
            properties={"delta.checkpointProtection": "false"},
            gate=request.gate,
            commit_options=request.commit_options,
        ),
    )
    return _feature_reports(
        properties_report=properties_report,
        features_report=None,
    )


def delta_disable_column_mapping(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta column mapping cannot be disabled safely."""
    msg = "Delta column mapping cannot be disabled safely once enabled."
    _raise_engine_error(msg, kind=ErrorKind.DELTA)


def delta_disable_generated_columns(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta generated columns cannot be disabled safely."""
    msg = "Delta generated columns cannot be disabled safely once enabled."
    _raise_engine_error(msg, kind=ErrorKind.DELTA)


def delta_disable_invariants(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta invariants cannot be disabled safely."""
    msg = "Delta invariants cannot be disabled safely once enabled."
    _raise_engine_error(msg, kind=ErrorKind.DELTA)


def delta_disable_check_constraints(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta check constraints must be dropped individually."""
    msg = "Delta check constraints must be dropped individually."
    _raise_engine_error(msg, kind=ErrorKind.DELTA)


def delta_disable_v2_checkpoints(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta v2 checkpoints cannot be disabled safely."""
    msg = "Delta v2 checkpoints cannot be disabled safely once enabled."
    _raise_engine_error(msg, kind=ErrorKind.DELTA)


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
