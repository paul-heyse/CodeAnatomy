"""Rust-backed Delta control-plane adapters.

This module centralizes access to the Rust Delta control plane exposed via
``datafusion._internal``. It provides a typed, canonical surface for:
1. Snapshot and protocol-aware metadata.
2. Provider construction with session-derived scan configuration.
3. File-pruned provider construction for scan planning.
"""

from __future__ import annotations

import base64
import importlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.arrow_schema.abi import schema_to_dict
from datafusion_engine.delta_protocol import (
    DeltaFeatureGate,
    delta_feature_gate_rust_payload,
)
from schema_spec.system import DeltaScanOptions
from utils.validation import ensure_mapping

if TYPE_CHECKING:
    from storage.deltalake.delta import DeltaCdfOptions


@dataclass(frozen=True)
class DeltaAppTransaction:
    """Idempotent application transaction metadata for Delta commits."""

    app_id: str
    version: int
    last_updated: int | None = None


@dataclass(frozen=True)
class DeltaCommitOptions:
    """Commit options for Rust-native Delta mutations and maintenance."""

    metadata: Mapping[str, str] = field(default_factory=dict)
    app_transaction: DeltaAppTransaction | None = None
    max_retries: int | None = None
    create_checkpoint: bool | None = None


@dataclass(frozen=True)
class DeltaProviderBundle:
    """Provider response with canonical control-plane metadata."""

    provider: object
    snapshot: Mapping[str, object]
    scan_config: Mapping[str, object]
    scan_effective: dict[str, object]
    add_actions: object | None = None
    predicate_error: str | None = None


@dataclass(frozen=True)
class DeltaCdfProviderBundle:
    """CDF provider response with canonical control-plane metadata."""

    provider: object
    snapshot: Mapping[str, object]
    cdf_options: Mapping[str, object]


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


def _require_datafusion_internal() -> object:
    """Import and return the ``datafusion._internal`` module.

    Returns
    -------
    object
        Imported ``datafusion._internal`` module.

    Raises
    ------
    RuntimeError
        Raised when ``datafusion._internal`` is unavailable.
    """
    try:
        return importlib.import_module("datafusion._internal")
    except ImportError as exc:  # pragma: no cover - environment contract
        msg = "Delta control-plane operations require datafusion._internal."
        raise RuntimeError(msg) from exc


def _schema_ipc_payload(schema: object | None) -> bytes | None:
    """Serialize an Arrow schema to IPC bytes when supported.

    Returns
    -------
    bytes | None
        IPC payload when schema serialization succeeds, otherwise None.

    Raises
    ------
    TypeError
        Raised when the schema cannot be serialized to IPC bytes.
    """
    if schema is None:
        return None
    serialize = getattr(schema, "serialize", None)
    if not callable(serialize):
        msg = "Delta scan schema does not support serialize()."
        raise TypeError(msg)
    buffer = serialize()
    to_bytes = getattr(buffer, "to_pybytes", None)
    if not callable(to_bytes):
        msg = "Delta scan schema serialize() did not return a compatible buffer."
        raise TypeError(msg)
    payload = to_bytes()
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, bytearray):
        return bytes(payload)
    if isinstance(payload, memoryview):
        return payload.tobytes()
    msg = "Delta scan schema serialize() returned unsupported buffer type."
    raise TypeError(msg)


def _decode_schema_ipc(payload: bytes) -> pa.Schema:
    """Decode Arrow schema IPC bytes into a ``pyarrow.Schema``.

    Returns
    -------
    pyarrow.Schema
        Decoded Arrow schema.

    Raises
    ------
    ValueError
        Raised when the payload cannot be decoded.
    """
    try:
        return pa.ipc.read_schema(pa.BufferReader(payload))
    except (pa.ArrowInvalid, TypeError, ValueError) as exc:
        msg = "Invalid Delta scan schema IPC payload."
        raise ValueError(msg) from exc


def _commit_payload(
    options: DeltaCommitOptions | None,
) -> tuple[
    list[tuple[str, str]] | None,
    str | None,
    int | None,
    int | None,
    int | None,
    bool | None,
]:
    """Convert commit options into Rust extension payload values.

    Returns
    -------
    tuple[list[tuple[str, str]] | None, str | None, int | None, int | None, int | None, bool | None]
        Commit metadata and idempotent transaction fields for Rust calls.
    """
    if options is None:
        return None, None, None, None, None, None
    metadata_items = sorted((str(key), str(value)) for key, value in options.metadata.items())
    metadata_payload = metadata_items or None
    app_id: str | None = None
    app_version: int | None = None
    app_last_updated: int | None = None
    if options.app_transaction is not None:
        app_id = options.app_transaction.app_id
        app_version = options.app_transaction.version
        app_last_updated = options.app_transaction.last_updated
    return (
        metadata_payload,
        app_id,
        app_version,
        app_last_updated,
        options.max_retries,
        options.create_checkpoint,
    )


def _cdf_options_payload(options: DeltaCdfOptions | None) -> dict[str, object]:
    """Return a normalized payload for CDF options.

    Returns
    -------
    dict[str, object]
        Payload describing the requested CDF window.
    """
    if options is None:
        return {
            "starting_version": None,
            "ending_version": None,
            "starting_timestamp": None,
            "ending_timestamp": None,
            "allow_out_of_range": False,
        }
    return {
        "starting_version": options.starting_version,
        "ending_version": options.ending_version,
        "starting_timestamp": options.starting_timestamp,
        "ending_timestamp": options.ending_timestamp,
        "allow_out_of_range": options.allow_out_of_range,
    }


def _cdf_options_to_ext(module: object, options: DeltaCdfOptions | None) -> object:
    """Convert Python CDF options into the Rust extension options type.

    Returns
    -------
    object
        Rust extension options value.

    Raises
    ------
    TypeError
        Raised when the Rust extension options type is unavailable.
    """
    options_type = getattr(module, "DeltaCdfOptions", None)
    if options_type is None:
        msg = "datafusion._internal.DeltaCdfOptions is unavailable."
        raise TypeError(msg)
    ext_options = options_type()
    payload = _cdf_options_payload(options)
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

    Raises
    ------
    TypeError
        Raised when the control-plane response is malformed.
    """
    module = _require_datafusion_internal()
    provider_factory = getattr(module, "delta_table_provider_from_session", None)
    if not callable(provider_factory):
        msg = "datafusion._internal.delta_table_provider_from_session is unavailable."
        raise TypeError(msg)
    schema_ipc = _schema_ipc_payload(request.delta_scan.schema) if request.delta_scan else None
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    response = provider_factory(
        ctx,
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
        raise TypeError(msg)
    return DeltaProviderBundle(
        provider=provider,
        snapshot=snapshot,
        scan_config=scan_config,
        scan_effective=_scan_effective_payload(scan_config),
        add_actions=payload.get("add_actions"),
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

    Raises
    ------
    TypeError
        Raised when the control-plane response is malformed.
    """
    module = _require_datafusion_internal()
    provider_factory = getattr(module, "delta_table_provider_with_files", None)
    if not callable(provider_factory):
        msg = "datafusion._internal.delta_table_provider_with_files is unavailable."
        raise TypeError(msg)
    schema_ipc = _schema_ipc_payload(request.delta_scan.schema) if request.delta_scan else None
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    response = provider_factory(
        ctx,
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
        raise TypeError(msg)
    return DeltaProviderBundle(
        provider=provider,
        snapshot=snapshot,
        scan_config=scan_config,
        scan_effective=_scan_effective_payload(scan_config),
        add_actions=payload.get("add_actions"),
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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane entrypoint is unavailable.
    """
    module = _require_datafusion_internal()
    provider_factory = getattr(module, "delta_cdf_table_provider", None)
    if not callable(provider_factory):
        msg = "datafusion._internal.delta_cdf_table_provider is unavailable."
        raise TypeError(msg)
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
        raise TypeError(msg)
    return DeltaCdfProviderBundle(
        provider=provider,
        snapshot=snapshot,
        cdf_options=_cdf_options_payload(request.options),
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

    Raises
    ------
    TypeError
        Raised when the control-plane response is malformed.
    """
    module = _require_datafusion_internal()
    snapshot_factory = getattr(module, "delta_snapshot_info", None)
    if not callable(snapshot_factory):
        msg = "datafusion._internal.delta_snapshot_info is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
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

    Raises
    ------
    TypeError
        Raised when the control-plane response is malformed.
    """
    module = _require_datafusion_internal()
    add_actions_factory = getattr(module, "delta_add_actions", None)
    if not callable(add_actions_factory):
        msg = "datafusion._internal.delta_add_actions is unavailable."
        raise TypeError(msg)
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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane entrypoint is unavailable.
    """
    module = _require_datafusion_internal()
    write_fn = getattr(module, "delta_write_ipc", None)
    if not callable(write_fn):
        msg = "datafusion._internal.delta_write_ipc is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload = _commit_payload(request.commit_options)
    constraints_payload = list(request.extra_constraints) if request.extra_constraints else None
    partitions_payload = list(request.partition_columns) if request.partition_columns else None
    response = write_fn(
        ctx,
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
        commit_payload[0],
        commit_payload[1],
        commit_payload[2],
        commit_payload[3],
        commit_payload[4],
        commit_payload[5],
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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane entrypoint is unavailable.
    """
    module = _require_datafusion_internal()
    delete_fn = getattr(module, "delta_delete", None)
    if not callable(delete_fn):
        msg = "datafusion._internal.delta_delete is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload = _commit_payload(request.commit_options)
    constraints_payload = list(request.extra_constraints) if request.extra_constraints else None
    response = delete_fn(
        ctx,
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
        commit_payload[0],
        commit_payload[1],
        commit_payload[2],
        commit_payload[3],
        commit_payload[4],
        commit_payload[5],
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
            raise ValueError(msg) from exc
        df = df.filter(predicate_expr)
    schema = df.schema()
    exprs = []
    for name in schema.names:
        if name in request.updates:
            try:
                expr = df.parse_sql_expr(request.updates[name]).alias(name)
            except (RuntimeError, TypeError, ValueError) as exc:
                msg = "Delta update expression parse failed."
                raise ValueError(msg) from exc
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
        raise ValueError(msg)


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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane entrypoint is unavailable.
    ValueError
        Raised when the update assignments are empty.
    """
    if not request.updates:
        msg = "Delta update requires at least one column assignment."
        raise ValueError(msg)
    _validate_update_constraints(ctx, request)
    module = _require_datafusion_internal()
    update_fn = getattr(module, "delta_update", None)
    if not callable(update_fn):
        msg = "datafusion._internal.delta_update is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload = _commit_payload(request.commit_options)
    constraints_payload = list(request.extra_constraints) if request.extra_constraints else None
    updates_payload = sorted((str(key), str(value)) for key, value in request.updates.items())
    response = update_fn(
        ctx,
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
        commit_payload[0],
        commit_payload[1],
        commit_payload[2],
        commit_payload[3],
        commit_payload[4],
        commit_payload[5],
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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane entrypoint is unavailable.
    """
    module = _require_datafusion_internal()
    merge_fn = getattr(module, "delta_merge", None)
    if not callable(merge_fn):
        msg = "datafusion._internal.delta_merge is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload = _commit_payload(request.commit_options)
    constraints_payload = list(request.extra_constraints) if request.extra_constraints else None
    matched_payload = sorted(
        (str(key), str(value)) for key, value in request.matched_updates.items()
    )
    inserts_payload = sorted(
        (str(key), str(value)) for key, value in request.not_matched_inserts.items()
    )
    response = merge_fn(
        ctx,
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
        commit_payload[0],
        commit_payload[1],
        commit_payload[2],
        commit_payload[3],
        commit_payload[4],
        commit_payload[5],
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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane entrypoint is unavailable.
    """
    module = _require_datafusion_internal()
    optimize_fn = getattr(module, "delta_optimize_compact", None)
    if not callable(optimize_fn):
        msg = "datafusion._internal.delta_optimize_compact is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload = _commit_payload(request.commit_options)
    z_order_payload = list(request.z_order_cols) if request.z_order_cols else None
    response = optimize_fn(
        ctx,
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
        commit_payload[0],
        commit_payload[1],
        commit_payload[2],
        commit_payload[3],
        commit_payload[4],
        commit_payload[5],
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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane entrypoint is unavailable.
    """
    module = _require_datafusion_internal()
    vacuum_fn = getattr(module, "delta_vacuum", None)
    if not callable(vacuum_fn):
        msg = "datafusion._internal.delta_vacuum is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload = _commit_payload(request.commit_options)
    response = vacuum_fn(
        ctx,
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
        commit_payload[0],
        commit_payload[1],
        commit_payload[2],
        commit_payload[3],
        commit_payload[4],
        commit_payload[5],
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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane entrypoint is unavailable.
    """
    module = _require_datafusion_internal()
    restore_fn = getattr(module, "delta_restore", None)
    if not callable(restore_fn):
        msg = "datafusion._internal.delta_restore is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload = _commit_payload(request.commit_options)
    response = restore_fn(
        ctx,
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
        commit_payload[0],
        commit_payload[1],
        commit_payload[2],
        commit_payload[3],
        commit_payload[4],
        commit_payload[5],
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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane entrypoint is unavailable.
    ValueError
        Raised when the property update payload is empty.
    """
    if not request.properties:
        msg = "Delta property update requires at least one key/value pair."
        raise ValueError(msg)
    module = _require_datafusion_internal()
    set_fn = getattr(module, "delta_set_properties", None)
    if not callable(set_fn):
        msg = "datafusion._internal.delta_set_properties is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload = _commit_payload(request.commit_options)
    properties_payload = sorted((str(key), str(value)) for key, value in request.properties.items())
    response = set_fn(
        ctx,
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        properties_payload,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload[0],
        commit_payload[1],
        commit_payload[2],
        commit_payload[3],
        commit_payload[4],
        commit_payload[5],
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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane entrypoint is unavailable.
    ValueError
        Raised when the feature list is empty.
    """
    if not request.features:
        msg = "Delta add-features requires at least one feature name."
        raise ValueError(msg)
    module = _require_datafusion_internal()
    add_fn = getattr(module, "delta_add_features", None)
    if not callable(add_fn):
        msg = "datafusion._internal.delta_add_features is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload = _commit_payload(request.commit_options)
    features_payload = [str(feature) for feature in request.features]
    response = add_fn(
        ctx,
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
        commit_payload[0],
        commit_payload[1],
        commit_payload[2],
        commit_payload[3],
        commit_payload[4],
        commit_payload[5],
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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane function is unavailable.
    ValueError
        Raised when the request is missing required constraints.
    """
    if not request.constraints:
        msg = "Delta add-constraints requires at least one constraint."
        raise ValueError(msg)
    module = _require_datafusion_internal()
    add_fn = getattr(module, "delta_add_constraints", None)
    if not callable(add_fn):
        msg = "datafusion._internal.delta_add_constraints is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload = _commit_payload(request.commit_options)
    constraints_payload = sorted(
        (str(name), str(expr)) for name, expr in request.constraints.items()
    )
    response = add_fn(
        ctx,
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        constraints_payload,
        gate_payload[0],
        gate_payload[1],
        gate_payload[2],
        gate_payload[3],
        commit_payload[0],
        commit_payload[1],
        commit_payload[2],
        commit_payload[3],
        commit_payload[4],
        commit_payload[5],
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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane function is unavailable.
    ValueError
        Raised when the request is missing constraint names.
    """
    if not request.constraints:
        msg = "Delta drop-constraints requires at least one constraint name."
        raise ValueError(msg)
    module = _require_datafusion_internal()
    drop_fn = getattr(module, "delta_drop_constraints", None)
    if not callable(drop_fn):
        msg = "datafusion._internal.delta_drop_constraints is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    commit_payload = _commit_payload(request.commit_options)
    response = drop_fn(
        ctx,
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
        commit_payload[0],
        commit_payload[1],
        commit_payload[2],
        commit_payload[3],
        commit_payload[4],
        commit_payload[5],
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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane function is unavailable.
    """
    module = _require_datafusion_internal()
    checkpoint_fn = getattr(module, "delta_create_checkpoint", None)
    if not callable(checkpoint_fn):
        msg = "datafusion._internal.delta_create_checkpoint is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    response = checkpoint_fn(
        ctx,
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

    Raises
    ------
    TypeError
        Raised when the Rust control-plane function is unavailable.
    """
    module = _require_datafusion_internal()
    cleanup_fn = getattr(module, "delta_cleanup_metadata", None)
    if not callable(cleanup_fn):
        msg = "datafusion._internal.delta_cleanup_metadata is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = delta_feature_gate_rust_payload(request.gate)
    response = cleanup_fn(
        ctx,
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
    """Raise when Delta column mapping cannot be disabled safely.

    Raises
    ------
    RuntimeError
        Raised because column mapping cannot be disabled safely.
    """
    msg = "Delta column mapping cannot be disabled safely once enabled."
    raise RuntimeError(msg)


def delta_disable_generated_columns(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta generated columns cannot be disabled safely.

    Raises
    ------
    RuntimeError
        Raised because generated columns cannot be disabled safely.
    """
    msg = "Delta generated columns cannot be disabled safely once enabled."
    raise RuntimeError(msg)


def delta_disable_invariants(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta invariants cannot be disabled safely.

    Raises
    ------
    RuntimeError
        Raised because invariants cannot be disabled safely.
    """
    msg = "Delta invariants cannot be disabled safely once enabled."
    raise RuntimeError(msg)


def delta_disable_check_constraints(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta check constraints must be dropped individually.

    Raises
    ------
    RuntimeError
        Raised because check constraints must be dropped individually.
    """
    msg = "Delta check constraints must be dropped individually."
    raise RuntimeError(msg)


def delta_disable_v2_checkpoints(*_args: object, **_kwargs: object) -> None:
    """Raise when Delta v2 checkpoints cannot be disabled safely.

    Raises
    ------
    RuntimeError
        Raised because v2 checkpoints cannot be disabled safely.
    """
    msg = "Delta v2 checkpoints cannot be disabled safely once enabled."
    raise RuntimeError(msg)


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
