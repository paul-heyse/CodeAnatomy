"""Rust-backed Delta control-plane adapters.

This module centralizes access to the Rust Delta control plane exposed via
``datafusion_ext``. It provides a typed, canonical surface for:
1. Snapshot and protocol-aware metadata.
2. Provider construction with session-derived scan configuration.
3. File-pruned provider construction for scan planning.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext

from arrowdsl.schema.abi import schema_to_dict
from schema_spec.system import DeltaScanOptions

if TYPE_CHECKING:
    from storage.deltalake.delta import DeltaCdfOptions


@dataclass(frozen=True)
class DeltaFeatureGate:
    """Protocol and feature-gating requirements for Delta tables.

    Parameters
    ----------
    min_reader_version:
        Minimum reader protocol version required.
    min_writer_version:
        Minimum writer protocol version required.
    required_reader_features:
        Required Delta reader features.
    required_writer_features:
        Required Delta writer features.
    """

    min_reader_version: int | None = None
    min_writer_version: int | None = None
    required_reader_features: tuple[str, ...] = ()
    required_writer_features: tuple[str, ...] = ()


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


def _require_datafusion_ext() -> object:
    """Import and return the ``datafusion_ext`` module.

    Returns
    -------
    object
        Imported ``datafusion_ext`` module.

    Raises
    ------
    RuntimeError
        Raised when ``datafusion_ext`` is unavailable.
    """
    try:
        return importlib.import_module("datafusion_ext")
    except ImportError as exc:  # pragma: no cover - environment contract
        msg = "Delta control-plane operations require datafusion_ext."
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


def _gate_payload(
    gate: DeltaFeatureGate | None,
) -> tuple[int | None, int | None, list[str] | None, list[str] | None]:
    """Convert a feature gate into Rust-compatible payload values.

    Returns
    -------
    tuple[int | None, int | None, list[str] | None, list[str] | None]
        Reader/writer versions and feature lists for the Rust interface.
    """
    if gate is None:
        return None, None, None, None
    reader_features = list(gate.required_reader_features) or None
    writer_features = list(gate.required_writer_features) or None
    return gate.min_reader_version, gate.min_writer_version, reader_features, writer_features


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
    """Convert commit options into Rust extension payload values."""
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
    """Return a normalized payload for CDF options."""
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
    """Convert Python CDF options into the Rust extension options type."""
    options_type = getattr(module, "DeltaCdfOptions", None)
    if options_type is None:
        msg = "datafusion_ext.DeltaCdfOptions is unavailable."
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
    schema_ipc = payload.get("schema_ipc")
    if isinstance(schema_ipc, (bytes, bytearray)):
        schema_payload = schema_to_dict(_decode_schema_ipc(bytes(schema_ipc)))
    return {
        "file_column_name": payload.get("file_column_name"),
        "enable_parquet_pushdown": payload.get("enable_parquet_pushdown"),
        "schema_force_view_types": payload.get("schema_force_view_types"),
        "wrap_partition_values": payload.get("wrap_partition_values"),
        "schema": schema_payload,
        "has_schema": payload.get("has_schema"),
    }


def _ensure_mapping(value: object, *, label: str) -> Mapping[str, object]:
    """Validate that a payload value is a mapping.

    Returns
    -------
    Mapping[str, object]
        Validated mapping payload.

    Raises
    ------
    TypeError
        Raised when the payload value is not a mapping.
    """
    if isinstance(value, Mapping):
        return value
    msg = f"Delta control-plane payload for {label} was not a mapping."
    raise TypeError(msg)


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
    module = _require_datafusion_ext()
    provider_factory = getattr(module, "delta_table_provider_from_session", None)
    if not callable(provider_factory):
        msg = "datafusion_ext.delta_table_provider_from_session is unavailable."
        raise TypeError(msg)
    schema_ipc = _schema_ipc_payload(request.delta_scan.schema) if request.delta_scan else None
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = _gate_payload(request.gate)
    response = provider_factory(
        ctx,
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
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
    payload = _ensure_mapping(response, label="delta_table_provider_from_session")
    snapshot = _ensure_mapping(payload.get("snapshot"), label="snapshot")
    scan_config = _ensure_mapping(payload.get("scan_config"), label="scan_config")
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
    module = _require_datafusion_ext()
    provider_factory = getattr(module, "delta_table_provider_with_files", None)
    if not callable(provider_factory):
        msg = "datafusion_ext.delta_table_provider_with_files is unavailable."
        raise TypeError(msg)
    schema_ipc = _schema_ipc_payload(request.delta_scan.schema) if request.delta_scan else None
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = _gate_payload(request.gate)
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
    payload = _ensure_mapping(response, label="delta_table_provider_with_files")
    snapshot = _ensure_mapping(payload.get("snapshot"), label="snapshot")
    scan_config = _ensure_mapping(payload.get("scan_config"), label="scan_config")
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
    """Build a Delta CDF provider through the Rust control plane."""
    module = _require_datafusion_ext()
    provider_factory = getattr(module, "delta_cdf_table_provider", None)
    if not callable(provider_factory):
        msg = "datafusion_ext.delta_cdf_table_provider is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = _gate_payload(request.gate)
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
    payload = _ensure_mapping(response, label="delta_cdf_table_provider")
    snapshot = _ensure_mapping(payload.get("snapshot"), label="snapshot")
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
    module = _require_datafusion_ext()
    snapshot_factory = getattr(module, "delta_snapshot_info", None)
    if not callable(snapshot_factory):
        msg = "datafusion_ext.delta_snapshot_info is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = _gate_payload(request.gate)
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
    return _ensure_mapping(response, label="delta_snapshot_info")


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
    module = _require_datafusion_ext()
    add_actions_factory = getattr(module, "delta_add_actions", None)
    if not callable(add_actions_factory):
        msg = "datafusion_ext.delta_add_actions is unavailable."
        raise TypeError(msg)
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    gate_payload = _gate_payload(request.gate)
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
    return _ensure_mapping(response, label="delta_add_actions")


def delta_write_ipc(
    ctx: SessionContext,
    *,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    version: int | None,
    timestamp: str | None,
    data_ipc: bytes,
    mode: str,
    schema_mode: str | None,
    partition_columns: Sequence[str] | None,
    target_file_size: int | None,
    extra_constraints: Sequence[str] | None,
    gate: DeltaFeatureGate | None = None,
    commit_options: DeltaCommitOptions | None = None,
) -> Mapping[str, object]:
    """Run a Rust-native Delta write over Arrow IPC bytes."""
    module = _require_datafusion_ext()
    write_fn = getattr(module, "delta_write_ipc", None)
    if not callable(write_fn):
        msg = "datafusion_ext.delta_write_ipc is unavailable."
        raise TypeError(msg)
    storage_payload = list(storage_options.items()) if storage_options else None
    gate_payload = _gate_payload(gate)
    commit_payload = _commit_payload(commit_options)
    constraints_payload = list(extra_constraints) if extra_constraints else None
    partitions_payload = list(partition_columns) if partition_columns else None
    response = write_fn(
        ctx,
        table_uri,
        storage_payload,
        version,
        timestamp,
        data_ipc,
        mode,
        schema_mode,
        partitions_payload,
        target_file_size,
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
    return _ensure_mapping(response, label="delta_write_ipc")


def delta_delete(
    ctx: SessionContext,
    *,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    version: int | None,
    timestamp: str | None,
    predicate: str | None,
    extra_constraints: Sequence[str] | None,
    gate: DeltaFeatureGate | None = None,
    commit_options: DeltaCommitOptions | None = None,
) -> Mapping[str, object]:
    """Run a Rust-native Delta delete."""
    module = _require_datafusion_ext()
    delete_fn = getattr(module, "delta_delete", None)
    if not callable(delete_fn):
        msg = "datafusion_ext.delta_delete is unavailable."
        raise TypeError(msg)
    storage_payload = list(storage_options.items()) if storage_options else None
    gate_payload = _gate_payload(gate)
    commit_payload = _commit_payload(commit_options)
    constraints_payload = list(extra_constraints) if extra_constraints else None
    response = delete_fn(
        ctx,
        table_uri,
        storage_payload,
        version,
        timestamp,
        predicate,
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
    return _ensure_mapping(response, label="delta_delete")


def delta_update(
    ctx: SessionContext,
    *,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    version: int | None,
    timestamp: str | None,
    predicate: str | None,
    updates: Mapping[str, str],
    extra_constraints: Sequence[str] | None,
    gate: DeltaFeatureGate | None = None,
    commit_options: DeltaCommitOptions | None = None,
) -> Mapping[str, object]:
    """Run a Rust-native Delta update."""
    if not updates:
        msg = "Delta update requires at least one column assignment."
        raise ValueError(msg)
    module = _require_datafusion_ext()
    update_fn = getattr(module, "delta_update", None)
    if not callable(update_fn):
        msg = "datafusion_ext.delta_update is unavailable."
        raise TypeError(msg)
    storage_payload = list(storage_options.items()) if storage_options else None
    gate_payload = _gate_payload(gate)
    commit_payload = _commit_payload(commit_options)
    constraints_payload = list(extra_constraints) if extra_constraints else None
    updates_payload = sorted((str(key), str(value)) for key, value in updates.items())
    response = update_fn(
        ctx,
        table_uri,
        storage_payload,
        version,
        timestamp,
        predicate,
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
    return _ensure_mapping(response, label="delta_update")


def delta_merge(
    ctx: SessionContext,
    *,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    version: int | None,
    timestamp: str | None,
    source_table: str,
    predicate: str,
    source_alias: str | None,
    target_alias: str | None,
    matched_predicate: str | None,
    matched_updates: Mapping[str, str],
    not_matched_predicate: str | None,
    not_matched_inserts: Mapping[str, str],
    not_matched_by_source_predicate: str | None,
    delete_not_matched_by_source: bool,
    extra_constraints: Sequence[str] | None,
    gate: DeltaFeatureGate | None = None,
    commit_options: DeltaCommitOptions | None = None,
) -> Mapping[str, object]:
    """Run a Rust-native Delta merge."""
    module = _require_datafusion_ext()
    merge_fn = getattr(module, "delta_merge", None)
    if not callable(merge_fn):
        msg = "datafusion_ext.delta_merge is unavailable."
        raise TypeError(msg)
    storage_payload = list(storage_options.items()) if storage_options else None
    gate_payload = _gate_payload(gate)
    commit_payload = _commit_payload(commit_options)
    constraints_payload = list(extra_constraints) if extra_constraints else None
    matched_payload = sorted((str(key), str(value)) for key, value in matched_updates.items())
    inserts_payload = sorted((str(key), str(value)) for key, value in not_matched_inserts.items())
    response = merge_fn(
        ctx,
        table_uri,
        storage_payload,
        version,
        timestamp,
        source_table,
        predicate,
        source_alias,
        target_alias,
        matched_predicate,
        matched_payload,
        not_matched_predicate,
        inserts_payload,
        not_matched_by_source_predicate,
        delete_not_matched_by_source,
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
    return _ensure_mapping(response, label="delta_merge")


def delta_optimize_compact(
    ctx: SessionContext,
    *,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    version: int | None,
    timestamp: str | None,
    target_size: int | None,
    gate: DeltaFeatureGate | None = None,
    commit_options: DeltaCommitOptions | None = None,
) -> Mapping[str, object]:
    """Run Rust-native Delta optimize/compact."""
    module = _require_datafusion_ext()
    optimize_fn = getattr(module, "delta_optimize_compact", None)
    if not callable(optimize_fn):
        msg = "datafusion_ext.delta_optimize_compact is unavailable."
        raise TypeError(msg)
    storage_payload = list(storage_options.items()) if storage_options else None
    gate_payload = _gate_payload(gate)
    commit_payload = _commit_payload(commit_options)
    response = optimize_fn(
        ctx,
        table_uri,
        storage_payload,
        version,
        timestamp,
        target_size,
        None,
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
    return _ensure_mapping(response, label="delta_optimize_compact")


def delta_vacuum(
    ctx: SessionContext,
    *,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    version: int | None,
    timestamp: str | None,
    retention_hours: int | None,
    dry_run: bool,
    enforce_retention_duration: bool,
    gate: DeltaFeatureGate | None = None,
    commit_options: DeltaCommitOptions | None = None,
) -> Mapping[str, object]:
    """Run Rust-native Delta vacuum."""
    module = _require_datafusion_ext()
    vacuum_fn = getattr(module, "delta_vacuum", None)
    if not callable(vacuum_fn):
        msg = "datafusion_ext.delta_vacuum is unavailable."
        raise TypeError(msg)
    storage_payload = list(storage_options.items()) if storage_options else None
    gate_payload = _gate_payload(gate)
    commit_payload = _commit_payload(commit_options)
    response = vacuum_fn(
        ctx,
        table_uri,
        storage_payload,
        version,
        timestamp,
        retention_hours,
        dry_run,
        enforce_retention_duration,
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
    return _ensure_mapping(response, label="delta_vacuum")


def delta_restore(
    ctx: SessionContext,
    *,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    version: int | None,
    timestamp: str | None,
    restore_version: int | None,
    restore_timestamp: str | None,
    gate: DeltaFeatureGate | None = None,
    commit_options: DeltaCommitOptions | None = None,
) -> Mapping[str, object]:
    """Run Rust-native Delta restore."""
    module = _require_datafusion_ext()
    restore_fn = getattr(module, "delta_restore", None)
    if not callable(restore_fn):
        msg = "datafusion_ext.delta_restore is unavailable."
        raise TypeError(msg)
    storage_payload = list(storage_options.items()) if storage_options else None
    gate_payload = _gate_payload(gate)
    commit_payload = _commit_payload(commit_options)
    response = restore_fn(
        ctx,
        table_uri,
        storage_payload,
        version,
        timestamp,
        restore_version,
        restore_timestamp,
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
    return _ensure_mapping(response, label="delta_restore")


def delta_set_properties(
    ctx: SessionContext,
    *,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    version: int | None,
    timestamp: str | None,
    properties: Mapping[str, str],
    gate: DeltaFeatureGate | None = None,
    commit_options: DeltaCommitOptions | None = None,
) -> Mapping[str, object]:
    """Run Rust-native Delta property updates."""
    if not properties:
        msg = "Delta property update requires at least one key/value pair."
        raise ValueError(msg)
    module = _require_datafusion_ext()
    set_fn = getattr(module, "delta_set_properties", None)
    if not callable(set_fn):
        msg = "datafusion_ext.delta_set_properties is unavailable."
        raise TypeError(msg)
    storage_payload = list(storage_options.items()) if storage_options else None
    gate_payload = _gate_payload(gate)
    commit_payload = _commit_payload(commit_options)
    properties_payload = sorted((str(key), str(value)) for key, value in properties.items())
    response = set_fn(
        ctx,
        table_uri,
        storage_payload,
        version,
        timestamp,
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
    return _ensure_mapping(response, label="delta_set_properties")


def delta_add_features(
    ctx: SessionContext,
    *,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    version: int | None,
    timestamp: str | None,
    features: Sequence[str],
    allow_protocol_versions_increase: bool,
    gate: DeltaFeatureGate | None = None,
    commit_options: DeltaCommitOptions | None = None,
) -> Mapping[str, object]:
    """Run Rust-native Delta feature enablement."""
    if not features:
        msg = "Delta add-features requires at least one feature name."
        raise ValueError(msg)
    module = _require_datafusion_ext()
    add_fn = getattr(module, "delta_add_features", None)
    if not callable(add_fn):
        msg = "datafusion_ext.delta_add_features is unavailable."
        raise TypeError(msg)
    storage_payload = list(storage_options.items()) if storage_options else None
    gate_payload = _gate_payload(gate)
    commit_payload = _commit_payload(commit_options)
    features_payload = [str(feature) for feature in features]
    response = add_fn(
        ctx,
        table_uri,
        storage_payload,
        version,
        timestamp,
        features_payload,
        allow_protocol_versions_increase,
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
    return _ensure_mapping(response, label="delta_add_features")


__all__ = [
    "DeltaFeatureGate",
    "DeltaAppTransaction",
    "DeltaCdfProviderBundle",
    "DeltaCdfRequest",
    "DeltaCommitOptions",
    "DeltaProviderBundle",
    "DeltaProviderRequest",
    "DeltaSnapshotRequest",
    "delta_add_actions",
    "delta_add_features",
    "delta_cdf_provider",
    "delta_delete",
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
