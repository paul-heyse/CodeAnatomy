"""Maintenance operations for the Delta control plane."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion import SessionContext

from datafusion_engine.delta.commit_payload import commit_payload_parts
from datafusion_engine.delta.control_plane_core import (
    DeltaCheckpointRequest,
    DeltaOptimizeRequest,
    DeltaVacuumRequest,
    _internal_ctx,
    _require_internal_entrypoint,
)
from datafusion_engine.delta.payload import commit_payload
from datafusion_engine.delta.protocol import delta_feature_gate_rust_payload
from utils.validation import ensure_mapping


def delta_optimize_compact(
    ctx: SessionContext,
    *,
    request: DeltaOptimizeRequest,
) -> Mapping[str, object]:
    """Run Delta optimize/compact through control-plane authority.

    Returns:
    -------
    Mapping[str, object]
        Control-plane optimize report payload.
    """
    optimize_fn = _require_internal_entrypoint("delta_optimize_compact")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    commit_parts = commit_payload_parts(commit_payload(request.commit_options))
    z_order_payload = list(request.z_order_cols) if request.z_order_cols else None
    response = optimize_fn(
        _internal_ctx(ctx, entrypoint="delta_optimize_compact"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.target_size,
        z_order_payload,
        *delta_feature_gate_rust_payload(request.gate),
        commit_parts.metadata_payload,
        commit_parts.app_id,
        commit_parts.app_version,
        commit_parts.app_last_updated,
        commit_parts.max_retries,
        commit_parts.create_checkpoint,
    )
    return ensure_mapping(response, label="delta_optimize_compact")


def delta_vacuum(
    ctx: SessionContext,
    *,
    request: DeltaVacuumRequest,
) -> Mapping[str, object]:
    """Run Delta vacuum through control-plane authority.

    Returns:
    -------
    Mapping[str, object]
        Control-plane vacuum report payload.
    """
    vacuum_fn = _require_internal_entrypoint("delta_vacuum")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    commit_parts = commit_payload_parts(commit_payload(request.commit_options))
    response = vacuum_fn(
        _internal_ctx(ctx, entrypoint="delta_vacuum"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.retention_hours,
        request.dry_run,
        request.dry_run,
        request.enforce_retention_duration,
        request.require_vacuum_protocol_check,
        *delta_feature_gate_rust_payload(request.gate),
        commit_parts.metadata_payload,
        commit_parts.app_id,
        commit_parts.app_version,
        commit_parts.app_last_updated,
        commit_parts.max_retries,
        commit_parts.create_checkpoint,
    )
    return ensure_mapping(response, label="delta_vacuum")


def delta_create_checkpoint(
    ctx: SessionContext,
    *,
    request: DeltaCheckpointRequest,
) -> Mapping[str, object]:
    """Create a Delta checkpoint through control-plane authority.

    Returns:
    -------
    Mapping[str, object]
        Control-plane checkpoint report payload.
    """
    checkpoint_fn = _require_internal_entrypoint("delta_create_checkpoint")
    storage_payload = list(request.storage_options.items()) if request.storage_options else None
    response = checkpoint_fn(
        _internal_ctx(ctx, entrypoint="delta_create_checkpoint"),
        request.table_uri,
        storage_payload,
        request.version,
        request.timestamp,
        *delta_feature_gate_rust_payload(request.gate),
    )
    return ensure_mapping(response, label="delta_create_checkpoint")


__all__ = [
    "DeltaCheckpointRequest",
    "DeltaOptimizeRequest",
    "DeltaVacuumRequest",
    "delta_create_checkpoint",
    "delta_optimize_compact",
    "delta_vacuum",
]
