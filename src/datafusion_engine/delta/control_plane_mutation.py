"""Mutation entrypoints for the Delta control plane."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion import SessionContext

from datafusion_engine.delta.commit_payload import commit_payload_parts
from datafusion_engine.delta.control_plane_core import (
    DeltaDeleteRequest,
    DeltaMergeRequest,
    DeltaUpdateRequest,
    DeltaWriteRequest,
    _internal_ctx,
    _raise_engine_error,
    _require_internal_entrypoint,
)
from datafusion_engine.delta.payload import commit_payload
from datafusion_engine.delta.protocol import delta_feature_gate_rust_payload
from datafusion_engine.errors import ErrorKind
from utils.validation import ensure_mapping


def delta_write_ipc(
    ctx: SessionContext,
    *,
    request: DeltaWriteRequest,
) -> Mapping[str, object]:
    """Run a Rust-native Delta write over Arrow IPC bytes.

    Returns:
    -------
    Mapping[str, object]
        Engine response payload converted to a mapping.
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

    Returns:
    -------
    Mapping[str, object]
        Engine response payload converted to a mapping.
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
    _ = ctx


def delta_update(
    ctx: SessionContext,
    *,
    request: DeltaUpdateRequest,
) -> Mapping[str, object]:
    """Run a Rust-native Delta update.

    Returns:
    -------
    Mapping[str, object]
        Engine response payload converted to a mapping.
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

    Returns:
    -------
    Mapping[str, object]
        Engine response payload converted to a mapping.
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


__all__ = [
    "delta_delete",
    "delta_merge",
    "delta_update",
    "delta_write_ipc",
]
