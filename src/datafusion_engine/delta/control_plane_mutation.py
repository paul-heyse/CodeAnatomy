"""Mutation entrypoints for the Delta control plane."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec
from datafusion import SessionContext

from datafusion_engine.delta.control_plane_core import (
    DeltaDeleteRequest,
    DeltaMergeRequest,
    DeltaUpdateRequest,
    DeltaWriteRequest,
    _internal_ctx,
    _raise_engine_error,
    _require_internal_entrypoint,
)
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
    write_fn = _require_internal_entrypoint("delta_write_ipc_request")
    payload = msgspec.msgpack.encode(request)
    response = write_fn(_internal_ctx(ctx, entrypoint="delta_write_ipc_request"), payload)
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
    delete_fn = _require_internal_entrypoint("delta_delete_request_payload")
    payload = msgspec.msgpack.encode(request)
    response = delete_fn(_internal_ctx(ctx, entrypoint="delta_delete_request_payload"), payload)
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
    update_fn = _require_internal_entrypoint("delta_update_request_payload")
    payload = msgspec.msgpack.encode(request)
    response = update_fn(_internal_ctx(ctx, entrypoint="delta_update_request_payload"), payload)
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
    merge_fn = _require_internal_entrypoint("delta_merge_request_payload")
    payload = msgspec.msgpack.encode(request)
    response = merge_fn(_internal_ctx(ctx, entrypoint="delta_merge_request_payload"), payload)
    return ensure_mapping(response, label="delta_merge")


__all__ = [
    "delta_delete",
    "delta_merge",
    "delta_update",
    "delta_write_ipc",
]
