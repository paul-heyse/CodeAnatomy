"""Mutation entrypoints for the Delta control plane."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec
from datafusion import SessionContext

from datafusion_engine.delta.capabilities import (
    native_dml_compatibility,
    provider_supports_native_dml,
)
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


def _require_non_empty_delete_predicate(predicate: object, *, operation: str) -> str:
    cleaned = predicate.strip() if isinstance(predicate, str) else ""
    if cleaned:
        return cleaned
    msg = (
        f"{operation} requires a non-empty predicate. "
        "To delete all rows use an explicit full-table delete operation."
    )
    raise ValueError(msg)


def _require_native_dml(
    ctx: SessionContext,
    *,
    operation: str,
) -> None:
    if provider_supports_native_dml(ctx, operation=operation, require_non_fallback=True):
        return
    compatibility = native_dml_compatibility(
        ctx,
        operation=operation,
        require_non_fallback=True,
    )
    detail = f" detail={compatibility.error}" if compatibility.error else ""
    _raise_engine_error(
        (
            f"Provider-native Delta {operation} is unavailable for this session.{detail} "
            "Degraded Python fallback mutation paths have been removed."
        ),
        kind=ErrorKind.PLUGIN,
    )


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
    _require_non_empty_delete_predicate(
        request.predicate,
        operation="delta_delete",
    )
    _require_native_dml(ctx, operation="delete")
    delete_fn = _require_internal_entrypoint("delta_delete_request")
    payload = msgspec.msgpack.encode(request)
    response = delete_fn(_internal_ctx(ctx, entrypoint="delta_delete_request"), payload)
    return ensure_mapping(response, label="delta_delete")


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
    _require_native_dml(ctx, operation="update")
    update_fn = _require_internal_entrypoint("delta_update_request")
    payload = msgspec.msgpack.encode(request)
    response = update_fn(_internal_ctx(ctx, entrypoint="delta_update_request"), payload)
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
    _require_native_dml(ctx, operation="merge")
    merge_fn = _require_internal_entrypoint("delta_merge_request")
    payload = msgspec.msgpack.encode(request)
    response = merge_fn(_internal_ctx(ctx, entrypoint="delta_merge_request"), payload)
    return ensure_mapping(response, label="delta_merge")


__all__ = [
    "delta_delete",
    "delta_merge",
    "delta_update",
    "delta_write_ipc",
]
