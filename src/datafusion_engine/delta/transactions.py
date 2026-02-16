"""Delta transaction helpers."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion import SessionContext

from datafusion_engine.delta.control_plane_core import (
    DeltaDeleteRequest,
    DeltaMergeRequest,
    DeltaUpdateRequest,
    DeltaWriteRequest,
    delta_delete,
    delta_merge,
    delta_update,
    delta_write_ipc,
)


def write_transaction(
    ctx: SessionContext,
    *,
    request: DeltaWriteRequest,
) -> Mapping[str, object]:
    """Execute a Delta write transaction.

    Returns:
    -------
    Mapping[str, object]
        Control-plane write report payload.
    """
    return delta_write_ipc(ctx, request=request)


def delete_transaction(
    ctx: SessionContext,
    *,
    request: DeltaDeleteRequest,
) -> Mapping[str, object]:
    """Execute a Delta delete transaction.

    Returns:
    -------
    Mapping[str, object]
        Control-plane delete report payload.
    """
    return delta_delete(ctx, request=request)


def update_transaction(
    ctx: SessionContext,
    *,
    request: DeltaUpdateRequest,
) -> Mapping[str, object]:
    """Execute a Delta update transaction.

    Returns:
    -------
    Mapping[str, object]
        Control-plane update report payload.
    """
    return delta_update(ctx, request=request)


def merge_transaction(
    ctx: SessionContext,
    *,
    request: DeltaMergeRequest,
) -> Mapping[str, object]:
    """Execute a Delta merge transaction.

    Returns:
    -------
    Mapping[str, object]
        Control-plane merge report payload.
    """
    return delta_merge(ctx, request=request)


__all__ = [
    "delete_transaction",
    "merge_transaction",
    "update_transaction",
    "write_transaction",
]
