"""Delta table-management facade helpers."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.delta.control_plane_core import (
    DeltaProviderBundle,
    DeltaProviderRequest,
    DeltaSnapshotRequest,
    delta_provider_from_session,
    delta_snapshot_info,
)


def provider_bundle(
    ctx: SessionContext,
    *,
    request: DeltaProviderRequest,
) -> DeltaProviderBundle:
    """Return Delta provider bundle for a session context."""
    return delta_provider_from_session(ctx, request=request)


def snapshot_info(request: DeltaSnapshotRequest) -> dict[str, object]:
    """Return Delta snapshot metadata."""
    return dict(delta_snapshot_info(request))


__all__ = ["provider_bundle", "snapshot_info"]
