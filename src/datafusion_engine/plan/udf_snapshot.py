"""UDF snapshot helpers extracted from bundle_artifact."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion_engine.plan.bundle_artifact import _udf_artifacts

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import SessionRuntime


@dataclass(frozen=True)
class UdfSnapshotArtifacts:
    """Normalized UDF snapshot payload and fingerprint."""

    snapshot: Mapping[str, object]
    snapshot_hash: str


def collect_udf_snapshot_artifacts(
    ctx: SessionContext,
    *,
    session_runtime: SessionRuntime | None,
) -> UdfSnapshotArtifacts:
    """Collect canonical UDF snapshot artifacts from runtime/session context.

    Returns:
        UdfSnapshotArtifacts: Normalized UDF snapshot payload and fingerprint.
    """
    artifacts = _udf_artifacts(ctx, registry_snapshot=None, session_runtime=session_runtime)
    return UdfSnapshotArtifacts(
        snapshot=dict(artifacts.snapshot), snapshot_hash=artifacts.snapshot_hash
    )


__all__ = ["UdfSnapshotArtifacts", "collect_udf_snapshot_artifacts"]
