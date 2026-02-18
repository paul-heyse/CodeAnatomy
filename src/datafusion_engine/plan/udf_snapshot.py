"""UDF snapshot and required-UDF helpers for plan artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime_session import SessionRuntime


@dataclass(frozen=True)
class UdfArtifacts:
    """UDF snapshot and metadata for planning."""

    snapshot: Mapping[str, object]
    snapshot_hash: str
    rewrite_tags: tuple[str, ...]
    domain_planner_names: tuple[str, ...]


@dataclass(frozen=True)
class RequiredUdfArtifacts:
    """Required UDFs and rewrite tags for a plan."""

    required_udfs: tuple[str, ...]
    required_rewrite_tags: tuple[str, ...]


@dataclass(frozen=True)
class UdfSnapshotArtifacts:
    """Normalized UDF snapshot payload and fingerprint."""

    snapshot: Mapping[str, object]
    snapshot_hash: str


def collect_udf_artifacts(
    ctx: SessionContext,
    *,
    registry_snapshot: Mapping[str, object] | None,
    session_runtime: SessionRuntime | None,
) -> UdfArtifacts:
    """Collect canonical UDF artifacts from runtime or the Rust extension.

    Returns:
        UdfArtifacts: Snapshot, hash, rewrite tags, and planner names.
    """
    if registry_snapshot is not None:
        snapshot = registry_snapshot
    elif session_runtime is not None:
        return UdfArtifacts(
            snapshot=session_runtime.udf_snapshot,
            snapshot_hash=session_runtime.udf_snapshot_hash,
            rewrite_tags=session_runtime.udf_rewrite_tags,
            domain_planner_names=session_runtime.domain_planner_names,
        )
    else:
        from datafusion_engine.udf.extension_runtime import rust_udf_snapshot

        snapshot = rust_udf_snapshot(ctx)

    from datafusion_engine.expr.domain_planner import domain_planner_names_from_snapshot
    from datafusion_engine.udf.extension_runtime import (
        rust_udf_snapshot_hash,
        validate_rust_udf_snapshot,
    )
    from datafusion_engine.udf.metadata import rewrite_tag_index

    validate_rust_udf_snapshot(snapshot)
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    tag_index = rewrite_tag_index(snapshot)
    rewrite_tags = tuple(sorted(tag_index))
    planner_names = domain_planner_names_from_snapshot(snapshot)
    return UdfArtifacts(
        snapshot=snapshot,
        snapshot_hash=snapshot_hash,
        rewrite_tags=rewrite_tags,
        domain_planner_names=planner_names,
    )


def required_udf_artifacts(
    plan: object | None,
    *,
    snapshot: Mapping[str, object],
    rust_required_udfs: Sequence[str] | None = None,
) -> RequiredUdfArtifacts:
    """Resolve required UDFs for a plan from Rust payload or lineage analysis.

    Returns:
        RequiredUdfArtifacts: Required UDF names and rewrite tags.
    """
    if rust_required_udfs is not None:
        from datafusion_engine.udf.metadata import rewrite_tag_index

        required_udfs = tuple(sorted({str(name) for name in rust_required_udfs if str(name)}))
        tag_index = rewrite_tag_index(snapshot)
        required_tags = tuple(
            sorted({tag for name in required_udfs for tag in tag_index.get(name, ())})
        )
        return RequiredUdfArtifacts(
            required_udfs=required_udfs,
            required_rewrite_tags=required_tags,
        )
    if plan is None:
        return RequiredUdfArtifacts(required_udfs=(), required_rewrite_tags=())

    from datafusion_engine.lineage.reporting import extract_lineage

    lineage = extract_lineage(plan, udf_snapshot=snapshot)
    return RequiredUdfArtifacts(
        required_udfs=lineage.required_udfs,
        required_rewrite_tags=lineage.required_rewrite_tags,
    )


def collect_udf_snapshot_artifacts(
    ctx: SessionContext,
    *,
    session_runtime: SessionRuntime | None,
) -> UdfSnapshotArtifacts:
    """Collect canonical UDF snapshot payload and hash for diagnostics/tests.

    Returns:
        UdfSnapshotArtifacts: Snapshot payload and stable snapshot hash.
    """
    artifacts = collect_udf_artifacts(
        ctx,
        registry_snapshot=None,
        session_runtime=session_runtime,
    )
    return UdfSnapshotArtifacts(
        snapshot=dict(artifacts.snapshot),
        snapshot_hash=artifacts.snapshot_hash,
    )


__all__ = [
    "RequiredUdfArtifacts",
    "UdfArtifacts",
    "UdfSnapshotArtifacts",
    "collect_udf_artifacts",
    "collect_udf_snapshot_artifacts",
    "required_udf_artifacts",
]
