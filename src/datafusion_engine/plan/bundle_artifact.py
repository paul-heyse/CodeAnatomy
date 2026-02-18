"""Canonical DataFusion plan bundle facade for planning and scheduling."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.plan.bundle_assembly import _bundle_components
from datafusion_engine.plan.cache import PlanProtoCacheEntry
from datafusion_engine.plan.plan_utils import plan_display
from obs.otel import SCOPE_PLANNING, stage_span
from serde_artifacts import DeltaInputPin, PlanArtifacts
from serde_msgspec_ext import (
    ExecutionPlanProtoBytes,
    LogicalPlanProtoBytes,
    OptimizedPlanProtoBytes,
)

if TYPE_CHECKING:
    from datafusion_engine.lineage.scheduling import ScanUnit
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.session.runtime_session import SessionRuntime
    from semantics.program_manifest import ManifestDatasetResolver


DataFrameBuilder = Callable[[SessionContext], DataFrame]


@dataclass(frozen=True)
class PlanBundleOptions:
    """Options for building a DataFusion plan bundle."""

    compute_execution_plan: bool = True
    compute_substrait: bool = True
    validate_udfs: bool = False
    enable_proto_serialization: bool | None = None
    registry_snapshot: Mapping[str, object] | None = None
    delta_inputs: Sequence[DeltaInputPin] = ()
    session_runtime: SessionRuntime | None = None
    scan_units: Sequence[ScanUnit] = ()
    dataset_resolver: ManifestDatasetResolver | None = None


@dataclass(frozen=True)
class DataFusionPlanArtifact:
    """Canonical plan artifact for all planning and scheduling."""

    df: DataFrame
    logical_plan: object
    optimized_logical_plan: object
    execution_plan: object | None
    substrait_bytes: bytes
    plan_fingerprint: str
    artifacts: PlanArtifacts
    delta_inputs: tuple[DeltaInputPin, ...] = ()
    scan_units: tuple[ScanUnit, ...] = ()
    plan_identity_hash: str | None = None
    required_udfs: tuple[str, ...] = ()
    required_rewrite_tags: tuple[str, ...] = ()
    plan_details: Mapping[str, object] = field(default_factory=dict)

    def display_logical_plan(self) -> str | None:
        """Return a string representation of the logical plan."""
        return plan_display(self.logical_plan, method="display_indent_schema")

    def display_optimized_plan(self) -> str | None:
        """Return a string representation of the optimized logical plan."""
        return plan_display(self.optimized_logical_plan, method="display_indent_schema")

    def display_execution_plan(self) -> str | None:
        """Return a string representation of the physical execution plan."""
        if self.execution_plan is None:
            return None
        return plan_display(self.execution_plan, method="display_indent")

    def graphviz(self) -> str | None:
        """Return GraphViz DOT representation of the optimized plan."""
        method = getattr(self.optimized_logical_plan, "display_graphviz", None)
        if not callable(method):
            return None
        try:
            return str(method())
        except (RuntimeError, TypeError, ValueError):
            return None


def build_plan_artifact(
    ctx: SessionContext,
    df: DataFrame,
    *,
    options: PlanBundleOptions | None = None,
) -> DataFusionPlanArtifact:
    """Build a canonical plan bundle from a DataFusion DataFrame.

    Returns:
    -------
    DataFusionPlanArtifact
        Bundle containing plan artifacts and deterministic metadata.

    Raises:
        ValueError: If strict substrait/runtime requirements are not satisfied.
    """
    with stage_span(
        "planning.plan_bundle",
        stage="planning",
        scope_name=SCOPE_PLANNING,
        attributes={"codeanatomy.plan_kind": "bundle"},
    ):
        resolved = options or PlanBundleOptions()
        if not resolved.compute_substrait:
            msg = "Substrait bytes are required for plan bundle construction."
            raise ValueError(msg)
        if resolved.session_runtime is None:
            msg = "SessionRuntime is required for plan bundle construction."
            raise ValueError(msg)
        components = _bundle_components(
            ctx,
            df,
            options=resolved,
        )

        bundle = DataFusionPlanArtifact(
            df=df,
            logical_plan=components.logical,
            optimized_logical_plan=components.optimized,
            execution_plan=components.execution,
            substrait_bytes=components.substrait_bytes,
            plan_fingerprint=components.fingerprint,
            artifacts=components.artifacts,
            delta_inputs=components.merged_delta_inputs,
            scan_units=components.scan_units,
            plan_identity_hash=components.plan_identity_hash,
            required_udfs=components.required_udfs,
            required_rewrite_tags=components.required_rewrite_tags,
            plan_details=components.plan_details,
        )
        if not bundle.plan_fingerprint:
            msg = "Plan bundle build produced an empty plan_fingerprint."
            raise ValueError(msg)
        _store_plan_cache_entry(
            bundle=bundle,
            runtime_profile=resolved.session_runtime.profile,
        )
        return bundle


def _store_plan_cache_entry(
    *,
    bundle: DataFusionPlanArtifact,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    cache = runtime_profile.plan_proto_cache
    if cache is None:
        return
    if bundle.plan_identity_hash is None:
        return
    artifacts = bundle.artifacts
    entry = PlanProtoCacheEntry(
        plan_identity_hash=bundle.plan_identity_hash,
        plan_fingerprint=bundle.plan_fingerprint,
        substrait_bytes=bundle.substrait_bytes,
        logical_plan_proto=_plan_proto_data(artifacts.logical_plan_proto),
        optimized_plan_proto=_plan_proto_data(artifacts.optimized_plan_proto),
        execution_plan_proto=_plan_proto_data(artifacts.execution_plan_proto),
    )
    cache.put(entry)


def _plan_proto_data(
    payload: LogicalPlanProtoBytes | OptimizedPlanProtoBytes | ExecutionPlanProtoBytes | None,
) -> bytes | None:
    if payload is None:
        return None
    return payload.data


__all__ = [
    "DataFrameBuilder",
    "DataFusionPlanArtifact",
    "DeltaInputPin",
    "PlanArtifacts",
    "PlanBundleOptions",
    "build_plan_artifact",
]
