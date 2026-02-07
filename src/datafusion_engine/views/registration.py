"""View graph registration orchestration for semantic pipelines."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion import SessionContext

from datafusion_engine.registry_facade import RegistrationPhase, RegistrationPhaseOrchestrator
from datafusion_engine.views.graph import (
    SchemaContractViolationError,
    ViewGraphRuntimeOptions,
    register_view_graph,
    view_graph_registry,
)
from datafusion_engine.views.registry_specs import view_graph_nodes
from serde_artifact_specs import (
    RUST_UDF_SNAPSHOT_SPEC,
    VIEW_CONTRACT_VIOLATIONS_SPEC,
    VIEW_FINGERPRINTS_SPEC,
    VIEW_UDF_PARITY_SPEC,
)

if TYPE_CHECKING:
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.udf.platform import RustUdfPlatformOptions
    from datafusion_engine.views.graph import ViewNode
    from semantics.program_manifest import ManifestDatasetResolver, SemanticProgramManifest


@dataclass
class _ViewGraphRegistrationContext:
    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile
    scan_units: Sequence[ScanUnit]
    semantic_manifest: SemanticProgramManifest
    dataset_resolver: ManifestDatasetResolver | None = None
    snapshot: Mapping[str, object] | None = None
    nodes: Sequence[ViewNode] = ()

    @staticmethod
    def validate_extract_metadata() -> None:
        from datafusion_engine.extract.metadata import extract_metadata_specs

        if not extract_metadata_specs():
            msg = "Extract metadata registry is empty."
            raise ValueError(msg)

    @staticmethod
    def validate_semantic_registry() -> None:
        from semantics.registry import semantic_table_registry

        if len(semantic_table_registry()) == 0:
            msg = "Semantic table registry is empty."
            raise ValueError(msg)

    def install_udf_platform(self) -> None:
        from datafusion_engine.udf.platform import install_rust_udf_platform
        from datafusion_engine.udf.runtime import rust_udf_snapshot

        options = _platform_options(self.runtime_profile)
        platform = install_rust_udf_platform(self.ctx, options=options)
        self.snapshot = platform.snapshot or rust_udf_snapshot(self.ctx)
        if self.snapshot is None:
            msg = "Rust UDF snapshot is required for view registration."
            raise RuntimeError(msg)

    def apply_scan_overrides(self) -> None:
        if not self.scan_units:
            return
        from datafusion_engine.dataset.resolution import apply_scan_unit_overrides

        apply_scan_unit_overrides(
            self.ctx,
            scan_units=self.scan_units,
            runtime_profile=self.runtime_profile,
            dataset_resolver=self.dataset_resolver,
        )

    def build_view_nodes(self) -> None:
        if self.snapshot is None:
            msg = "UDF snapshot is required before building view nodes."
            raise RuntimeError(msg)
        self.nodes = view_graph_nodes(
            self.ctx,
            snapshot=self.snapshot,
            runtime_profile=self.runtime_profile,
            semantic_ir=self.semantic_manifest.semantic_ir,
            manifest=self.semantic_manifest,
        )
        _ = view_graph_registry(self.nodes)

    def register_view_graph(self) -> None:
        if self.snapshot is None:
            msg = "UDF snapshot is required before view graph registration."
            raise RuntimeError(msg)
        try:
            register_view_graph(
                self.ctx,
                nodes=self.nodes,
                snapshot=self.snapshot,
                runtime_options=ViewGraphRuntimeOptions(
                    runtime_profile=self.runtime_profile,
                    require_artifacts=True,
                    dataset_resolver=self.dataset_resolver,
                ),
            )
        except Exception as exc:
            if isinstance(exc, SchemaContractViolationError):
                from datafusion_engine.lineage.diagnostics import record_artifact

                payload = {
                    "view": exc.table_name,
                    "violations": [
                        {
                            "violation_type": violation.violation_type.value,
                            "column_name": violation.column_name,
                            "expected": violation.expected,
                            "actual": violation.actual,
                        }
                        for violation in exc.violations
                    ],
                }
                record_artifact(self.runtime_profile, VIEW_CONTRACT_VIOLATIONS_SPEC, payload)
            raise


def _view_graph_phases(
    registration: _ViewGraphRegistrationContext,
) -> tuple[RegistrationPhase, ...]:
    phases: list[RegistrationPhase] = [
        RegistrationPhase(
            name="extract_metadata",
            validate=registration.validate_extract_metadata,
        ),
        RegistrationPhase(
            name="semantic_registry",
            requires=("extract_metadata",),
            validate=registration.validate_semantic_registry,
        ),
        RegistrationPhase(
            name="udf_platform",
            requires=("semantic_registry",),
            validate=registration.install_udf_platform,
        ),
    ]
    if registration.scan_units:
        phases.append(
            RegistrationPhase(
                name="scan_overrides",
                requires=("udf_platform",),
                validate=registration.apply_scan_overrides,
            )
        )
        view_nodes_requires = ("scan_overrides",)
    else:
        view_nodes_requires = ("udf_platform",)
    phases.append(
        RegistrationPhase(
            name="view_nodes",
            requires=view_nodes_requires,
            validate=registration.build_view_nodes,
        )
    )
    phases.append(
        RegistrationPhase(
            name="view_registration",
            requires=("view_nodes",),
            validate=registration.register_view_graph,
        )
    )
    return tuple(phases)


def ensure_view_graph(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    scan_units: Sequence[ScanUnit] = (),
    semantic_manifest: SemanticProgramManifest,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> Mapping[str, object]:
    """Install UDF platform (if needed) and register the semantic view graph.

    Args:
        ctx: DataFusion session context.
        runtime_profile: Active runtime profile.
        scan_units: Optional scan units for registration context.
        semantic_manifest: Compiled semantic program manifest.
        dataset_resolver: Optional manifest-based dataset resolver.

    Returns:
        Mapping[str, object]: Result.

    Raises:
        RuntimeError: If registration completes without producing a UDF snapshot.
        ValueError: If runtime profile is missing.
    """
    if runtime_profile is None:
        msg = "Runtime profile is required for view registration."
        raise ValueError(msg)
    if dataset_resolver is not None:
        from semantics.resolver_identity import record_resolver_if_tracking

        record_resolver_if_tracking(dataset_resolver, label="view_registration")
    registration = _ViewGraphRegistrationContext(
        ctx=ctx,
        runtime_profile=runtime_profile,
        scan_units=scan_units,
        semantic_manifest=semantic_manifest,
        dataset_resolver=dataset_resolver,
    )
    RegistrationPhaseOrchestrator().run(_view_graph_phases(registration))
    if registration.snapshot is None:
        msg = "UDF snapshot is required for view registration."
        raise RuntimeError(msg)
    from datafusion_engine.lineage.diagnostics import (
        record_artifact,
        rust_udf_snapshot_payload,
        view_fingerprint_payload,
        view_udf_parity_payload,
    )

    record_artifact(
        runtime_profile,
        RUST_UDF_SNAPSHOT_SPEC,
        rust_udf_snapshot_payload(registration.snapshot),
    )
    record_artifact(
        runtime_profile,
        VIEW_UDF_PARITY_SPEC,
        view_udf_parity_payload(
            snapshot=registration.snapshot,
            view_nodes=registration.nodes,
            ctx=ctx,
        ),
    )
    record_artifact(
        runtime_profile,
        VIEW_FINGERPRINTS_SPEC,
        view_fingerprint_payload(view_nodes=registration.nodes),
    )
    return registration.snapshot


def _platform_options(
    runtime_profile: DataFusionRuntimeProfile | None,
) -> RustUdfPlatformOptions:
    from datafusion_engine.udf.platform import RustUdfPlatformOptions

    profile = runtime_profile
    if profile is None:
        return RustUdfPlatformOptions(
            enable_udfs=True,
            enable_function_factory=True,
            enable_expr_planners=True,
            expr_planner_names=("codeanatomy_domain",),
            strict=True,
        )
    features = profile.features
    policies = profile.policies
    return RustUdfPlatformOptions(
        enable_udfs=features.enable_udfs,
        enable_async_udfs=features.enable_async_udfs,
        async_udf_timeout_ms=policies.async_udf_timeout_ms,
        async_udf_batch_size=policies.async_udf_batch_size,
        enable_function_factory=features.enable_function_factory,
        enable_expr_planners=features.enable_expr_planners,
        function_factory_hook=policies.function_factory_hook,
        expr_planner_hook=policies.expr_planner_hook,
        expr_planner_names=policies.expr_planner_names,
        strict=True,
    )


__all__ = ["ensure_view_graph"]
