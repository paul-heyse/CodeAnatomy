"""View graph registration orchestration for semantic pipelines."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from datafusion import SessionContext

from datafusion_engine.views.graph import (
    SchemaContractViolationError,
    ViewGraphRuntimeOptions,
    register_view_graph,
)
from datafusion_engine.views.registry_specs import view_graph_nodes

if TYPE_CHECKING:
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.udf.platform import RustUdfPlatformOptions
    from semantics.ir import SemanticIR


def ensure_view_graph(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    scan_units: Sequence[ScanUnit] = (),
    semantic_ir: SemanticIR,
) -> Mapping[str, object]:
    """Install UDF platform (if needed) and register the semantic view graph.

    Parameters
    ----------
    ctx
        Active DataFusion session context.
    runtime_profile
        Runtime profile used for registration policies and diagnostics.
    scan_units
        Optional scan units that pin Delta versions and candidate files
        before view registration.
    semantic_ir
        Compiled semantic IR artifact used to define view registration.

    Returns
    -------
    Mapping[str, object]
        Rust UDF snapshot used for view registration.

    Raises
    ------
    ValueError
        Raised when the runtime profile is unavailable.
    """
    if runtime_profile is None:
        msg = "Runtime profile is required for view registration."
        raise ValueError(msg)
    from datafusion_engine.udf.platform import install_rust_udf_platform
    from datafusion_engine.udf.runtime import rust_udf_snapshot

    options = _platform_options(runtime_profile)
    platform = install_rust_udf_platform(ctx, options=options)
    snapshot = platform.snapshot or rust_udf_snapshot(ctx)
    if scan_units:
        from datafusion_engine.dataset.resolution import apply_scan_unit_overrides

        apply_scan_unit_overrides(
            ctx,
            scan_units=scan_units,
            runtime_profile=runtime_profile,
        )
    nodes = view_graph_nodes(
        ctx,
        snapshot=snapshot,
        runtime_profile=runtime_profile,
        semantic_ir=semantic_ir,
    )
    try:
        register_view_graph(
            ctx,
            nodes=nodes,
            snapshot=snapshot,
            runtime_options=ViewGraphRuntimeOptions(
                runtime_profile=runtime_profile,
                require_artifacts=True,
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
            record_artifact(runtime_profile, "view_contract_violations_v1", payload)
        raise
    from datafusion_engine.lineage.diagnostics import (
        record_artifact,
        rust_udf_snapshot_payload,
        view_fingerprint_payload,
        view_udf_parity_payload,
    )

    record_artifact(
        runtime_profile,
        "rust_udf_snapshot_v1",
        rust_udf_snapshot_payload(snapshot),
    )
    record_artifact(
        runtime_profile,
        "view_udf_parity_v1",
        view_udf_parity_payload(snapshot=snapshot, view_nodes=nodes, ctx=ctx),
    )
    record_artifact(
        runtime_profile,
        "view_fingerprints_v1",
        view_fingerprint_payload(view_nodes=nodes),
    )
    return snapshot


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
    return RustUdfPlatformOptions(
        enable_udfs=getattr(profile, "enable_udfs", True),
        enable_async_udfs=getattr(profile, "enable_async_udfs", False),
        async_udf_timeout_ms=getattr(profile, "async_udf_timeout_ms", None),
        async_udf_batch_size=getattr(profile, "async_udf_batch_size", None),
        enable_function_factory=getattr(profile, "enable_function_factory", True),
        enable_expr_planners=getattr(profile, "enable_expr_planners", True),
        function_factory_hook=getattr(profile, "function_factory_hook", None),
        expr_planner_hook=getattr(profile, "expr_planner_hook", None),
        expr_planner_names=getattr(profile, "expr_planner_names", ()),
        strict=True,
    )


__all__ = ["ensure_view_graph"]
