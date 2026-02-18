"""Operational helpers extracted from ``session.facade``."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

import msgspec
from datafusion import SessionContext
from opentelemetry.semconv.attributes import db_attributes

from datafusion_engine.io.write_core import (
    WritePipeline,
    WriteRequest,
    WriteViewRequest,
)
from datafusion_engine.plan.result_types import ExecutionResult
from datafusion_engine.session.runtime_session import session_runtime_for_context
from obs.otel import (
    SCOPE_DATAFUSION,
    get_tracer,
    record_error,
    record_exception,
    record_write_duration,
    set_span_attributes,
    span_attributes,
)

if TYPE_CHECKING:
    from datafusion import DataFrame

    from datafusion_engine.bootstrap.zero_row import (
        ZeroRowBootstrapReport,
        ZeroRowBootstrapRequest,
    )
    from datafusion_engine.dataset.registration_core import DataFusionCachePolicy
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.lineage.diagnostics import DiagnosticsRecorder
    from datafusion_engine.lineage.scheduling import ScanUnit
    from datafusion_engine.schema.introspection_core import SchemaIntrospector
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.session.runtime_session import SessionRuntime
    from semantics.program_manifest import ManifestDatasetResolver, SemanticProgramManifest


def install_planner_extensions(
    ctx: SessionContext,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> None:
    """Install Rust UDF platform features for facade operations."""
    from datafusion_engine.udf.contracts import InstallRustUdfPlatformRequestV1
    from datafusion_engine.udf.extension_validation import extension_capabilities_report
    from datafusion_engine.udf.platform import (
        RustUdfPlatformOptions,
        install_rust_udf_platform,
    )

    try:
        capabilities = extension_capabilities_report()
    except (RuntimeError, TypeError, ValueError):
        capabilities = {"available": False, "compatible": False}
    platform_strict = bool(capabilities.get("available")) and bool(capabilities.get("compatible"))

    if runtime_profile is None:
        options = RustUdfPlatformOptions(
            enable_udfs=True,
            enable_function_factory=True,
            enable_expr_planners=True,
            expr_planner_names=("codeanatomy_domain",),
            strict=platform_strict,
        )
        install_rust_udf_platform(
            InstallRustUdfPlatformRequestV1(options=msgspec.to_builtins(options)),
            ctx=ctx,
        )
        return

    features = runtime_profile.features
    policies = runtime_profile.policies
    options = RustUdfPlatformOptions(
        enable_udfs=features.enable_udfs,
        enable_async_udfs=features.enable_async_udfs,
        async_udf_timeout_ms=policies.async_udf_timeout_ms,
        async_udf_batch_size=policies.async_udf_batch_size,
        enable_function_factory=features.enable_function_factory,
        enable_expr_planners=features.enable_expr_planners,
        function_factory_hook=policies.function_factory_hook,
        expr_planner_hook=policies.expr_planner_hook,
        expr_planner_names=policies.expr_planner_names,
        strict=platform_strict,
    )
    install_rust_udf_platform(
        InstallRustUdfPlatformRequestV1(options=msgspec.to_builtins(options)),
        ctx=ctx,
    )


def diagnostics_recorder(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    operation_id: str,
) -> DiagnosticsRecorder | None:
    """Return diagnostics recorder when runtime profile is configured."""
    if runtime_profile is None:
        return None
    from datafusion_engine.lineage.diagnostics import recorder_for_profile

    return recorder_for_profile(runtime_profile, operation_id=operation_id)


def write_pipeline(
    ctx: SessionContext,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> WritePipeline:
    """Build a ``WritePipeline`` bound to facade context/runtime.

    Returns:
    -------
    WritePipeline
        Write pipeline configured for the context and runtime profile.
    """
    recorder = None
    if runtime_profile is not None:
        from datafusion_engine.lineage.diagnostics import recorder_for_profile

        recorder = recorder_for_profile(runtime_profile, operation_id="write_pipeline")
    return WritePipeline(
        ctx=ctx,
        sql_options=runtime_profile.sql_options() if runtime_profile is not None else None,
        recorder=recorder,
        runtime_profile=runtime_profile,
    )


def session_runtime(
    ctx: SessionContext,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> SessionRuntime | None:
    """Return planning-ready ``SessionRuntime`` matching the facade context.

    Returns:
    -------
    SessionRuntime | None
        Matching runtime when available.
    """
    if runtime_profile is None:
        return None
    resolved = runtime_profile.session_runtime()
    if resolved.ctx is ctx:
        return resolved
    return session_runtime_for_context(runtime_profile, ctx)


def write(
    ctx: SessionContext,
    runtime_profile: DataFusionRuntimeProfile | None,
    request: WriteRequest,
) -> ExecutionResult:
    """Execute a write request using write pipeline with telemetry.

    Returns:
    -------
    ExecutionResult
        Unified write execution result.
    """
    tracer = get_tracer(SCOPE_DATAFUSION)
    start = time.perf_counter()
    with tracer.start_as_current_span(
        "datafusion.write",
        attributes=span_attributes(
            attrs={
                db_attributes.DB_SYSTEM_NAME: "datafusion",
                db_attributes.DB_OPERATION_NAME: "write",
                "destination": request.destination,
                "mode": request.mode,
                "format": request.format,
            }
        ),
    ) as span:
        try:
            pipeline = write_pipeline(ctx, runtime_profile)
            result = pipeline.write(request)
        except Exception as exc:  # intentionally broad: backend errors are dynamic
            record_exception(span, exc)
            duration_s = time.perf_counter() - start
            record_error("datafusion", type(exc).__name__)
            record_write_duration(
                duration_s,
                status="error",
                destination=request.destination,
            )
            set_span_attributes(span, {"duration_s": duration_s, "status": "error"})
            raise
        duration_s = time.perf_counter() - start
        record_write_duration(
            duration_s,
            status="ok",
            destination=request.destination,
        )
        set_span_attributes(span, {"duration_s": duration_s, "status": "ok"})
        return ExecutionResult.from_write(result)


def write_view(
    ctx: SessionContext,
    runtime_profile: DataFusionRuntimeProfile | None,
    request: WriteViewRequest,
) -> ExecutionResult:
    """Write a registered view using write pipeline with telemetry.

    Returns:
    -------
    ExecutionResult
        Unified write execution result.
    """
    tracer = get_tracer(SCOPE_DATAFUSION)
    start = time.perf_counter()
    with tracer.start_as_current_span(
        "datafusion.write_view",
        attributes=span_attributes(
            attrs={
                db_attributes.DB_SYSTEM_NAME: "datafusion",
                db_attributes.DB_OPERATION_NAME: "write_view",
                "destination": request.destination,
                "view_name": request.view_name,
                "mode": request.mode,
            }
        ),
    ) as span:
        try:
            pipeline = write_pipeline(ctx, runtime_profile)
            result = pipeline.write_view(request)
        except Exception as exc:  # intentionally broad: backend errors are dynamic
            record_exception(span, exc)
            duration_s = time.perf_counter() - start
            record_error("datafusion", type(exc).__name__)
            record_write_duration(
                duration_s,
                status="error",
                destination=request.destination,
            )
            set_span_attributes(span, {"duration_s": duration_s, "status": "error"})
            raise
        duration_s = time.perf_counter() - start
        record_write_duration(
            duration_s,
            status="ok",
            destination=request.destination,
        )
        set_span_attributes(span, {"duration_s": duration_s, "status": "ok"})
        return ExecutionResult.from_write(result)


def ensure_view_graph(
    ctx: SessionContext,
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    scan_units: Sequence[ScanUnit],
    semantic_manifest: SemanticProgramManifest,
    dataset_resolver: ManifestDatasetResolver | None,
) -> Mapping[str, object]:
    """Ensure the view graph is registered for the current context.

    Returns:
    -------
    Mapping[str, object]
        Runtime UDF snapshot payload from registration.

    Raises:
        ValueError: If no runtime profile is configured.
    """
    if runtime_profile is None:
        msg = "Runtime profile is required for view registration."
        raise ValueError(msg)
    from datafusion_engine.views.registration import ensure_view_graph as _ensure_view_graph

    return _ensure_view_graph(
        ctx,
        runtime_profile=runtime_profile,
        scan_units=tuple(scan_units),
        semantic_manifest=semantic_manifest,
        dataset_resolver=dataset_resolver,
    )


def register_dataset(
    ctx: SessionContext,
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    name: str,
    location: DatasetLocation,
    cache_policy: DataFusionCachePolicy | None,
    overwrite: bool,
) -> DataFrame:
    """Register dataset location via registry bridge.

    Returns:
    -------
    DataFrame
        DataFusion dataframe for the registered dataset.

    Raises:
        ValueError: If no runtime profile is configured.
    """
    if runtime_profile is None:
        msg = "Runtime profile is required for dataset registration."
        raise ValueError(msg)
    from datafusion_engine.registry_facade import registry_facade_for_context
    from semantics.program_manifest import ManifestDatasetBindings

    registry_facade = registry_facade_for_context(
        ctx,
        runtime_profile=runtime_profile,
        dataset_resolver=ManifestDatasetBindings(locations={}),
    )
    return registry_facade.register_dataset_df(
        name=name,
        location=location,
        cache_policy=cache_policy,
        overwrite=overwrite,
    )


def register_cdf_inputs(
    ctx: SessionContext,
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    table_names: Sequence[str],
    dataset_resolver: ManifestDatasetResolver,
) -> Mapping[str, str]:
    """Register Delta CDF-backed inputs for provided table names.

    Returns:
    -------
    Mapping[str, str]
        Mapping of source table names to registered CDF view names.

    Raises:
        ValueError: If runtime profile or CDF feature requirements are not met.
    """
    if runtime_profile is None:
        msg = "Runtime profile is required for CDF registration."
        raise ValueError(msg)
    if not runtime_profile.features.enable_delta_cdf:
        msg = "Delta CDF registration requires enable_delta_cdf to be True."
        raise ValueError(msg)
    from datafusion_engine.delta.cdf import register_cdf_inputs as _register_cdf_inputs

    return _register_cdf_inputs(
        ctx,
        runtime_profile,
        table_names=table_names,
        dataset_resolver=dataset_resolver,
    )


def run_zero_row_bootstrap_validation(
    ctx: SessionContext,
    runtime_profile: DataFusionRuntimeProfile | None,
    request: ZeroRowBootstrapRequest | None,
) -> ZeroRowBootstrapReport:
    """Run zero-row bootstrap validation using facade context.

    Returns:
    -------
    ZeroRowBootstrapReport
        Validation and materialization report.

    Raises:
        ValueError: If no runtime profile is configured.
    """
    if runtime_profile is None:
        msg = "Runtime profile is required for zero-row bootstrap validation."
        raise ValueError(msg)
    return runtime_profile.run_zero_row_bootstrap_validation(
        request=request,
        ctx=ctx,
    )


def schema_introspector(
    ctx: SessionContext,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> SchemaIntrospector:
    """Build schema introspector for facade context.

    Returns:
    -------
    SchemaIntrospector
        Introspector bound to the provided context/profile.
    """
    from datafusion_engine.schema.introspection_core import SchemaIntrospector

    sql_options = runtime_profile.sql_options() if runtime_profile is not None else None
    return SchemaIntrospector(ctx, sql_options=sql_options)


__all__ = [
    "diagnostics_recorder",
    "ensure_view_graph",
    "install_planner_extensions",
    "register_cdf_inputs",
    "register_dataset",
    "run_zero_row_bootstrap_validation",
    "schema_introspector",
    "session_runtime",
    "write",
    "write_pipeline",
    "write_view",
]
