"""Unified execution facade for DataFusion compilation and execution.

Internal execution paths use builder/plan-based approaches only.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion import DataFrame, SessionContext
from opentelemetry.semconv.attributes import db_attributes

from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.io.write import (
    WritePipeline,
    WriteRequest,
    WriteViewRequest,
)
from datafusion_engine.lineage.diagnostics import DiagnosticsRecorder, recorder_for_profile
from datafusion_engine.plan.bundle import (
    DataFusionPlanBundle,
    PlanBundleOptions,
    build_plan_bundle,
)
from datafusion_engine.plan.cache import PlanCacheEntry
from datafusion_engine.plan.result_types import (
    ExecutionResult,
    ExecutionResultKind,
)
from datafusion_engine.session.runtime import session_runtime_for_context
from obs.otel.metrics import record_datafusion_duration, record_error, record_write_duration
from obs.otel.scopes import SCOPE_DATAFUSION
from obs.otel.tracing import get_tracer, record_exception, set_span_attributes, span_attributes
from utils.validation import validate_required_items

if TYPE_CHECKING:
    from datafusion_engine.bootstrap.zero_row import (
        ZeroRowBootstrapReport,
        ZeroRowBootstrapRequest,
    )
    from datafusion_engine.dataset.registration import DataFusionCachePolicy
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.schema.introspection import SchemaIntrospector
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
    from semantics.program_manifest import ManifestDatasetResolver, SemanticProgramManifest


DataFrameBuilder = Callable[[SessionContext], DataFrame]


def _validate_required_rewrite_tags(
    snapshot: Mapping[str, object],
    *,
    required_tags: tuple[str, ...],
) -> None:
    if not required_tags:
        return
    from datafusion_engine.udf.catalog import rewrite_tag_index

    tag_index = rewrite_tag_index(snapshot)
    validate_required_items(
        required_tags,
        tag_index,
        item_label="rewrite tags at execution time",
        error_type=ValueError,
    )


def _ensure_udf_compatibility(ctx: SessionContext, bundle: DataFusionPlanBundle) -> None:
    """Fail fast when the execution UDF platform diverges from the plan bundle.

    Args:
        ctx: Description.
        bundle: Description.

    Raises:
        RuntimeError: If the operation cannot be completed.
    """
    from datafusion_engine.udf.runtime import (
        rust_udf_snapshot,
        rust_udf_snapshot_hash,
        validate_required_udfs,
    )

    snapshot = rust_udf_snapshot(ctx)
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    planned_hash = bundle.artifacts.udf_snapshot_hash
    if snapshot_hash != planned_hash:
        msg = (
            "UDF snapshot mismatch between planning and execution. "
            f"planned={planned_hash} execution={snapshot_hash}"
        )
        raise RuntimeError(msg)
    if bundle.required_udfs:
        validate_required_udfs(snapshot, required=bundle.required_udfs)
    _validate_required_rewrite_tags(snapshot, required_tags=bundle.required_rewrite_tags)


# NOTE: ExecutionResult and ExecutionResultKind were extracted to
# datafusion_engine.plan.result_types to break the circular dependency.
# They are imported and re-exported here for backward compatibility.


@dataclass(frozen=True)
class _ExecutionArtifactRequest:
    """Execution artifact persistence input."""

    bundle: DataFusionPlanBundle
    view_name: str | None
    duration_ms: float
    status: str
    error: str | None
    scan_units: Sequence[ScanUnit]
    scan_keys: Sequence[str]


@dataclass(frozen=True)
class DataFusionExecutionFacade:
    """Facade coordinating compilation, execution, registration, and writes.

    DataFusion-native planning and execution are the only supported surfaces.
    """

    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile | None = None

    def __post_init__(self) -> None:
        """Ensure planner extensions are installed before any plan operations.

        Planner extensions (Rust UDFs, ExprPlanner, FunctionFactory) are
        planning-critical features and must be installed before any
        plan-bundle construction.

        """
        from datafusion_engine.udf.platform import (
            RustUdfPlatformOptions,
            install_rust_udf_platform,
            native_udf_platform_available,
        )

        platform_strict = native_udf_platform_available()

        if self.runtime_profile is None:
            # Default configuration: enable all planner extensions
            options = RustUdfPlatformOptions(
                enable_udfs=True,
                enable_function_factory=True,
                enable_expr_planners=True,
                expr_planner_names=("codeanatomy_domain",),
                strict=platform_strict,
            )
            install_rust_udf_platform(self.ctx, options=options)
            return

        # Profile-driven configuration with strict validation
        features = self.runtime_profile.features
        policies = self.runtime_profile.policies
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
        install_rust_udf_platform(self.ctx, options=options)

    def io_adapter(self) -> DataFusionIOAdapter:
        """Return a DataFusionIOAdapter for the session context.

        Returns:
        -------
        DataFusionIOAdapter
            IO adapter bound to the DataFusion session context.
        """
        return DataFusionIOAdapter(ctx=self.ctx, profile=self.runtime_profile)

    def diagnostics_recorder(self, *, operation_id: str) -> DiagnosticsRecorder | None:
        """Return a diagnostics recorder when configured.

        Parameters
        ----------
        operation_id
            Operation identifier for the recorder.

        Returns:
        -------
        DiagnosticsRecorder | None
            Recorder if a runtime profile is configured.
        """
        if self.runtime_profile is None:
            return None
        return recorder_for_profile(self.runtime_profile, operation_id=operation_id)

    def write_pipeline(self) -> WritePipeline:
        """Return a WritePipeline bound to this facade.

        Returns:
        -------
        WritePipeline
            Write pipeline configured for the session context.
        """
        recorder = None
        if self.runtime_profile is not None:
            recorder = recorder_for_profile(self.runtime_profile, operation_id="write_pipeline")
        return WritePipeline(
            ctx=self.ctx,
            sql_options=self.runtime_profile.sql_options()
            if self.runtime_profile is not None
            else None,
            recorder=recorder,
            runtime_profile=self.runtime_profile,
        )

    def _session_runtime(self) -> SessionRuntime | None:
        """Return the planning-ready SessionRuntime when it matches the context.

        Returns:
        -------
        SessionRuntime | None
            Session runtime when the profile matches the session context.
        """
        if self.runtime_profile is None:
            return None
        session_runtime = self.runtime_profile.session_runtime()
        if session_runtime.ctx is self.ctx:
            return session_runtime
        return session_runtime_for_context(self.runtime_profile, self.ctx)

    def compile_to_bundle(
        self,
        builder: DataFrameBuilder,
        *,
        compute_execution_plan: bool = True,
    ) -> DataFusionPlanBundle:
        """Compile a DataFrame builder to a DataFusionPlanBundle.

        Args:
            builder: DataFrame builder callable.
            compute_execution_plan: Whether to include execution plan capture.

        Returns:
            DataFusionPlanBundle: Result.

        Raises:
            ValueError: If session runtime is unavailable.
        """
        session_runtime = self._session_runtime()
        if session_runtime is None:
            msg = "SessionRuntime is required to compile plan bundles."
            raise ValueError(msg)
        tracer = get_tracer(SCOPE_DATAFUSION)
        start = time.perf_counter()
        with tracer.start_as_current_span(
            "datafusion.plan.compile",
            attributes=span_attributes(
                attrs={
                    db_attributes.DB_SYSTEM_NAME: "datafusion",
                    db_attributes.DB_OPERATION_NAME: "plan.compile",
                    "plan_kind": "compile",
                    "compute_execution_plan": compute_execution_plan,
                }
            ),
        ) as span:
            try:
                df = builder(self.ctx)
                bundle = build_plan_bundle(
                    self.ctx,
                    df,
                    options=PlanBundleOptions(
                        compute_execution_plan=compute_execution_plan,
                        compute_substrait=True,
                        session_runtime=session_runtime,
                    ),
                )
            except Exception as exc:
                record_exception(span, exc)
                duration_s = time.perf_counter() - start
                record_error("datafusion", type(exc).__name__)
                record_datafusion_duration(duration_s, status="error", plan_kind="compile")
                set_span_attributes(
                    span,
                    {
                        "duration_s": duration_s,
                        "status": "error",
                        "plan_kind": "compile",
                    },
                )
                raise
            duration_s = time.perf_counter() - start
            record_datafusion_duration(duration_s, status="ok", plan_kind="compile")
            set_span_attributes(
                span,
                {
                    "plan_fingerprint": bundle.plan_fingerprint,
                    "plan_identity_hash": bundle.plan_identity_hash,
                    "duration_s": duration_s,
                },
            )
            return bundle

    def execute_plan_bundle(
        self,
        bundle: DataFusionPlanBundle,
        *,
        view_name: str | None = None,
        scan_units: Sequence[ScanUnit] = (),
        scan_keys: Sequence[str] = (),
    ) -> ExecutionResult:
        """Execute a plan bundle with Substrait-first replay.

        Args:
            bundle: Plan bundle to execute.
            view_name: Optional view name for telemetry context.
            scan_units: Scan units associated with the execution.
            scan_keys: Scan keys associated with the execution.

        Returns:
            ExecutionResult: Result.

        Raises:
            RuntimeError: If bundle execution fails.
            TypeError: If substrait replay APIs are unavailable.
            ValueError: If substrait payload is invalid.
        """
        tracer = get_tracer(SCOPE_DATAFUSION)
        start = time.perf_counter()
        plan_kind = "substrait"
        with tracer.start_as_current_span(
            "datafusion.execute",
            attributes=span_attributes(
                attrs={
                    db_attributes.DB_SYSTEM_NAME: "datafusion",
                    db_attributes.DB_OPERATION_NAME: "execute",
                    "view_name": view_name,
                    "plan_fingerprint": bundle.plan_fingerprint,
                    "plan_identity_hash": bundle.plan_identity_hash,
                }
            ),
        ) as span:
            try:
                _ensure_udf_compatibility(self.ctx, bundle)
                df, used_fallback = self._substrait_first_df(bundle)
                if used_fallback:
                    plan_kind = "fallback"
                    self._record_substrait_fallback(
                        bundle,
                        view_name=view_name,
                        reason="substrait_replay_failed",
                    )
                result = ExecutionResult.from_dataframe(df, plan_bundle=bundle)
            except (RuntimeError, ValueError, TypeError) as exc:
                duration_s = time.perf_counter() - start
                duration_ms = duration_s * 1000.0
                record_exception(span, exc)
                record_error("datafusion", type(exc).__name__)
                record_datafusion_duration(duration_s, status="error", plan_kind=plan_kind)
                from datafusion_engine.plan.diagnostics import (
                    PlanExecutionDiagnostics,
                    record_plan_execution_diagnostics,
                )

                record_plan_execution_diagnostics(
                    request=PlanExecutionDiagnostics(
                        bundle=bundle,
                        runtime_profile=self.runtime_profile,
                        view_name=view_name,
                        plan_kind=plan_kind,
                        status="error",
                        duration_ms=duration_ms,
                        error=str(exc),
                    ),
                )
                set_span_attributes(
                    span,
                    {
                        "duration_s": duration_s,
                        "status": "error",
                        "plan_kind": plan_kind,
                    },
                )
                self._record_execution_artifact(
                    _ExecutionArtifactRequest(
                        bundle=bundle,
                        view_name=view_name,
                        duration_ms=duration_ms,
                        status="error",
                        error=str(exc),
                        scan_units=scan_units,
                        scan_keys=scan_keys,
                    )
                )
                raise
            duration_s = time.perf_counter() - start
            duration_ms = duration_s * 1000.0
            record_datafusion_duration(duration_s, status="ok", plan_kind=plan_kind)
            from datafusion_engine.plan.diagnostics import (
                PlanExecutionDiagnostics,
                record_plan_execution_diagnostics,
            )

            record_plan_execution_diagnostics(
                request=PlanExecutionDiagnostics(
                    bundle=bundle,
                    runtime_profile=self.runtime_profile,
                    view_name=view_name,
                    plan_kind=plan_kind,
                    status="ok",
                    duration_ms=duration_ms,
                    error=None,
                ),
            )
            self._record_execution_artifact(
                _ExecutionArtifactRequest(
                    bundle=bundle,
                    view_name=view_name,
                    duration_ms=duration_ms,
                    status="ok",
                    error=None,
                    scan_units=scan_units,
                    scan_keys=scan_keys,
                )
            )
            set_span_attributes(
                span,
                {
                    "duration_s": duration_s,
                    "plan_kind": plan_kind,
                    "status": "ok",
                },
            )
            return result

    def _record_execution_artifact(self, request: _ExecutionArtifactRequest) -> None:
        """Persist an execution artifact row when runtime settings allow it."""
        if (
            self.runtime_profile is None
            or request.view_name is None
            or not self.runtime_profile.diagnostics.capture_plan_artifacts
        ):
            return
        from datafusion_engine.plan.artifact_store import (
            PlanArtifactBuildRequest,
            persist_execution_artifact,
        )

        try:
            persist_execution_artifact(
                self.ctx,
                self.runtime_profile,
                request=PlanArtifactBuildRequest(
                    view_name=request.view_name,
                    bundle=request.bundle,
                    scan_units=request.scan_units,
                    scan_keys=request.scan_keys,
                    execution_duration_ms=request.duration_ms,
                    execution_status=request.status,
                    execution_error=request.error,
                ),
            )
        except (RuntimeError, ValueError, OSError, KeyError, ImportError, TypeError) as exc:
            from datafusion_engine.lineage.diagnostics import record_artifact

            record_artifact(
                self.runtime_profile,
                "plan_artifacts_execution_failed_v1",
                {
                    "view_name": request.view_name,
                    "plan_fingerprint": request.bundle.plan_fingerprint,
                    "status": request.status,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            )

    def _substrait_first_df(
        self,
        bundle: DataFusionPlanBundle,
    ) -> tuple[DataFrame, bool]:
        """Return DataFrame using Substrait-first execution.

        Args:
            bundle: Description.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        substrait_bytes = bundle.substrait_bytes
        if not substrait_bytes:
            msg = "Plan bundle is missing Substrait bytes."
            raise ValueError(msg)
        cached_entry = self._plan_cache_entry(bundle)
        self._record_plan_cache_event(
            bundle,
            status="hit" if cached_entry is not None else "miss",
            source="substrait",
        )
        # Lazy import to avoid circular dependency with execution.py
        from datafusion_engine.plan.execution import replay_substrait_bytes

        try:
            df = replay_substrait_bytes(self.ctx, substrait_bytes)
        except (RuntimeError, TypeError, ValueError):
            cached_df = self._rehydrate_from_proto(bundle)
            if cached_df is not None:
                self._record_plan_cache_event(bundle, status="hit", source="proto")
                return cached_df, True
            self._record_plan_cache_event(bundle, status="miss", source="proto")
            return bundle.df, True
        else:
            return df, False

    def _plan_cache_entry(
        self,
        bundle: DataFusionPlanBundle,
    ) -> PlanCacheEntry | None:
        if self.runtime_profile is None:
            return None
        cache = self.runtime_profile.plan_proto_cache
        if cache is None:
            return None
        plan_identity_hash = bundle.plan_identity_hash
        if plan_identity_hash is None:
            return None
        entry = cache.get(plan_identity_hash)
        if isinstance(entry, PlanCacheEntry):
            return entry
        return None

    def _rehydrate_from_proto(
        self,
        bundle: DataFusionPlanBundle,
    ) -> DataFrame | None:
        cached_entry = self._plan_cache_entry(bundle)
        if cached_entry is None:
            return None
        from_proto = getattr(self.ctx, "from_proto", None)
        if not callable(from_proto):
            return None
        proto_candidates = (
            getattr(cached_entry, "execution_plan_proto", None),
            getattr(cached_entry, "optimized_plan_proto", None),
            getattr(cached_entry, "logical_plan_proto", None),
        )
        for payload in proto_candidates:
            if payload is None:
                continue
            try:
                df = from_proto(payload)
            except (RuntimeError, TypeError, ValueError):
                continue
            if isinstance(df, DataFrame):
                return df
        return None

    def _record_plan_cache_event(
        self,
        bundle: DataFusionPlanBundle,
        *,
        status: str,
        source: str,
    ) -> None:
        if self.runtime_profile is None:
            return
        if bundle.plan_identity_hash is None:
            return
        from datafusion_engine.lineage.diagnostics import record_artifact

        record_artifact(
            self.runtime_profile,
            "plan_cache_events_v1",
            {
                "plan_identity_hash": bundle.plan_identity_hash,
                "plan_fingerprint": bundle.plan_fingerprint,
                "status": status,
                "source": source,
            },
        )

    def _record_substrait_fallback(
        self,
        bundle: DataFusionPlanBundle,
        *,
        view_name: str | None,
        reason: str,
    ) -> None:
        """Record diagnostics when Substrait replay falls back.

        Parameters
        ----------
        bundle
            Plan bundle that fell back to original DataFrame.
        view_name
            Optional view name for diagnostics context.
        reason
            Reason for the fallback (e.g., 'replay_failed').
        """
        if self.runtime_profile is None:
            return
        from datafusion_engine.lineage.diagnostics import record_artifact

        substrait_bytes = bundle.substrait_bytes
        record_artifact(
            self.runtime_profile,
            "substrait_fallback_v1",
            {
                "view_name": view_name,
                "plan_fingerprint": bundle.plan_fingerprint,
                "reason": reason,
                "has_substrait_bytes": True,
                "substrait_bytes_len": len(substrait_bytes),
            },
        )

    def write(
        self,
        request: WriteRequest,
    ) -> ExecutionResult:
        """Execute a write request using the write pipeline.

        Parameters
        ----------
        request
            Write request specification.

        Returns:
        -------
        ExecutionResult
            Unified execution result for the write operation.
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
                pipeline = self.write_pipeline()
                result = pipeline.write(request)
            except Exception as exc:
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
        self,
        request: WriteViewRequest,
        *,
        prefer_streaming: bool = True,
    ) -> ExecutionResult:
        """Write a registered view using the write pipeline.

        Parameters
        ----------
        request
            Write request specifying the registered view.
        prefer_streaming
            Prefer streaming writes when possible.

        Returns:
        -------
        ExecutionResult
            Execution result wrapping the write metadata.
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
                pipeline = self.write_pipeline()
                result = pipeline.write_view(request, prefer_streaming=prefer_streaming)
            except Exception as exc:
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
        self,
        *,
        scan_units: Sequence[ScanUnit] = (),
        semantic_manifest: SemanticProgramManifest,
        dataset_resolver: ManifestDatasetResolver | None = None,
    ) -> Mapping[str, object]:
        """Ensure the view graph is registered for the current context.

        Parameters
        ----------
        scan_units
            Optional scan-unit overrides applied during registration.
        semantic_manifest
            Compiled semantic program manifest for registration.

        Returns:
            Mapping[str, object]: Rust UDF snapshot used for view registration.

        Raises:
            ValueError: If no runtime profile is configured.
        """
        from datafusion_engine.views.registration import ensure_view_graph

        if self.runtime_profile is None:
            msg = "Runtime profile is required for view registration."
            raise ValueError(msg)

        return ensure_view_graph(
            self.ctx,
            runtime_profile=self.runtime_profile,
            scan_units=tuple(scan_units),
            semantic_manifest=semantic_manifest,
            dataset_resolver=dataset_resolver,
        )

    def register_dataset(
        self,
        *,
        name: str,
        location: DatasetLocation,
        cache_policy: DataFusionCachePolicy | None = None,
        overwrite: bool = False,
    ) -> DataFrame:
        """Register a dataset location via the registry bridge.

        Args:
            name: Dataset name to register.
            location: Dataset location descriptor.
            cache_policy: Optional cache policy override.
            overwrite: Whether to overwrite an existing registration.

        Returns:
            DataFrame: Result.

        Raises:
            ValueError: If runtime profile is unavailable.
        """
        if self.runtime_profile is None:
            msg = "Runtime profile is required for dataset registration."
            raise ValueError(msg)
        from datafusion_engine.registry_facade import registry_facade_for_context

        registry_facade = registry_facade_for_context(
            self.ctx,
            runtime_profile=self.runtime_profile,
        )
        return registry_facade.register_dataset_df(
            name=name,
            location=location,
            cache_policy=cache_policy,
            overwrite=overwrite,
        )

    def register_cdf_inputs(
        self,
        *,
        table_names: Sequence[str],
        dataset_resolver: ManifestDatasetResolver | None = None,
    ) -> Mapping[str, str]:
        """Register Delta CDF-backed inputs for the provided table names.

        Args:
            table_names: Description.
            dataset_resolver: Description.

        Returns:
            Mapping[str, str]: Result.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if self.runtime_profile is None:
            msg = "Runtime profile is required for CDF registration."
            raise ValueError(msg)
        if not self.runtime_profile.features.enable_delta_cdf:
            msg = "Delta CDF registration requires enable_delta_cdf to be True."
            raise ValueError(msg)
        from datafusion_engine.delta.cdf import register_cdf_inputs

        if dataset_resolver is None:
            from semantics.compile_context import dataset_bindings_for_profile

            dataset_resolver = dataset_bindings_for_profile(self.runtime_profile)
        return register_cdf_inputs(
            self.ctx,
            self.runtime_profile,
            table_names=table_names,
            dataset_resolver=dataset_resolver,
        )

    def run_zero_row_bootstrap_validation(
        self,
        request: ZeroRowBootstrapRequest | None = None,
    ) -> ZeroRowBootstrapReport:
        """Run zero-row bootstrap validation using the facade context.

        Parameters
        ----------
        request
            Optional zero-row bootstrap request. When omitted, runtime defaults
            configured on the profile are used.

        Returns:
        -------
        ZeroRowBootstrapReport
            Bootstrap validation and materialization report.

        Raises:
            ValueError: If runtime profile is unavailable.
        """
        if self.runtime_profile is None:
            msg = "Runtime profile is required for zero-row bootstrap validation."
            raise ValueError(msg)
        return self.runtime_profile.run_zero_row_bootstrap_validation(
            request=request,
            ctx=self.ctx,
        )

    def schema_introspector(self) -> SchemaIntrospector:
        """Return a SchemaIntrospector bound to the facade context.

        Returns:
        -------
        SchemaIntrospector
            Introspector for information_schema queries.
        """
        from datafusion_engine.schema.introspection import SchemaIntrospector

        sql_options = (
            self.runtime_profile.sql_options() if self.runtime_profile is not None else None
        )
        return SchemaIntrospector(self.ctx, sql_options=sql_options)

    def build_plan_bundle(
        self,
        df: DataFrame,
        *,
        compute_execution_plan: bool = True,
        compute_substrait: bool = True,
    ) -> DataFusionPlanBundle:
        """Build a plan bundle from a DataFrame.

        Args:
            df: DataFrame to bundle.
            compute_execution_plan: Whether to include execution plan capture.
            compute_substrait: Whether to generate substrait bytes.

        Returns:
            DataFusionPlanBundle: Result.

        Raises:
            ValueError: If session runtime is unavailable.
        """
        session_runtime = self._session_runtime()
        if session_runtime is None:
            msg = "SessionRuntime is required to build plan bundles."
            raise ValueError(msg)
        return build_plan_bundle(
            self.ctx,
            df,
            options=PlanBundleOptions(
                compute_execution_plan=compute_execution_plan,
                compute_substrait=compute_substrait,
                session_runtime=session_runtime,
            ),
        )


__all__ = [
    "DataFrameBuilder",
    "DataFusionExecutionFacade",
    "ExecutionResult",
    "ExecutionResultKind",
]
