"""Unified execution facade for DataFusion compilation and execution."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion import DataFrame, SessionContext
from opentelemetry.semconv.attributes import db_attributes

from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.io.write_core import (
    WritePipeline,
    WriteRequest,
    WriteViewRequest,
)
from datafusion_engine.lineage.diagnostics import DiagnosticsRecorder
from datafusion_engine.plan.bundle_artifact import (
    DataFusionPlanArtifact,
    PlanBundleOptions,
    build_plan_artifact,
)
from datafusion_engine.plan.cache import PlanCacheEntry
from datafusion_engine.plan.result_types import (
    ExecutionResult,
    ExecutionResultKind,
)
from datafusion_engine.session import facade_ops
from obs.otel import (
    SCOPE_DATAFUSION,
    get_tracer,
    record_datafusion_duration,
    record_error,
    record_exception,
    set_span_attributes,
    span_attributes,
)
from utils.validation import validate_required_items

if TYPE_CHECKING:
    from datafusion_engine.bootstrap.zero_row import (
        ZeroRowBootstrapReport,
        ZeroRowBootstrapRequest,
    )
    from datafusion_engine.dataset.registration_core import DataFusionCachePolicy
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.lineage.scheduling import ScanUnit
    from datafusion_engine.schema.introspection_core import SchemaIntrospector
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.session.runtime_session import SessionRuntime
    from semantics.program_manifest import ManifestDatasetResolver, SemanticProgramManifest

DataFrameBuilder = Callable[[SessionContext], DataFrame]


def _validate_required_rewrite_tags(
    snapshot: Mapping[str, object],
    *,
    required_tags: tuple[str, ...],
) -> None:
    if not required_tags:
        return
    from datafusion_engine.udf.metadata import rewrite_tag_index

    tag_index = rewrite_tag_index(snapshot)
    validate_required_items(
        required_tags,
        tag_index,
        item_label="rewrite tags at execution time",
        error_type=ValueError,
    )


def _ensure_udf_compatibility(ctx: SessionContext, bundle: DataFusionPlanArtifact) -> None:
    """Fail fast when the execution UDF platform diverges from the plan bundle.

    Args:
        ctx: Description.
        bundle: Description.

    Raises:
        RuntimeError: If the operation cannot be completed.
    """
    from datafusion_engine.udf.extension_core import (
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

    bundle: DataFusionPlanArtifact
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
        facade_ops.install_planner_extensions(self.ctx, self.runtime_profile)

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
        return facade_ops.diagnostics_recorder(
            self.runtime_profile,
            operation_id=operation_id,
        )

    def write_pipeline(self) -> WritePipeline:
        """Return a WritePipeline bound to this facade.

        Returns:
        -------
        WritePipeline
            Write pipeline configured for the session context.
        """
        return facade_ops.write_pipeline(self.ctx, self.runtime_profile)

    def _session_runtime(self) -> SessionRuntime | None:
        """Return the planning-ready SessionRuntime when it matches the context.

        Returns:
        -------
        SessionRuntime | None
            Session runtime when the profile matches the session context.
        """
        return facade_ops.session_runtime(self.ctx, self.runtime_profile)

    def compile_to_bundle(
        self,
        builder: DataFrameBuilder,
        *,
        compute_execution_plan: bool = True,
    ) -> DataFusionPlanArtifact:
        """Compile a DataFrame builder to a DataFusionPlanArtifact.

        Args:
            builder: DataFrame builder callable.
            compute_execution_plan: Whether to include execution plan capture.

        Returns:
            DataFusionPlanArtifact: Result.

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
                bundle = build_plan_artifact(
                    self.ctx,
                    df,
                    options=PlanBundleOptions(
                        compute_execution_plan=compute_execution_plan,
                        compute_substrait=True,
                        session_runtime=session_runtime,
                    ),
                )
            except Exception as exc:  # intentionally broad: DataFusion backend errors are dynamic
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

    def execute_plan_artifact(
        self,
        bundle: DataFusionPlanArtifact,
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
                df = self._substrait_first_df(bundle)
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
        from datafusion_engine.plan.artifact_store_core import (
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
            from serde_artifact_specs import PLAN_ARTIFACTS_EXECUTION_FAILED_SPEC

            record_artifact(
                self.runtime_profile,
                PLAN_ARTIFACTS_EXECUTION_FAILED_SPEC,
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
        bundle: DataFusionPlanArtifact,
    ) -> DataFrame:
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
        # Lazy import to avoid circular dependency
        from datafusion_engine.plan.result_types import replay_substrait_bytes

        return replay_substrait_bytes(self.ctx, substrait_bytes)

    def _plan_cache_entry(
        self,
        bundle: DataFusionPlanArtifact,
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

    def _record_plan_cache_event(
        self,
        bundle: DataFusionPlanArtifact,
        *,
        status: str,
        source: str,
    ) -> None:
        if self.runtime_profile is None:
            return
        if bundle.plan_identity_hash is None:
            return
        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import PLAN_CACHE_EVENTS_SPEC

        record_artifact(
            self.runtime_profile,
            PLAN_CACHE_EVENTS_SPEC,
            {
                "plan_identity_hash": bundle.plan_identity_hash,
                "plan_fingerprint": bundle.plan_fingerprint,
                "status": status,
                "source": source,
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
        return facade_ops.write(self.ctx, self.runtime_profile, request)

    def write_view(
        self,
        request: WriteViewRequest,
    ) -> ExecutionResult:
        """Write a registered view using the write pipeline.

        Parameters
        ----------
        request
            Write request specifying the registered view.

        Returns:
        -------
        ExecutionResult
            Execution result wrapping the write metadata.
        """
        return facade_ops.write_view(self.ctx, self.runtime_profile, request)

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

        """
        return facade_ops.ensure_view_graph(
            self.ctx,
            self.runtime_profile,
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

        """
        return facade_ops.register_dataset(
            self.ctx,
            self.runtime_profile,
            name=name,
            location=location,
            cache_policy=cache_policy,
            overwrite=overwrite,
        )

    def register_cdf_inputs(
        self,
        *,
        table_names: Sequence[str],
        dataset_resolver: ManifestDatasetResolver,
    ) -> Mapping[str, str]:
        """Register Delta CDF-backed inputs for the provided table names.

        Args:
            table_names: Description.
            dataset_resolver: Description.

        Returns:
            Mapping[str, str]: Result.

        """
        return facade_ops.register_cdf_inputs(
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

        """
        return facade_ops.run_zero_row_bootstrap_validation(
            self.ctx,
            self.runtime_profile,
            request=request,
        )

    def schema_introspector(self) -> SchemaIntrospector:
        """Return a SchemaIntrospector bound to the facade context.

        Returns:
        -------
        SchemaIntrospector
            Introspector for information_schema queries.
        """
        return facade_ops.schema_introspector(self.ctx, self.runtime_profile)

    def build_plan_artifact(
        self,
        df: DataFrame,
        *,
        compute_execution_plan: bool = True,
        compute_substrait: bool = True,
    ) -> DataFusionPlanArtifact:
        """Build a plan bundle from a DataFrame.

        Args:
            df: DataFrame to bundle.
            compute_execution_plan: Whether to include execution plan capture.
            compute_substrait: Whether to generate substrait bytes.

        Returns:
            DataFusionPlanArtifact: Result.

        Raises:
            ValueError: If session runtime is unavailable.
        """
        session_runtime = self._session_runtime()
        if session_runtime is None:
            msg = "SessionRuntime is required to build plan bundles."
            raise ValueError(msg)
        return build_plan_artifact(
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
