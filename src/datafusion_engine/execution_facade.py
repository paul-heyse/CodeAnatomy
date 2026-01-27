"""Unified execution facade for DataFusion compilation and execution.

Internal execution paths use builder/plan-based approaches only.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from datafusion import DataFrame, SessionContext

from datafusion_engine.diagnostics import DiagnosticsRecorder, recorder_for_profile
from datafusion_engine.execution_helpers import replay_substrait_bytes
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.plan_bundle import (
    DataFusionPlanBundle,
    build_plan_bundle,
)
from datafusion_engine.write_pipeline import (
    WritePipeline,
    WriteRequest,
    WriteResult,
    WriteViewRequest,
)

if TYPE_CHECKING:
    from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
    from datafusion_engine.dataset_registry import DatasetLocation
    from datafusion_engine.registry_bridge import DataFusionCachePolicy
    from datafusion_engine.runtime import DataFusionRuntimeProfile, SessionRuntime
    from datafusion_engine.scan_planner import ScanUnit
    from datafusion_engine.schema_introspection import SchemaIntrospector


DataFrameBuilder = Callable[[SessionContext], DataFrame]


def _validate_required_rewrite_tags(
    snapshot: Mapping[str, object],
    *,
    required_tags: tuple[str, ...],
) -> None:
    if not required_tags:
        return
    from datafusion_engine.udf_catalog import rewrite_tag_index

    tag_index = rewrite_tag_index(snapshot)
    missing = [tag for tag in required_tags if tag not in tag_index]
    if missing:
        msg = f"Missing required rewrite tags at execution time: {sorted(missing)}."
        raise ValueError(msg)


def _ensure_udf_compatibility(ctx: SessionContext, bundle: DataFusionPlanBundle) -> None:
    """Fail fast when the execution UDF platform diverges from the plan bundle.

    Raises
    ------
    RuntimeError
        Raised when the execution UDF snapshot hash does not match the plan bundle.
    """
    from datafusion_engine.udf_runtime import (
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


class ExecutionResultKind(StrEnum):
    """Execution result kind discriminator."""

    DATAFRAME = "dataframe"
    TABLE = "table"
    READER = "reader"
    WRITE = "write"


@dataclass(frozen=True)
class ExecutionResult:
    """Unified execution result wrapper."""

    kind: ExecutionResultKind
    dataframe: DataFrame | None = None
    table: TableLike | None = None
    reader: RecordBatchReaderLike | None = None
    write_result: WriteResult | None = None
    plan_bundle: DataFusionPlanBundle | None = None

    @staticmethod
    def from_dataframe(
        df: DataFrame,
        *,
        plan_bundle: DataFusionPlanBundle | None = None,
    ) -> ExecutionResult:
        """Wrap a DataFusion DataFrame.

        Parameters
        ----------
        df
            DataFusion DataFrame to wrap.
        plan_bundle
            Optional plan bundle for lineage tracking.

        Returns
        -------
        ExecutionResult
            Wrapped execution result.
        """
        return ExecutionResult(
            kind=ExecutionResultKind.DATAFRAME,
            dataframe=df,
            plan_bundle=plan_bundle,
        )

    @staticmethod
    def from_table(table: TableLike) -> ExecutionResult:
        """Wrap a materialized table-like object.

        Parameters
        ----------
        table
            Materialized table-like object.

        Returns
        -------
        ExecutionResult
            Wrapped execution result.
        """
        return ExecutionResult(kind=ExecutionResultKind.TABLE, table=table)

    @staticmethod
    def from_reader(reader: RecordBatchReaderLike) -> ExecutionResult:
        """Wrap a record batch reader.

        Parameters
        ----------
        reader
            Record batch reader to wrap.

        Returns
        -------
        ExecutionResult
            Wrapped execution result.
        """
        return ExecutionResult(kind=ExecutionResultKind.READER, reader=reader)

    @staticmethod
    def from_write(result: WriteResult) -> ExecutionResult:
        """Wrap a write result.

        Parameters
        ----------
        result
            Write result to wrap.

        Returns
        -------
        ExecutionResult
            Wrapped execution result.
        """
        return ExecutionResult(kind=ExecutionResultKind.WRITE, write_result=result)

    def require_dataframe(self) -> DataFrame:
        """Return the DataFrame result or raise when missing.

        Returns
        -------
        datafusion.DataFrame
            DataFrame result for this execution.

        Raises
        ------
        ValueError
            Raised when the execution result is not a DataFrame.
        """
        if self.dataframe is None:
            msg = f"Execution result is not a dataframe: {self.kind}."
            raise ValueError(msg)
        return self.dataframe

    def require_table(self) -> TableLike:
        """Return the materialized table result or raise when missing.

        Returns
        -------
        TableLike
            Materialized table result for this execution.

        Raises
        ------
        ValueError
            Raised when the execution result is not a table.
        """
        if self.table is None:
            msg = f"Execution result is not a table: {self.kind}."
            raise ValueError(msg)
        return self.table

    def require_reader(self) -> RecordBatchReaderLike:
        """Return the record batch reader or raise when missing.

        Returns
        -------
        RecordBatchReaderLike
            Streaming reader for this execution.

        Raises
        ------
        ValueError
            Raised when the execution result is not a reader.
        """
        if self.reader is None:
            msg = f"Execution result is not a reader: {self.kind}."
            raise ValueError(msg)
        return self.reader

    def require_write(self) -> WriteResult:
        """Return the write result or raise when missing.

        Returns
        -------
        WriteResult
            Write result for this execution.

        Raises
        ------
        ValueError
            Raised when the execution result is not a write.
        """
        if self.write_result is None:
            msg = f"Execution result is not a write result: {self.kind}."
            raise ValueError(msg)
        return self.write_result


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
        from datafusion_engine.udf_platform import (
            RustUdfPlatformOptions,
            install_rust_udf_platform,
        )

        if self.runtime_profile is None:
            # Default configuration: enable all planner extensions
            options = RustUdfPlatformOptions(
                enable_udfs=True,
                enable_function_factory=True,
                enable_expr_planners=True,
                expr_planner_names=("codeanatomy_domain",),
                strict=True,
            )
            install_rust_udf_platform(self.ctx, options=options)
            return

        # Profile-driven configuration with strict validation
        options = RustUdfPlatformOptions(
            enable_udfs=self.runtime_profile.enable_udfs,
            enable_async_udfs=self.runtime_profile.enable_async_udfs,
            async_udf_timeout_ms=self.runtime_profile.async_udf_timeout_ms,
            async_udf_batch_size=self.runtime_profile.async_udf_batch_size,
            enable_function_factory=self.runtime_profile.enable_function_factory,
            enable_expr_planners=self.runtime_profile.enable_expr_planners,
            function_factory_hook=self.runtime_profile.function_factory_hook,
            expr_planner_hook=self.runtime_profile.expr_planner_hook,
            expr_planner_names=self.runtime_profile.expr_planner_names,
            strict=True,
        )
        install_rust_udf_platform(self.ctx, options=options)

    def io_adapter(self) -> DataFusionIOAdapter:
        """Return a DataFusionIOAdapter for the session context.

        Returns
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

        Returns
        -------
        DiagnosticsRecorder | None
            Recorder if a runtime profile is configured.
        """
        if self.runtime_profile is None:
            return None
        return recorder_for_profile(self.runtime_profile, operation_id=operation_id)

    def write_pipeline(self) -> WritePipeline:
        """Return a WritePipeline bound to this facade.

        Returns
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

        Returns
        -------
        SessionRuntime | None
            Session runtime when the profile matches the session context.
        """
        if self.runtime_profile is None:
            return None
        session_runtime = self.runtime_profile.session_runtime()
        if session_runtime.ctx is not self.ctx:
            return None
        return session_runtime

    def compile_to_bundle(
        self,
        builder: DataFrameBuilder,
        *,
        compute_execution_plan: bool = False,
    ) -> DataFusionPlanBundle:
        """Compile a DataFrame builder to a DataFusionPlanBundle.

        This is the canonical compilation path for DataFusion-native planning.

        Parameters
        ----------
        builder
            Callable that returns a DataFrame given a SessionContext.
        compute_execution_plan
            Whether to compute the physical execution plan (expensive).

        Returns
        -------
        DataFusionPlanBundle
            Canonical plan artifact for execution and scheduling.

        Examples
        --------
        >>> def build_query(ctx: SessionContext) -> DataFrame:
        ...     return ctx.sql("SELECT * FROM my_table")
        >>> bundle = facade.compile_to_bundle(build_query)
        """
        df = builder(self.ctx)
        session_runtime = self._session_runtime()
        return build_plan_bundle(
            self.ctx,
            df,
            compute_execution_plan=compute_execution_plan,
            compute_substrait=True,
            session_runtime=session_runtime,
        )

    def execute_plan_bundle(
        self,
        bundle: DataFusionPlanBundle,
        *,
        view_name: str | None = None,
        scan_units: Sequence[ScanUnit] = (),
        scan_keys: Sequence[str] = (),
    ) -> ExecutionResult:
        """Execute a plan bundle with Substrait-first replay.

        Substrait replay is the primary execution path for determinism. The
        original DataFrame is used as fallback when Substrait is unavailable
        or replay fails. Fallback events are recorded for diagnostics.

        Returns
        -------
        ExecutionResult
            Unified execution result for the plan bundle.

        Raises
        ------
        RuntimeError
            Raised when UDF compatibility checks fail.
        ValueError
            Raised when execution fails with invalid inputs.
        TypeError
            Raised when replay fails due to incompatible plan types.
        """
        start = time.perf_counter()
        try:
            _ensure_udf_compatibility(self.ctx, bundle)
            df, used_fallback = self._substrait_first_df(bundle)
            if used_fallback:
                self._record_substrait_fallback(
                    bundle,
                    view_name=view_name,
                    reason="substrait_replay_failed",
                )
            result = ExecutionResult.from_dataframe(df, plan_bundle=bundle)
        except (RuntimeError, ValueError, TypeError) as exc:
            duration_ms = (time.perf_counter() - start) * 1000.0
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
        duration_ms = (time.perf_counter() - start) * 1000.0
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
        return result

    def _record_execution_artifact(self, request: _ExecutionArtifactRequest) -> None:
        """Persist an execution artifact row when runtime settings allow it."""
        if self.runtime_profile is None or request.view_name is None:
            return
        from datafusion_engine.plan_artifact_store import (
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
            from datafusion_engine.diagnostics import record_artifact

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

        Substrait replay is the primary path for deterministic execution.
        Falls back to the original DataFrame when Substrait is unavailable.

        Returns
        -------
        tuple[DataFrame, bool]
            DataFrame and whether fallback was used.
        """
        if bundle.substrait_bytes is None:
            return bundle.df, True
        try:
            df = replay_substrait_bytes(self.ctx, bundle.substrait_bytes)
        except (RuntimeError, TypeError, ValueError):
            return bundle.df, True
        else:
            return df, False

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
            Reason for the fallback (e.g., 'substrait_unavailable', 'replay_failed').
        """
        if self.runtime_profile is None:
            return
        from datafusion_engine.diagnostics import record_artifact

        substrait_bytes = bundle.substrait_bytes
        has_substrait = substrait_bytes is not None
        record_artifact(
            self.runtime_profile,
            "substrait_fallback_v1",
            {
                "view_name": view_name,
                "plan_fingerprint": bundle.plan_fingerprint,
                "reason": reason,
                "has_substrait_bytes": has_substrait,
                "substrait_bytes_len": len(substrait_bytes) if substrait_bytes is not None else 0,
            },
        )

    def execute_builder(
        self,
        builder: DataFrameBuilder,
        *,
        compute_execution_plan: bool = False,
        compute_substrait: bool = True,
    ) -> ExecutionResult:
        """Execute a DataFrame builder and return a unified result.

        Parameters
        ----------
        builder
            Callable that returns a DataFrame given a SessionContext.
        compute_execution_plan
            Whether to compute the physical execution plan (expensive).
        compute_substrait
            Whether to compute Substrait bytes for fingerprinting.

        Returns
        -------
        ExecutionResult
            Unified execution result for the builder.
        """
        df = builder(self.ctx)
        session_runtime = self._session_runtime()
        bundle = build_plan_bundle(
            self.ctx,
            df,
            compute_execution_plan=compute_execution_plan,
            compute_substrait=compute_substrait,
            session_runtime=session_runtime,
        )
        return self.execute_plan_bundle(bundle)

    def execute_dataframe(
        self,
        df: DataFrame,
        *,
        compute_execution_plan: bool = False,
        compute_substrait: bool = True,
    ) -> ExecutionResult:
        """Wrap a DataFrame with a plan bundle for scheduling.

        Parameters
        ----------
        df
            DataFusion DataFrame to execute.
        compute_execution_plan
            Whether to compute the physical execution plan (expensive).
        compute_substrait
            Whether to compute Substrait bytes for fingerprinting.

        Returns
        -------
        ExecutionResult
            Unified execution result for the DataFrame.
        """
        session_runtime = self._session_runtime()
        bundle = build_plan_bundle(
            self.ctx,
            df,
            compute_execution_plan=compute_execution_plan,
            compute_substrait=compute_substrait,
            session_runtime=session_runtime,
        )
        return self.execute_plan_bundle(bundle)

    def write(
        self,
        request: WriteRequest,
    ) -> ExecutionResult:
        """Execute a write request using the write pipeline.

        Parameters
        ----------
        request
            Write request specification.

        Returns
        -------
        ExecutionResult
            Unified execution result for the write operation.
        """
        pipeline = self.write_pipeline()
        result = pipeline.write(request)
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

        Returns
        -------
        ExecutionResult
            Execution result wrapping the write metadata.
        """
        pipeline = self.write_pipeline()
        result = pipeline.write_view(request, prefer_streaming=prefer_streaming)
        return ExecutionResult.from_write(result)

    def ensure_view_graph(
        self,
        *,
        include_registry_views: bool = True,
    ) -> Mapping[str, object]:
        """Ensure the view graph is registered for the current context.

        Parameters
        ----------
        include_registry_views
            Whether to register view registry fragments prior to pipeline views.

        Returns
        -------
        Mapping[str, object]
            Rust UDF snapshot used for view registration.
        """
        from datafusion_engine.view_registry import ensure_view_graph

        return ensure_view_graph(
            self.ctx,
            runtime_profile=self.runtime_profile,
            include_registry_views=include_registry_views,
        )

    def register_dataset(
        self,
        *,
        name: str,
        location: DatasetLocation,
        cache_policy: DataFusionCachePolicy | None = None,
    ) -> DataFrame:
        """Register a dataset location via the registry bridge.

        Parameters
        ----------
        name
            Dataset name to register.
        location
            Dataset location metadata.
        cache_policy
            Optional cache policy to apply during registration.

        Returns
        -------
        DataFrame
            DataFusion DataFrame representing the registered dataset.
        """
        from datafusion_engine.registry_bridge import register_dataset_df

        return register_dataset_df(
            self.ctx,
            name=name,
            location=location,
            cache_policy=cache_policy,
            runtime_profile=self.runtime_profile,
        )

    def schema_introspector(self) -> SchemaIntrospector:
        """Return a SchemaIntrospector bound to the facade context.

        Returns
        -------
        SchemaIntrospector
            Introspector for information_schema queries.
        """
        from datafusion_engine.schema_introspection import SchemaIntrospector

        sql_options = (
            self.runtime_profile.sql_options() if self.runtime_profile is not None else None
        )
        return SchemaIntrospector(self.ctx, sql_options=sql_options)

    def build_plan_bundle(
        self,
        df: DataFrame,
        *,
        compute_execution_plan: bool = False,
        compute_substrait: bool = True,
    ) -> DataFusionPlanBundle:
        """Build a plan bundle from a DataFrame.

        This is the canonical way to capture plan artifacts for scheduling
        and lineage analysis.

        Parameters
        ----------
        df
            DataFusion DataFrame to build plan bundle from.
        compute_execution_plan
            Whether to compute the physical execution plan (expensive).
        compute_substrait
            Whether to compute Substrait bytes for fingerprinting.

        Returns
        -------
        DataFusionPlanBundle
            Canonical plan artifact for the DataFrame.
        """
        session_runtime = self._session_runtime()
        return build_plan_bundle(
            self.ctx,
            df,
            compute_execution_plan=compute_execution_plan,
            compute_substrait=compute_substrait,
            session_runtime=session_runtime,
        )


__all__ = [
    "DataFrameBuilder",
    "DataFusionExecutionFacade",
    "ExecutionResult",
    "ExecutionResultKind",
]
