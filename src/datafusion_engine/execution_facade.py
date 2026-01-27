"""Unified execution facade for DataFusion compilation and execution.

SQL ingress is gated by DataFusion SQLOptions. Internal execution paths
use builder/plan-based approaches only. SQLGlot policy and safety validation
are deprecated for internal paths.

DEPRECATION NOTICE: Ibis compilation paths are deprecated. Prefer DataFusion-native
builder functions returning DataFrame directly. See datafusion_engine.sqlglot_exprs
for recommended DataFrame construction patterns.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import TYPE_CHECKING, cast

import sqlglot.expressions as exp
from datafusion import DataFrame, SessionContext

from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.compile_pipeline import CompiledExpression
from datafusion_engine.diagnostics import DiagnosticsRecorder, recorder_for_profile
from datafusion_engine.execution_helpers import (
    _compilation_pipeline,
    _maybe_collect_plan_artifacts,
    _maybe_collect_semantic_diff,
    _maybe_explain,
    _resolve_cache_policy,
    _should_cache_df,
    _sql_options_for_named_params,
    _sql_options_for_options,
)
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.param_binding import resolve_param_bindings
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
    from collections.abc import Mapping

    from ibis.expr.types import Table as IbisTable
    from ibis.expr.types import Value as IbisValue

    from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
    from datafusion_engine.registry_bridge import DataFusionCachePolicy
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.schema_introspection import SchemaIntrospector
    from datafusion_engine.sql_policy_engine import SQLPolicyProfile
    from ibis_engine.registry import DatasetLocation
    from sqlglot_tools.bridge import IbisCompilerBackend
    from sqlglot_tools.compat import Expression


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
class CompiledPlan:
    """Bundle a compiled expression with compile options."""

    compiled: CompiledExpression
    options: DataFusionCompileOptions


@dataclass(frozen=True)
class DataFusionExecutionFacade:
    """Facade coordinating compilation, execution, registration, and writes.

    DEPRECATION NOTICE: The ibis_backend field is deprecated and will be removed
    in a future version. Use DataFusion-native builder functions instead of Ibis
    compilation paths.
    """

    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile | None = None
    ibis_backend: IbisCompilerBackend | None = None  # DEPRECATED: Ibis compilation is deprecated

    def __post_init__(self) -> None:
        """Ensure planner extensions are installed before any plan operations.

        Planner extensions (Rust UDFs, ExprPlanner, FunctionFactory) are
        planning-critical features and must be installed before any
        plan-bundle construction.

        DEPRECATION WARNING: Emits warning when ibis_backend is provided.
        """
        import warnings

        from datafusion_engine.udf_platform import (
            RustUdfPlatformOptions,
            install_rust_udf_platform,
        )

        if self.ibis_backend is not None:
            warnings.warn(
                "ibis_backend parameter is deprecated and will be removed in a future version. "
                "Use DataFusion-native builder functions instead of Ibis compilation paths. "
                "See datafusion_engine.sqlglot_exprs for recommended patterns.",
                DeprecationWarning,
                stacklevel=2,
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

    def write_pipeline(
        self,
        *,
        profile: SQLPolicyProfile,
    ) -> WritePipeline:
        """Return a WritePipeline bound to this facade.

        Parameters
        ----------
        profile
            SQL policy profile for write execution.

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
            profile=profile,
            sql_options=self.runtime_profile.sql_options()
            if self.runtime_profile is not None
            else None,
            recorder=recorder,
            runtime_profile=self.runtime_profile,
        )

    def compile(
        self,
        expr: Expression | IbisTable | str,
        *,
        options: DataFusionCompileOptions | None = None,
    ) -> CompiledPlan:
        """Compile an expression into a canonicalized plan.

        DEPRECATED: This method is deprecated. Use compile_to_bundle() for
        DataFusion-native planning or use builder functions that return
        DataFrame directly.

        SQLGlot/Ibis compilation paths are deprecated. Prefer DataFusion-native
        builder functions returning DataFrame directly.

        SQL string ingress is gated by SQLOptions and only accepted when
        sql_ingest_hook is configured to mark explicit external SQL ingress.

        Parameters
        ----------
        expr
            SQLGlot expression or Ibis table to compile. SQL text is only accepted
            when sql_ingest_hook is configured to mark explicit SQL ingress.
        options
            Optional compile options to override defaults.

        Returns
        -------
        CompiledPlan
            Compiled plan and resolved compile options.

        Raises
        ------
        ValueError
            Raised when SQL ingestion is not explicitly enabled.
        """
        resolved = self._resolve_compile_options(options)
        # DEPRECATED: SQLGlot policy snapshot recording removed for DataFusion-native planning
        # Use DataFusionPlanBundle for plan artifacts instead
        pipeline = _compilation_pipeline(self.ctx, options=resolved)
        if isinstance(expr, str):
            # SQL string ingress: strict gating via sql_ingest_hook
            if resolved.sql_ingest_hook is None:
                msg = (
                    "SQL string compilation is restricted to explicit SQL ingress. "
                    "Provide a SQLGlot AST/Ibis expression or configure sql_ingest_hook."
                )
                raise ValueError(msg)
            from sqlglot.errors import ParseError

            from datafusion_engine.sql_safety import sanitize_external_sql
            from sqlglot_tools.optimizer import (
                StrictParseOptions,
                parse_sql_strict,  # DEPRECATED: Use DataFusion's native SQL parser for validation
                register_datafusion_dialect,
            )

            # DEPRECATED: SQLGlot-based SQL validation for DataFusion queries.
            # For DataFusion query ingress, prefer DataFusion's native SQL parser
            # with SQLOptions gating.
            try:
                sanitized = sanitize_external_sql(expr)
                register_datafusion_dialect()
                ast = parse_sql_strict(
                    sanitized,
                    dialect=resolved.dialect,
                    options=StrictParseOptions(preserve_params=True),
                )
            except (ParseError, TypeError, ValueError) as exc:
                msg = "SQL ingress parse failed."
                raise ValueError(msg) from exc
            compiled = pipeline.compile_ast(ast, original_sql=expr)
        elif isinstance(expr, exp.Expression):
            compiled = pipeline.compile_ast(expr)
        else:
            # DEPRECATED: Ibis compilation path. Use DataFrame builder functions instead.
            # See datafusion_engine.sqlglot_exprs for recommended patterns.
            compiled = pipeline.compile_ibis(
                cast("IbisTable", expr),
                backend=self._resolve_ibis_backend(),
            )
        return CompiledPlan(compiled=compiled, options=resolved)

    def compile_to_bundle(
        self,
        builder: Callable[[SessionContext], DataFrame],
        *,
        compute_execution_plan: bool = False,
    ) -> DataFusionPlanBundle:
        """Compile a DataFrame builder to a DataFusionPlanBundle.

        This is the preferred compilation path for DataFusion-native planning.
        Use this method instead of compile() for new code.

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
        return build_plan_bundle(
            self.ctx,
            df,
            compute_execution_plan=compute_execution_plan,
            compute_substrait=True,
        )

    def execute(
        self,
        plan: CompiledPlan,
        *,
        params: Mapping[str, object] | Mapping[IbisValue, object] | None = None,
        named_params: Mapping[str, object] | None = None,
    ) -> ExecutionResult:
        """Execute a compiled plan and return a unified result.

        Internal execution uses DataFusion-native planning with SQLOptions gating.
        SQLGlot policy validation is deprecated for internal paths.

        Parameters
        ----------
        plan
            Compiled plan to execute.
        params
            Positional parameter bindings for the plan.
        named_params
            Named parameter bindings for the plan.

        Returns
        -------
        ExecutionResult
            Unified execution result for the compiled plan.

        Raises
        ------
        ValueError
            Raised when parameter bindings are invalid.
        """
        resolved = plan.options
        params = params if params is not None else resolved.params
        named_params = named_params if named_params is not None else resolved.named_params
        resolved, cache_reason = _resolve_cache_policy(
            plan.compiled.sqlglot_ast,
            ctx=self.ctx,
            options=resolved,
        )
        pipeline = _compilation_pipeline(self.ctx, options=resolved)
        bindings = resolve_param_bindings(
            params,
            allowlist=resolved.param_identifier_allowlist,
        )
        if bindings.named_tables:
            msg = "Table-like parameters must be passed via named params."
            raise ValueError(msg)
        named_bindings = resolve_param_bindings(
            named_params,
            allowlist=resolved.param_identifier_allowlist,
        )
        if named_bindings.param_values:
            msg = "Named parameters must be table-like."
            raise ValueError(msg)
        named_tables = named_bindings.named_tables
        # Internal execution: SQLOptions gating only, no SQLGlot policy validation
        df = pipeline.execute(
            plan.compiled,
            params=bindings.param_values or None,
            named_params=named_tables or None,
            sql_options=(
                _sql_options_for_named_params(resolved)
                if named_tables
                else _sql_options_for_options(resolved)
            ),
        )
        _maybe_explain(self.ctx, plan.compiled.sqlglot_ast, options=resolved)
        _maybe_collect_plan_artifacts(self.ctx, plan.compiled.sqlglot_ast, options=resolved, df=df)
        _maybe_collect_semantic_diff(self.ctx, plan.compiled.sqlglot_ast, options=resolved)

        # Build plan bundle for DataFusion-native lineage tracking
        bundle = self.build_plan_bundle(df, compute_substrait=True)

        # Prefer DataFusion plan bundle fingerprint over legacy AST fingerprint
        # Legacy AST fingerprinting is deprecated (lines 375-379)
        fingerprint = bundle.plan_fingerprint
        # Use empty policy hash for DataFusion-native plans
        policy_hash = ""

        df = (
            df.cache()
            if _should_cache_df(
                df,
                options=resolved,
                reason=cache_reason,
                ast_fingerprint=fingerprint,
                policy_hash=policy_hash,
            )
            else df
        )
        return ExecutionResult.from_dataframe(df, plan_bundle=bundle)

    def execute_expr(
        self,
        expr: Expression | IbisTable,
        *,
        options: DataFusionCompileOptions | None = None,
    ) -> ExecutionResult:
        """Compile and execute an expression in one step.

        Parameters
        ----------
        expr
            SQLGlot expression or Ibis table to execute.
        options
            Optional compile options to override defaults.

        Returns
        -------
        ExecutionResult
            Unified execution result for the expression.
        """
        plan = self.compile(expr, options=options)
        return self.execute(plan)

    def write(
        self,
        request: WriteRequest,
        *,
        profile: SQLPolicyProfile,
    ) -> ExecutionResult:
        """Execute a write request using the write pipeline.

        Parameters
        ----------
        request
            Write request specification.
        profile
            SQL policy profile for the write operation.

        Returns
        -------
        ExecutionResult
            Unified execution result for the write operation.
        """
        pipeline = self.write_pipeline(profile=profile)
        result = pipeline.write(request)
        return ExecutionResult.from_write(result)

    def write_view(
        self,
        request: WriteViewRequest,
        *,
        profile: SQLPolicyProfile,
        prefer_streaming: bool = True,
    ) -> ExecutionResult:
        """Write a registered view using the write pipeline.

        Parameters
        ----------
        request
            Write request specifying the registered view.
        profile
            SQL policy profile for the write operation.
        prefer_streaming
            Prefer streaming writes when possible.

        Returns
        -------
        ExecutionResult
            Execution result wrapping the write metadata.
        """
        pipeline = self.write_pipeline(profile=profile)
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
        return build_plan_bundle(
            self.ctx,
            df,
            compute_execution_plan=compute_execution_plan,
            compute_substrait=compute_substrait,
        )

    def _resolve_ibis_backend(self) -> IbisCompilerBackend:
        """Resolve Ibis backend for legacy compilation paths (DEPRECATED).

        DEPRECATED: This method supports deprecated Ibis compilation. Use DataFusion-native
        builder functions instead.

        Returns
        -------
        IbisCompilerBackend
            Resolved Ibis compiler backend.

        Raises
        ------
        ValueError
            Raised when ibis_backend is None.
        """
        if self.ibis_backend is None:
            msg = "Ibis backend required for Ibis compilation."
            raise ValueError(msg)
        return self.ibis_backend

    def _resolve_compile_options(
        self,
        options: DataFusionCompileOptions | None,
    ) -> DataFusionCompileOptions:
        if self.runtime_profile is None:
            return options or DataFusionCompileOptions()
        resolved = self.runtime_profile.compile_options(options=options)
        if resolved.runtime_profile is None:
            resolved = replace(resolved, runtime_profile=self.runtime_profile)
        return resolved


__all__ = [
    "CompiledPlan",
    "DataFusionExecutionFacade",
    "ExecutionResult",
    "ExecutionResultKind",
]
