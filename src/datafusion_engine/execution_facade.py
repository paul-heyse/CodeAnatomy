"""Unified execution facade for DataFusion compilation and execution."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import TYPE_CHECKING, cast

import sqlglot.expressions as exp
from datafusion import DataFrame, SessionContext

from datafusion_engine.compile_options import DataFusionCompileOptions, DataFusionSqlPolicy
from datafusion_engine.compile_pipeline import CompiledExpression
from datafusion_engine.diagnostics import DiagnosticsRecorder, recorder_for_profile
from datafusion_engine.execution_helpers import (
    _compilation_pipeline,
    _execution_policy_for_options,
    _maybe_collect_plan_artifacts,
    _maybe_collect_semantic_diff,
    _maybe_explain,
    _policy_violations,
    _resolve_cache_policy,
    _should_cache_df,
    _sql_options_for_named_params,
    _sql_options_for_options,
)
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.param_binding import resolve_param_bindings
from datafusion_engine.sql_safety import validate_sql_safety
from datafusion_engine.write_pipeline import (
    WritePipeline,
    WriteRequest,
    WriteResult,
    WriteViewRequest,
)
from sqlglot_tools.optimizer import sqlglot_policy_snapshot

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

    @staticmethod
    def from_dataframe(df: DataFrame) -> ExecutionResult:
        """Wrap a DataFusion DataFrame.

        Parameters
        ----------
        df
            DataFusion DataFrame to wrap.

        Returns
        -------
        ExecutionResult
            Wrapped execution result.
        """
        return ExecutionResult(kind=ExecutionResultKind.DATAFRAME, dataframe=df)

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
    """Facade coordinating compilation, execution, registration, and writes."""

    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile | None = None
    ibis_backend: IbisCompilerBackend | None = None

    def __post_init__(self) -> None:
        """Ensure the Rust UDF platform is installed for this context."""
        from datafusion_engine.udf_platform import (
            RustUdfPlatformOptions,
            install_rust_udf_platform,
        )

        if self.runtime_profile is None:
            options = RustUdfPlatformOptions(
                enable_udfs=True,
                enable_function_factory=True,
                enable_expr_planners=True,
                expr_planner_names=("codeanatomy_domain",),
                strict=True,
            )
            install_rust_udf_platform(self.ctx, options=options)
            return
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
        )

    def compile(
        self,
        expr: Expression | IbisTable | str,
        *,
        options: DataFusionCompileOptions | None = None,
    ) -> CompiledPlan:
        """Compile an expression into a canonicalized plan.

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
        recorder = self.diagnostics_recorder(operation_id="compile")
        if recorder is not None:
            snapshot = sqlglot_policy_snapshot()
            recorder.record_artifact(
                "sqlglot_tokenizer_mode_v1",
                {"tokenizer_mode": snapshot.tokenizer_mode},
            )
        pipeline = _compilation_pipeline(self.ctx, options=resolved)
        if isinstance(expr, str):
            if resolved.sql_ingest_hook is None:
                msg = (
                    "SQL string compilation is restricted to explicit SQL ingress. "
                    "Provide a SQLGlot AST/Ibis expression or configure sql_ingest_hook."
                )
                raise ValueError(msg)
            from sqlglot.errors import ParseError

            from sqlglot_tools.optimizer import (
                StrictParseOptions,
                parse_sql_strict,
                register_datafusion_dialect,
            )

            try:
                register_datafusion_dialect()
                ast = parse_sql_strict(
                    expr,
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
            compiled = pipeline.compile_ibis(
                cast("IbisTable", expr),
                backend=self._resolve_ibis_backend(),
            )
        return CompiledPlan(compiled=compiled, options=resolved)

    def execute(
        self,
        plan: CompiledPlan,
        *,
        params: Mapping[str, object] | Mapping[IbisValue, object] | None = None,
        named_params: Mapping[str, object] | None = None,
    ) -> ExecutionResult:
        """Execute a compiled plan and return a unified result.

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
            Raised when the SQL fails safety validation.
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
        if named_tables:
            read_only_policy = DataFusionSqlPolicy()
            violations = _policy_violations(plan.compiled.sqlglot_ast, read_only_policy)
            if violations:
                msg = f"Named parameter SQL must be read-only: {', '.join(violations)}."
                raise ValueError(msg)
        if plan.compiled.source == "sql":
            policy = _execution_policy_for_options(resolved)
            violations = validate_sql_safety(
                plan.compiled.rendered_sql,
                policy,
                dialect=resolved.dialect,
            )
            if violations:
                msg = f"SQL policy violations: {', '.join(violations)}."
                raise ValueError(msg)
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
        df = df.cache() if _should_cache_df(df, options=resolved, reason=cache_reason) else df
        return ExecutionResult.from_dataframe(df)

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

    def _resolve_ibis_backend(self) -> IbisCompilerBackend:
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
