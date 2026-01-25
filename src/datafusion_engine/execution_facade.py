"""Unified execution facade for DataFusion compilation and execution."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import TYPE_CHECKING, cast

from datafusion import DataFrame, SessionContext
from sqlglot import exp  # type: ignore[attr-defined]

from datafusion_engine.bridge import (
    _compilation_pipeline,
    _execution_policy_for_options,
    _maybe_collect_plan_artifacts,
    _maybe_collect_semantic_diff,
    _maybe_explain,
    _resolve_cache_policy,
    _should_cache_df,
    _sql_options_for_options,
    df_from_sqlglot_or_sql,
    ibis_to_datafusion,
)
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.compile_pipeline import CompiledExpression
from datafusion_engine.diagnostics import DiagnosticsRecorder, recorder_for_profile
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.sql_safety import validate_sql_safety
from datafusion_engine.write_pipeline import WritePipeline, WriteRequest, WriteResult

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ibis.expr.types import Table as IbisTable
    from ibis.expr.types import Value as IbisValue

    from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.sql_policy_engine import SQLPolicyProfile
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

    def io_adapter(self) -> DataFusionIOAdapter:
        """Return a DataFusionIOAdapter for the session context.

        Returns
        -------
        DataFusionIOAdapter
            IO adapter bound to the DataFusion session context.
        """
        return DataFusionIOAdapter(ctx=self.ctx, profile=self.runtime_profile)

    def diagnostics_recorder(
        self, *, operation_id: str
    ) -> DiagnosticsRecorder | None:
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
            sql_options=self.runtime_profile.sql_options() if self.runtime_profile is not None else None,
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
            SQL text, SQLGlot expression, or Ibis table to compile.
        options
            Optional compile options to override defaults.

        Returns
        -------
        CompiledPlan
            Compiled plan and resolved compile options.
        """
        resolved = self._resolve_compile_options(options)
        pipeline = _compilation_pipeline(self.ctx, options=resolved)
        if isinstance(expr, str):
            compiled = pipeline.compile_sql(expr)
        elif isinstance(expr, exp.Expression):
            compiled = pipeline.compile_ast(cast("Expression", expr))
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
        resolved, cache_reason = _resolve_cache_policy(
            plan.compiled.sqlglot_ast,
            ctx=self.ctx,
            options=resolved,
        )
        pipeline = _compilation_pipeline(self.ctx, options=resolved)
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
            params=params,
            named_params=named_params,
            sql_options=_sql_options_for_options(resolved),
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
        resolved = self._resolve_compile_options(options)
        if isinstance(expr, exp.Expression):
            df = df_from_sqlglot_or_sql(self.ctx, cast("Expression", expr), options=resolved)
        else:
            df = ibis_to_datafusion(
                cast("IbisTable", expr),
                backend=self._resolve_ibis_backend(),
                ctx=self.ctx,
                options=resolved,
            )
        return ExecutionResult.from_dataframe(df)

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
