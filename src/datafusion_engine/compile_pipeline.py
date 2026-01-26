"""Unified compilation pipeline for Ibis expressions and SQL.

This module provides a centralized compilation orchestration layer that
routes all expression types through a single deterministic pipeline:
Ibis → SQLGlot AST → Canonicalization → Execution.

Rendered SQL is treated as a diagnostics artifact only; execution is derived
from the canonical AST to keep policy enforcement and caching stable.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

import sqlglot.expressions as exp
from datafusion import DataFrame, SessionContext, SQLOptions

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable
    from ibis.expr.types import Value as IbisValue

    from datafusion_engine.param_binding import DataFusionParamBindings
    from datafusion_engine.sql_policy_engine import (
        CompilationArtifacts,
        SQLPolicyProfile,
    )
    from sqlglot_tools.bridge import IbisCompilerBackend
    from sqlglot_tools.compat import Expression
    from sqlglot_tools.optimizer import SchemaMapping



@dataclass(frozen=True)
class CompiledExpression:
    """Triple checkpoint: Ibis IR + SQLGlot AST + rendered SQL.

    This is the canonical compilation result that flows through
    the execution pipeline.

    Parameters
    ----------
    ibis_expr
        Original Ibis expression (None if compiled from raw SQL).
    sqlglot_ast
        Canonicalized SQLGlot AST after policy application.
    rendered_sql
        Final SQL string ready for execution.
    artifacts
        Compilation artifacts including fingerprint and lineage.
    """

    ibis_expr: IbisTable | None
    sqlglot_ast: exp.Expression
    rendered_sql: str
    artifacts: CompilationArtifacts
    source: Literal["ibis", "sql", "ast"]

    @property
    def fingerprint(self) -> str:
        """Cache key combining AST fingerprint and policy.

        Returns
        -------
        str
            AST fingerprint for cache keying.
        """
        return self.artifacts.ast_fingerprint

    @property
    def lineage(self) -> dict[str, set[tuple[str, str]]]:
        """Column-level lineage graph.

        Returns
        -------
        dict[str, set[tuple[str, str]]]
            Mapping of output_column -> {(source_table, source_column)}.
        """
        return self.artifacts.lineage_by_column


@dataclass(frozen=True)
class CompileOptions:
    """Options controlling compilation behavior.

    Parameters
    ----------
    prefer_substrait
        Prefer Substrait serialization when available (future use).
    prefer_ast_execution
        Use raw_sql(ast) when available for execution.
    profile
        SQL policy profile controlling canonicalization and optimization.
    schema
        Schema mapping for qualification. If None, will be introspected from context.
    """

    prefer_substrait: bool = False
    prefer_ast_execution: bool = True
    profile: SQLPolicyProfile | None = None
    schema: SchemaMapping | None = None
    enable_rewrites: bool = True
    rewrite_hook: Callable[[Expression], Expression] | None = None

    def __post_init__(self) -> None:
        """Initialize default profile if not provided."""
        if self.profile is None:
            from datafusion_engine.sql_policy_engine import SQLPolicyProfile

            # Frozen dataclass requires object.__setattr__
            object.__setattr__(self, "profile", SQLPolicyProfile())


class CompilationPipeline:
    """Unified compilation pipeline for all expression types.

    Supports Ibis expressions, raw SQL, and SQLGlot ASTs. All expressions
    flow through the same canonicalization and policy application pipeline.

    Parameters
    ----------
    ctx
        DataFusion SessionContext for execution and schema introspection.
    options
        Compilation options controlling behavior.
    """

    def __init__(
        self,
        ctx: SessionContext,
        options: CompileOptions,
    ) -> None:
        """Initialize compilation pipeline with context and options.

        Parameters
        ----------
        ctx
            DataFusion SessionContext for execution and schema introspection.
        options
            Compilation options controlling behavior.
        """
        self.ctx = ctx
        self.options = options
        self._schema_cache: SchemaMapping | None = None

    def compile_ibis(
        self,
        expr: IbisTable,
        *,
        backend: IbisCompilerBackend | None = None,
    ) -> CompiledExpression:
        """Compile Ibis expression through SQLGlot canonicalization.

        Parameters
        ----------
        expr
            Ibis table expression to compile.
        backend
            Optional Ibis backend. If None, will create from context.

        Returns
        -------
        CompiledExpression
            Compiled expression with AST, SQL, and artifacts.

        Raises
        ------
        ValueError
            Raised when the SQL policy profile is missing.
        """
        from ibis.backends.datafusion import Backend

        from datafusion_engine.sql_policy_engine import (
            compile_sql_policy,
            render_for_execution,
        )

        if backend is None:
            backend = cast("IbisCompilerBackend", Backend.from_connection(self.ctx))
        compiler_backend = backend

        # 1. Ibis → SQLGlot AST (documented internal API)
        raw_ast = compiler_backend.compiler.to_sqlglot(expr)
        if self.options.enable_rewrites and self.options.rewrite_hook is not None:
            raw_ast = self.options.rewrite_hook(raw_ast)

        # 2. Get schema from introspection or cache
        schema = self._get_schema()

        # 3. Apply policy canonicalization
        profile = self.options.profile
        if profile is None:
            msg = "SQL policy profile is required for compilation."
            raise ValueError(msg)
        canonical_ast, artifacts = compile_sql_policy(
            raw_ast,
            schema=schema,
            profile=profile,
        )

        # 4. Render for execution
        rendered = render_for_execution(canonical_ast, profile)

        return CompiledExpression(
            ibis_expr=expr,
            sqlglot_ast=canonical_ast,
            rendered_sql=rendered,
            artifacts=artifacts,
            source="ibis",
        )

    def compile_sql(
        self,
        sql: str,
    ) -> CompiledExpression:
        """Compile raw SQL through SQLGlot canonicalization.

        Parameters
        ----------
        sql
            Raw SQL string to compile.

        Returns
        -------
        CompiledExpression
            Compiled expression with AST, SQL, and artifacts.

        Raises
        ------
        ValueError
            Raised when the SQL policy profile is missing.
        """
        from datafusion_engine.sql_policy_engine import (
            compile_sql_policy,
            render_for_execution,
        )

        profile = self.options.profile
        if profile is None:
            msg = "SQL policy profile is required for compilation."
            raise ValueError(msg)

        from sqlglot_tools.optimizer import StrictParseOptions, parse_sql_strict

        raw_ast = parse_sql_strict(
            sql,
            dialect=profile.read_dialect,
            options=StrictParseOptions(
                error_level=profile.error_level,
                unsupported_level=profile.unsupported_level,
            ),
        )
        if self.options.enable_rewrites and self.options.rewrite_hook is not None:
            raw_ast = self.options.rewrite_hook(raw_ast)

        # Get schema
        schema = self._get_schema()

        # Canonicalize
        canonical_ast, artifacts = compile_sql_policy(
            raw_ast,
            schema=schema,
            profile=profile,
            original_sql=sql,
        )

        # Render
        rendered = render_for_execution(canonical_ast, profile)

        return CompiledExpression(
            ibis_expr=None,
            sqlglot_ast=canonical_ast,
            rendered_sql=rendered,
            artifacts=artifacts,
            source="sql",
        )

    def compile_ast(
        self,
        expr: exp.Expression,
        *,
        original_sql: str | None = None,
    ) -> CompiledExpression:
        """Compile SQLGlot AST through SQL policy canonicalization.

        Parameters
        ----------
        expr
            SQLGlot AST to compile.
        original_sql
            Optional original SQL string for error context.

        Returns
        -------
        CompiledExpression
            Compiled expression with AST, SQL, and artifacts.

        Raises
        ------
        ValueError
            Raised when the SQL policy profile is missing.
        """
        from datafusion_engine.sql_policy_engine import (
            compile_sql_policy,
            render_for_execution,
        )

        profile = self.options.profile
        if profile is None:
            msg = "SQL policy profile is required for compilation."
            raise ValueError(msg)

        raw_ast = expr
        if self.options.enable_rewrites and self.options.rewrite_hook is not None:
            raw_ast = self.options.rewrite_hook(raw_ast)

        schema = self._get_schema()
        canonical_ast, artifacts = compile_sql_policy(
            raw_ast,
            schema=schema,
            profile=profile,
            original_sql=original_sql,
        )
        rendered = render_for_execution(canonical_ast, profile)
        return CompiledExpression(
            ibis_expr=None,
            sqlglot_ast=canonical_ast,
            rendered_sql=rendered,
            artifacts=artifacts,
            source="ast",
        )

    def execute(
        self,
        compiled: CompiledExpression,
        *,
        params: Mapping[str, object] | Mapping[IbisValue, object] | None = None,
        named_params: Mapping[str, object] | None = None,
        sql_options: SQLOptions | None = None,
        **kwargs: object,
    ) -> DataFrame:
        """Execute compiled expression and return DataFrame.

        Parameters
        ----------
        compiled
            Compiled expression to execute.
        params
            Parameter bindings for execution (scalars and tables).
        named_params
            Table-like parameters to register for execution.
        sql_options
            Optional SQLOptions to use for execution.
        **kwargs
            Convenience keyword bindings (str keys). Mutually exclusive with params.

        Returns
        -------
        DataFrame
            DataFusion DataFrame result.

        Raises
        ------
        ValueError
            If both ``params`` and keyword arguments are provided.
        ValueError
            If SQL safety or statement option validation fails.
        TypeError
            If AST execution does not yield a DataFusion DataFrame.
        """
        if params is not None and kwargs:
            msg = "Pass either params mapping or keyword parameters, not both."
            raise ValueError(msg)
        bound_params = params if params is not None else kwargs
        _ = sql_options

        bindings = _resolve_bindings(bound_params, named_params=named_params)
        profile = _require_profile(self.options.profile)
        _validate_sql_execution(compiled, profile)
        exec_ast = _bind_exec_ast(compiled.sqlglot_ast, bindings)
        if _requires_statement_options(exec_ast):
            msg = "AST execution does not support statement options; use write pipeline."
            raise ValueError(msg)
        df = _execute_ast(self.ctx, exec_ast, bindings)
        if not isinstance(df, DataFrame):
            msg = "AST execution did not return a DataFusion DataFrame."
            raise TypeError(msg)
        return df

    def compile_and_execute(
        self,
        expr: IbisTable | str,
        *,
        params: Mapping[str, object] | Mapping[IbisValue, object] | None = None,
        named_params: Mapping[str, object] | None = None,
        sql_options: SQLOptions | None = None,
        **kwargs: object,
    ) -> DataFrame:
        """Compile and execute expression in single operation.

        Parameters
        ----------
        expr
            Ibis expression or SQL string to compile and execute.
        params
            Parameter bindings for execution.
        named_params
            Table-like parameters to register for execution.
        sql_options
            Optional SQLOptions to use for execution.
        **kwargs
            Convenience keyword bindings (str keys). Mutually exclusive with params.

        Returns
        -------
        DataFrame
            DataFusion DataFrame result.
        """
        compiled = self.compile_sql(expr) if isinstance(expr, str) else self.compile_ibis(expr)
        return self.execute(
            compiled,
            params=params,
            named_params=named_params,
            sql_options=sql_options,
            **kwargs,
        )

    def _get_schema(self) -> SchemaMapping:
        """Get schema from introspection or cache.

        Returns
        -------
        SchemaMapping
            SQLGlot-compatible schema mapping.
        """
        if self._schema_cache is not None:
            return self._schema_cache

        if self.options.schema:
            self._schema_cache = self.options.schema
        else:
            # Build from introspection
            self._schema_cache = build_schema_from_introspection(self.ctx)

        return self._schema_cache


def _requires_statement_options(expr: exp.Expression) -> bool:
    return (
        bool(expr.find(exp.Create))
        or bool(expr.find(exp.Drop))
        or bool(expr.find(exp.Alter))
        or bool(expr.find(exp.Insert))
        or bool(expr.find(exp.Update))
        or bool(expr.find(exp.Delete))
        or bool(expr.find(exp.Merge))
        or bool(expr.find(exp.Copy))
        or bool(expr.find(exp.Replace))
        or bool(expr.find(exp.Command))
    )


def _require_profile(profile: SQLPolicyProfile | None) -> SQLPolicyProfile:
    if profile is None:
        msg = "SQL policy profile is required for execution."
        raise ValueError(msg)
    return profile


def _resolve_bindings(
    params: Mapping[str, object] | Mapping[IbisValue, object] | None,
    *,
    named_params: Mapping[str, object] | None,
) -> DataFusionParamBindings:
    from datafusion_engine.param_binding import resolve_param_bindings

    bindings = resolve_param_bindings(params)
    if named_params is None:
        return bindings
    named_bindings = resolve_param_bindings(named_params)
    if named_bindings.param_values:
        msg = "Named parameters must be table-like."
        raise ValueError(msg)
    return bindings.merge(named_bindings)


def _validate_sql_execution(
    compiled: CompiledExpression,
    profile: SQLPolicyProfile,
) -> None:
    if compiled.source != "sql":
        return
    from datafusion_engine.sql_safety import (
        ExecutionContext,
        ExecutionPolicy,
        validate_sql_safety,
    )

    policy = ExecutionPolicy.for_context(ExecutionContext.QUERY_ONLY)
    violations = validate_sql_safety(
        compiled.rendered_sql,
        policy,
        dialect=profile.write_dialect,
    )
    if violations:
        msg = f"SQL policy violations: {'; '.join(violations)}"
        raise ValueError(msg)


def _bind_exec_ast(
    ast: exp.Expression,
    bindings: DataFusionParamBindings,
) -> exp.Expression:
    if not bindings.param_values:
        return ast
    from sqlglot_tools.optimizer import bind_params

    return bind_params(ast, params=bindings.param_values)


def _execute_ast(
    ctx: SessionContext,
    exec_ast: exp.Expression,
    bindings: DataFusionParamBindings,
) -> DataFrame:
    import ibis

    from datafusion_engine.param_binding import register_table_params

    backend = ibis.datafusion.connect(ctx)
    with register_table_params(ctx, bindings):
        return backend.raw_sql(exec_ast)


def build_schema_from_introspection(ctx: SessionContext) -> SchemaMapping:
    """Build SQLGlot-compatible schema mapping from DataFusion context.

    This function introspects the DataFusion SessionContext to extract
    all registered tables and their column schemas, returning a nested
    mapping structure compatible with SQLGlot's qualification system.

    Parameters
    ----------
    ctx
        DataFusion SessionContext to introspect.

    Returns
    -------
    SchemaMapping
        Nested schema mapping: {catalog: {schema: {table: {column: type}}}}.
    """
    from datafusion_engine.schema_introspection import (
        SchemaIntrospector,
        schema_map_for_sqlglot,
    )

    introspector = SchemaIntrospector(ctx, sql_options=None)
    return schema_map_for_sqlglot(introspector)
