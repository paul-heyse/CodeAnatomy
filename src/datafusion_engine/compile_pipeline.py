"""Unified compilation pipeline for Ibis expressions and SQL.

This module provides a centralized compilation orchestration layer that
routes all expression types through a single deterministic pipeline:
Ibis → SQLGlot AST → Canonicalization → Rendered SQL → Execution.

The compilation pipeline produces checkpointed artifacts (AST fingerprint,
lineage metadata, rendered SQL) that enable caching, incremental compilation,
and diagnostic tracing.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import sqlglot.expressions as exp
from datafusion import DataFrame, SessionContext, SQLOptions

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable
    from ibis.expr.types import Value as IbisValue

    from datafusion_engine.sql_policy_engine import (
        CompilationArtifacts,
        SQLPolicyProfile,
    )
    from sqlglot_tools.bridge import IbisCompilerBackend
    from sqlglot_tools.compat import Expression
    from sqlglot_tools.optimizer import SchemaMapping

from datafusion_engine.compile_options import DataFusionSqlPolicy


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
        # Type narrowing: profile is always non-None after __post_init__
        assert self.options.profile is not None
        canonical_ast, artifacts = compile_sql_policy(
            raw_ast,
            schema=schema,
            profile=self.options.profile,
        )

        # 4. Render for execution
        rendered = render_for_execution(canonical_ast, self.options.profile)

        return CompiledExpression(
            ibis_expr=expr,
            sqlglot_ast=canonical_ast,
            rendered_sql=rendered,
            artifacts=artifacts,
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
        """
        from datafusion_engine.sql_policy_engine import (
            compile_sql_policy,
            render_for_execution,
        )

        # Type narrowing: profile is always non-None after __post_init__
        assert self.options.profile is not None

        from sqlglot_tools.optimizer import StrictParseOptions, parse_sql_strict

        raw_ast = parse_sql_strict(
            sql,
            dialect=self.options.profile.read_dialect,
            options=StrictParseOptions(
                error_level=self.options.profile.error_level,
                unsupported_level=self.options.profile.unsupported_level,
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
            profile=self.options.profile,
            original_sql=sql,
        )

        # Render
        rendered = render_for_execution(canonical_ast, self.options.profile)

        return CompiledExpression(
            ibis_expr=None,
            sqlglot_ast=canonical_ast,
            rendered_sql=rendered,
            artifacts=artifacts,
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
        """
        from datafusion_engine.sql_policy_engine import (
            compile_sql_policy,
            render_for_execution,
        )

        # Type narrowing: profile is always non-None after __post_init__
        assert self.options.profile is not None

        raw_ast = expr
        if self.options.enable_rewrites and self.options.rewrite_hook is not None:
            raw_ast = self.options.rewrite_hook(raw_ast)

        schema = self._get_schema()
        canonical_ast, artifacts = compile_sql_policy(
            raw_ast,
            schema=schema,
            profile=self.options.profile,
            original_sql=original_sql,
        )
        rendered = render_for_execution(canonical_ast, self.options.profile)
        return CompiledExpression(
            ibis_expr=None,
            sqlglot_ast=canonical_ast,
            rendered_sql=rendered,
            artifacts=artifacts,
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
        """
        from datafusion_engine.param_binding import (
            register_table_params,
            resolve_param_bindings,
        )

        if params is not None and kwargs:
            msg = "Pass either params mapping or keyword parameters, not both."
            raise ValueError(msg)
        bound_params = params if params is not None else kwargs

        # Resolve parameters
        bindings = resolve_param_bindings(bound_params)
        if named_params is not None:
            named_bindings = resolve_param_bindings(named_params)
            if named_bindings.param_values:
                msg = "Named parameters must be table-like."
                raise ValueError(msg)
            bindings = bindings.merge(named_bindings)

        resolved_options = sql_options or DataFusionSqlPolicy().to_sql_options()

        # Register table params with cleanup
        from datafusion_engine.sql_safety import (
            ExecutionContext,
            ExecutionPolicy,
            validate_sql_safety,
        )

        profile = self.options.profile
        assert profile is not None

        policy = ExecutionPolicy.for_context(ExecutionContext.QUERY_ONLY)
        violations = validate_sql_safety(
            compiled.rendered_sql,
            policy,
            dialect=profile.write_dialect,
        )
        if violations:
            msg = f"SQL policy violations: {'; '.join(violations)}"
            raise ValueError(msg)
        param_values = dict(bindings.param_values) if bindings.param_values else None
        with register_table_params(self.ctx, bindings):
            return self.ctx.sql_with_options(
                compiled.rendered_sql,
                resolved_options,
                param_values=param_values,
            )

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
