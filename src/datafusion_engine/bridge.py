"""Bridge helpers for Ibis/SQLGlot execution in DataFusion."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, cast

from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value
from sqlglot import ErrorLevel, Expression, exp

from arrowdsl.core.context import Ordering, OrderingLevel
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.metadata import ordering_metadata_spec
from datafusion_engine.compile_options import (
    DataFusionCompileOptions,
    DataFusionFallbackEvent,
    DataFusionSqlPolicy,
)
from datafusion_engine.df_builder import TranslationError, df_from_sqlglot
from ibis_engine.params_bridge import datafusion_param_bindings
from ibis_engine.plan import IbisPlan
from sqlglot_tools.bridge import IbisCompilerBackend, ibis_to_sqlglot
from sqlglot_tools.optimizer import (
    bind_params,
    normalize_expr,
    register_datafusion_dialect,
    rewrite_expr,
)

logger = logging.getLogger(__name__)

try:
    from datafusion.substrait import Consumer as SubstraitConsumer
    from datafusion.substrait import Serde as SubstraitSerde
except ImportError:
    SubstraitConsumer = None
    SubstraitSerde = None

if TYPE_CHECKING:
    from datafusion.substrait import Consumer as SubstraitConsumerType
    from datafusion.substrait import Serde as SubstraitSerdeType


def _should_cache_df(df: DataFrame, *, options: DataFusionCompileOptions) -> bool:
    if options.cache is not True:
        return False
    if options.cache_max_columns is None:
        return True
    column_count = len(df.schema().names)
    return column_count <= options.cache_max_columns


def _default_sql_policy() -> DataFusionSqlPolicy:
    return DataFusionSqlPolicy()


def _ensure_dialect(name: str) -> None:
    if name == "datafusion_ext":
        register_datafusion_dialect()


def df_from_sqlglot_or_sql(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions | None = None,
) -> DataFrame:
    """Translate a SQLGlot expression into a DataFusion DataFrame.

    Falls back to SQL execution when direct translation is not supported.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame representing the expression.
    """
    resolved = options or DataFusionCompileOptions()
    bound_expr = expr
    if resolved.params:
        bound_expr = bind_params(
            expr,
            params=datafusion_param_bindings(resolved.params),
        )
    try:
        return df_from_sqlglot(ctx, bound_expr)
    except TranslationError as exc:
        _ensure_dialect(resolved.dialect)
        sql = bound_expr.sql(dialect=resolved.dialect, unsupported_level=ErrorLevel.RAISE)
        bindings = datafusion_param_bindings(resolved.params or {})
        policy = resolved.sql_policy or _default_sql_policy()
        sql_options = resolved.sql_options or policy.to_sql_options()
        violations = ()
        if resolved.sql_options is None or resolved.sql_policy is not None:
            violations = _policy_violations(bound_expr, policy)
            if violations:
                logger.warning(
                    "DataFusion SQL policy violations detected: %s",
                    ", ".join(violations),
                )
        event = DataFusionFallbackEvent(
            reason="translation_error",
            error=str(exc),
            expression_type=bound_expr.__class__.__name__,
            sql=sql,
            dialect=resolved.dialect,
            policy_violations=violations,
        )
        _emit_fallback_diagnostics(
            event,
            fallback_hook=resolved.fallback_hook,
        )
        return ctx.sql_with_options(sql, sql_options, param_values=bindings)


def sqlglot_to_datafusion(
    expr: Expression,
    *,
    ctx: SessionContext,
    options: DataFusionCompileOptions | None = None,
) -> DataFrame:
    """Translate a SQLGlot expression into a DataFusion DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame representing the expression.
    """
    options = options or DataFusionCompileOptions()
    rewritten = _apply_rewrite_hook(expr, options=options)
    return df_from_sqlglot_or_sql(
        ctx,
        rewritten,
        options=options,
    )


def ibis_to_datafusion(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    options: DataFusionCompileOptions | None = None,
) -> DataFrame:
    """Compile an Ibis expression into a DataFusion DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the Ibis expression.
    """
    options = options or DataFusionCompileOptions()
    sg_expr = ibis_to_sqlglot(expr, backend=backend, params=_ibis_params(options.params))
    sg_expr = _apply_rewrite_hook(sg_expr, options=options)
    if options.optimize:
        sg_expr = normalize_expr(sg_expr, schema=options.schema_map)
    df = df_from_sqlglot_or_sql(
        ctx,
        sg_expr,
        options=options,
    )
    return df.cache() if _should_cache_df(df, options=options) else df


def ibis_plan_to_datafusion(
    plan: IbisPlan,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    options: DataFusionCompileOptions | None = None,
) -> DataFrame:
    """Compile an Ibis plan into a DataFusion DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the plan.
    """
    return ibis_to_datafusion(plan.expr, backend=backend, ctx=ctx, options=options)


def datafusion_to_table(
    df: DataFrame,
    *,
    ordering: Ordering | None = None,
) -> TableLike:
    """Materialize a DataFusion DataFrame with optional ordering metadata.

    Returns
    -------
    TableLike
        Arrow table with ordering metadata applied when provided.
    """
    table = df.to_arrow_table()
    if ordering is None or ordering.level == OrderingLevel.UNORDERED:
        return table
    spec = ordering_metadata_spec(ordering.level, keys=ordering.keys)
    return table.cast(spec.apply(table.schema))


def ibis_plan_to_table(
    plan: IbisPlan,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    options: DataFusionCompileOptions | None = None,
) -> TableLike:
    """Compile an Ibis plan into a DataFusion-backed Arrow table.

    Returns
    -------
    TableLike
        Arrow table with ordering metadata applied when available.
    """
    df = ibis_plan_to_datafusion(plan, backend=backend, ctx=ctx, options=options)
    return datafusion_to_table(df, ordering=plan.ordering)


def _ibis_params(
    params: Mapping[str, object] | Mapping[Value, object] | None,
) -> Mapping[Value, object] | None:
    if not params:
        return None
    if all(isinstance(key, Value) for key in params):
        return cast("Mapping[Value, object]", params)
    return None


def _apply_rewrite_hook(expr: Expression, *, options: DataFusionCompileOptions) -> Expression:
    return rewrite_expr(
        expr,
        rewrite_hook=options.rewrite_hook,
        enabled=options.enable_rewrites,
    )


def _policy_violations(expr: Expression, policy: DataFusionSqlPolicy) -> tuple[str, ...]:
    violations: list[str] = []
    if not policy.allow_ddl and _contains_ddl(expr):
        violations.append("ddl")
    if not policy.allow_dml and _contains_dml(expr):
        violations.append("dml")
    if not policy.allow_statements and _contains_statements(expr):
        violations.append("statements")
    return tuple(violations)


def _contains_ddl(expr: Expression) -> bool:
    return (
        bool(expr.find(exp.Create))
        or bool(expr.find(exp.Drop))
        or bool(expr.find(exp.Alter))
    )


def _contains_dml(expr: Expression) -> bool:
    return (
        bool(expr.find(exp.Insert))
        or bool(expr.find(exp.Update))
        or bool(expr.find(exp.Delete))
        or bool(expr.find(exp.Merge))
        or bool(expr.find(exp.Copy))
        or bool(expr.find(exp.Replace))
    )


def _contains_statements(expr: Expression) -> bool:
    return (
        bool(expr.find(exp.Command))
        or bool(expr.find(exp.Set))
        or bool(expr.find(exp.Show))
        or bool(expr.find(exp.Describe))
        or bool(expr.find(exp.Use))
        or bool(expr.find(exp.Pragma))
    )


def _emit_fallback_diagnostics(
    event: DataFusionFallbackEvent,
    *,
    fallback_hook: Callable[[DataFusionFallbackEvent], None] | None,
) -> None:
    if fallback_hook is None:
        logger.info(
            "DataFusion fallback used for %s.",
            event.expression_type,
        )
        return
    fallback_hook(event)


def replay_substrait_bytes(ctx: SessionContext, plan_bytes: bytes) -> DataFrame:
    """Replay a Substrait plan into a DataFusion DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFrame constructed from the Substrait plan bytes.

    Raises
    ------
    RuntimeError
        Raised when the DataFusion Substrait helpers are unavailable.
    """
    if SubstraitConsumer is None or SubstraitSerde is None:
        msg = "DataFusion Substrait helpers are unavailable."
        raise RuntimeError(msg)
    consumer = cast("SubstraitConsumerType", SubstraitConsumer)
    serde = cast("SubstraitSerdeType", SubstraitSerde)
    plan = serde.deserialize_bytes(plan_bytes)
    logical_plan = consumer.from_substrait_plan(ctx, plan)
    return ctx.create_dataframe_from_logical_plan(logical_plan)


__all__ = [
    "DataFusionCompileOptions",
    "DataFusionFallbackEvent",
    "DataFusionSqlPolicy",
    "IbisCompilerBackend",
    "datafusion_to_table",
    "ibis_plan_to_datafusion",
    "ibis_plan_to_table",
    "ibis_to_datafusion",
    "replay_substrait_bytes",
    "sqlglot_to_datafusion",
]
