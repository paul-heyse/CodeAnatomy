"""Bridge helpers for Ibis/SQLGlot execution in DataFusion."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from ibis.expr.types import Table as IbisTable
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
from engine.plan_cache import PlanCacheEntry, PlanCacheKey
from ibis_engine.params_bridge import datafusion_param_bindings
from ibis_engine.plan import IbisPlan
from sqlglot_tools.bridge import IbisCompilerBackend, ibis_to_sqlglot
from sqlglot_tools.optimizer import (
    normalize_expr,
    plan_fingerprint,
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


def _plan_cache_key(
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> PlanCacheKey | None:
    if options.plan_cache is None or options.profile_hash is None:
        return None
    if options.params is not None:
        return None
    plan_hash = options.plan_hash or plan_fingerprint(expr, dialect=options.dialect)
    return PlanCacheKey(plan_hash=plan_hash, profile_hash=options.profile_hash)


def _substrait_bytes(ctx: SessionContext, sql: str) -> bytes | None:
    if SubstraitSerde is None:
        return None
    serde = cast("SubstraitSerdeType", SubstraitSerde)
    try:
        return serde.serialize_bytes(sql, ctx)
    except (RuntimeError, TypeError, ValueError):  # pragma: no cover - defensive around FFI errors.
        return None


def _try_replay_substrait(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> DataFrame | None:
    cache_key = _plan_cache_key(expr, options=options)
    if cache_key is None or options.plan_cache is None:
        return None
    cached = options.plan_cache.get(cache_key)
    if cached is None:
        return None
    try:
        return replay_substrait_bytes(ctx, cached)
    except (
        RuntimeError,
        TypeError,
        ValueError,
    ) as exc:  # pragma: no cover - defensive around FFI errors.
        logger.warning("Substrait replay failed; recompiling. error=%s", exc)
        return None


def _maybe_store_substrait(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> None:
    cache_key = _plan_cache_key(expr, options=options)
    if cache_key is None or options.plan_cache is None:
        return
    if options.plan_cache.contains(cache_key):
        return
    _ensure_dialect(options.dialect)
    sql = expr.sql(dialect=options.dialect, unsupported_level=ErrorLevel.RAISE)
    plan_bytes = _substrait_bytes(ctx, sql)
    if plan_bytes is None:
        return
    entry = PlanCacheEntry(
        plan_hash=cache_key.plan_hash,
        profile_hash=cache_key.profile_hash,
        plan_bytes=plan_bytes,
    )
    options.plan_cache.put(entry)


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
    replayed = _try_replay_substrait(ctx, bound_expr, options=resolved)
    if replayed is not None:
        if not (resolved.params and _contains_params(expr)):
            _maybe_explain(ctx, bound_expr, options=resolved)
        return replayed
    if resolved.force_sql:
        df = _df_from_sql(
            ctx,
            bound_expr,
            options=resolved,
            fallback=SqlFallbackContext(reason="forced_sql", emit_fallback=False),
        )
        _maybe_store_substrait(ctx, bound_expr, options=resolved)
        if not (resolved.params and _contains_params(expr)):
            _maybe_explain(ctx, bound_expr, options=resolved)
        return df
    if resolved.params and _contains_params(expr):
        return _df_from_sql(
            ctx,
            bound_expr,
            options=resolved,
            fallback=SqlFallbackContext(reason="params"),
        )
    try:
        df = df_from_sqlglot(ctx, bound_expr)
    except TranslationError as exc:
        _ensure_dialect(resolved.dialect)
        df = _df_from_sql(
            ctx,
            bound_expr,
            options=resolved,
            fallback=SqlFallbackContext(reason="translation_error", error=exc),
        )
    _maybe_store_substrait(ctx, bound_expr, options=resolved)
    _maybe_explain(ctx, bound_expr, options=resolved)
    return df


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


def compile_sqlglot_expr(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    options: DataFusionCompileOptions | None = None,
) -> Expression:
    """Compile an Ibis expression into a SQLGlot expression.

    Parameters
    ----------
    expr:
        Ibis expression to compile.
    backend:
        Backend providing SQLGlot compilation support.
    options:
        Optional compilation options controlling rewrites and normalization.

    Returns
    -------
    sqlglot.Expression
        SQLGlot expression for the Ibis input.
    """
    resolved = options or DataFusionCompileOptions()
    sg_expr = ibis_to_sqlglot(expr, backend=backend, params=None)
    sg_expr = _apply_rewrite_hook(sg_expr, options=resolved)
    if resolved.optimize:
        sg_expr = normalize_expr(sg_expr, schema=resolved.schema_map)
    return sg_expr


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
    resolved = options or DataFusionCompileOptions()
    sg_expr = compile_sqlglot_expr(expr, backend=backend, options=resolved)
    df = df_from_sqlglot_or_sql(
        ctx,
        sg_expr,
        options=resolved,
    )
    return df.cache() if _should_cache_df(df, options=resolved) else df


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


def datafusion_to_reader(
    df: DataFrame,
    *,
    ordering: Ordering | None = None,
) -> pa.RecordBatchReader:
    """Return a RecordBatchReader for a DataFusion DataFrame.

    Returns
    -------
    pyarrow.RecordBatchReader
        Record batch reader for the DataFusion results.
    """
    try:
        reader = pa.RecordBatchReader.from_stream(df)
        return _apply_ordering(reader, ordering=ordering)
    except (TypeError, ValueError):
        pass
    stream = getattr(df, "execute_stream", None)
    if callable(stream):
        reader = stream()
        return _apply_ordering(reader, ordering=ordering)
    to_batches = getattr(df, "to_arrow_batches", None)
    if callable(to_batches):
        batch_iter = cast("Iterable[pa.RecordBatch]", to_batches())
        batches = list(batch_iter)
        reader = pa.RecordBatchReader.from_batches(df.schema(), batches)
        return _apply_ordering(reader, ordering=ordering)
    collect = getattr(df, "collect", None)
    if callable(collect):
        batch_iter = cast("Iterable[pa.RecordBatch]", collect())
        batches = list(batch_iter)
        reader = pa.RecordBatchReader.from_batches(df.schema(), batches)
        return _apply_ordering(reader, ordering=ordering)
    reader = pa.RecordBatchReader.from_batches(df.schema(), [])
    return _apply_ordering(reader, ordering=ordering)


def datafusion_partitioned_readers(df: DataFrame) -> list[pa.RecordBatchReader]:
    """Return partitioned stream readers when supported by DataFusion.

    Returns
    -------
    list[pyarrow.RecordBatchReader]
        Partitioned readers when supported, otherwise an empty list.
    """
    stream_partitions = getattr(df, "execute_stream_partitioned", None)
    if not callable(stream_partitions):
        return []
    readers = stream_partitions()
    if not isinstance(readers, Iterable):
        return []
    return [cast("pa.RecordBatchReader", reader) for reader in readers]


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


def ibis_plan_to_reader(
    plan: IbisPlan,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    options: DataFusionCompileOptions | None = None,
) -> pa.RecordBatchReader:
    """Return a DataFusion-backed RecordBatchReader for an Ibis plan.

    Returns
    -------
    pyarrow.RecordBatchReader
        Record batch reader for the plan results.
    """
    df = ibis_plan_to_datafusion(plan, backend=backend, ctx=ctx, options=options)
    return datafusion_to_reader(df, ordering=plan.ordering)


def _apply_ordering(
    reader: pa.RecordBatchReader,
    *,
    ordering: Ordering | None,
) -> pa.RecordBatchReader:
    if ordering is None or ordering.level == OrderingLevel.UNORDERED:
        return reader
    spec = ordering_metadata_spec(ordering.level, keys=ordering.keys)
    return pa.RecordBatchReader.from_batches(spec.apply(reader.schema), reader)


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
    return bool(expr.find(exp.Create)) or bool(expr.find(exp.Drop)) or bool(expr.find(exp.Alter))


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


def _contains_params(expr: Expression) -> bool:
    return bool(expr.find(exp.Parameter))


@dataclass(frozen=True)
class SqlFallbackContext:
    """Parameters controlling SQL fallback diagnostics."""

    reason: str
    error: TranslationError | None = None
    emit_fallback: bool = True


def _df_from_sql(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
    fallback: SqlFallbackContext,
) -> DataFrame:
    _ensure_dialect(options.dialect)
    sql = expr.sql(dialect=options.dialect, unsupported_level=ErrorLevel.RAISE)
    bindings = datafusion_param_bindings(options.params or {})
    policy = options.sql_policy or _default_sql_policy()
    sql_options = options.sql_options or policy.to_sql_options()
    violations = ()
    if options.sql_options is None or options.sql_policy is not None:
        violations = _policy_violations(expr, policy)
        if violations:
            logger.warning(
                "DataFusion SQL policy violations detected: %s",
                ", ".join(violations),
            )
    if fallback.emit_fallback:
        event = DataFusionFallbackEvent(
            reason=fallback.reason,
            error=str(fallback.error) if fallback.error is not None else "",
            expression_type=expr.__class__.__name__,
            sql=sql,
            dialect=options.dialect,
            policy_violations=violations,
        )
        _emit_fallback_diagnostics(
            event,
            fallback_hook=options.fallback_hook,
        )
    return ctx.sql_with_options(sql, sql_options, param_values=bindings)


def df_from_sql(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
    fallback: SqlFallbackContext,
) -> DataFrame:
    """Execute a SQLGlot expression using DataFusion SQL execution.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame representing the expression.
    """
    return _df_from_sql(ctx, expr, options=options, fallback=fallback)


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


def _maybe_explain(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> None:
    if not options.capture_explain and options.explain_hook is None:
        return
    _ensure_dialect(options.dialect)
    sql = expr.sql(dialect=options.dialect, unsupported_level=ErrorLevel.RAISE)
    prefix = "EXPLAIN ANALYZE" if options.explain_analyze else "EXPLAIN"
    explain_df = ctx.sql(f"{prefix} {sql}")
    rows = _explain_rows(explain_df)
    if options.explain_hook is not None:
        options.explain_hook(sql, rows)


def _explain_rows(df: DataFrame) -> list[Mapping[str, object]]:
    to_arrow = getattr(df, "to_arrow_table", None)
    if callable(to_arrow):
        table = cast("pa.Table", to_arrow())
        return [dict(row) for row in table.to_pylist()]
    collect = getattr(df, "collect", None)
    if callable(collect):
        rows = cast("Iterable[Mapping[str, object]]", collect())
        return [dict(row) for row in rows]
    return []


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
    "SqlFallbackContext",
    "compile_sqlglot_expr",
    "datafusion_partitioned_readers",
    "datafusion_to_reader",
    "datafusion_to_table",
    "df_from_sql",
    "ibis_plan_to_datafusion",
    "ibis_plan_to_reader",
    "ibis_plan_to_table",
    "ibis_to_datafusion",
    "replay_substrait_bytes",
    "sqlglot_to_datafusion",
]
