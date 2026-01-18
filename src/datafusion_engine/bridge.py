"""Bridge helpers for Ibis/SQLGlot execution in DataFusion."""

from __future__ import annotations

import base64
import hashlib
import logging
import re
from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

import datafusion
import pyarrow as pa
import pyarrow.ipc as pa_ipc

try:
    import pyarrow.substrait as pa_substrait
except ImportError:
    pa_substrait = None
try:
    from datafusion.unparser import Unparser as DataFusionUnparser
    from datafusion.unparser import Dialect as DataFusionDialect
    from datafusion.unparser import LogicalPlan as DataFusionLogicalPlan
except ImportError:
    DataFusionUnparser = None
    DataFusionDialect = None
    DataFusionLogicalPlan = None
from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value as IbisValue
from sqlglot import ErrorLevel, Expression, exp

from arrowdsl.core.context import Ordering, OrderingLevel
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike, coerce_table_like
from arrowdsl.core.streaming import to_reader
from arrowdsl.schema.metadata import ordering_metadata_spec
from datafusion_engine.compile_options import (
    DataFusionCacheEvent,
    DataFusionCompileOptions,
    DataFusionDmlOptions,
    DataFusionFallbackEvent,
    DataFusionSqlPolicy,
    resolve_sql_policy,
)
from datafusion_engine.df_builder import TranslationError, df_from_sqlglot
from engine.plan_cache import PlanCacheEntry, PlanCacheKey
from ibis_engine.params_bridge import datafusion_param_bindings
from ibis_engine.plan import IbisPlan
from obs.diagnostics import PreparedStatementSpec
from sqlglot_tools.bridge import IbisCompilerBackend, ibis_to_sqlglot
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    default_sqlglot_policy,
    normalize_expr,
    parse_sql_strict,
    plan_fingerprint,
    register_datafusion_dialect,
    rewrite_expr,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PreparedStatementOptions:
    """Options for preparing DataFusion SQL statements."""

    param_types: Sequence[str]
    sql_options: DataFusionSqlPolicy | None = None
    record_hook: Callable[[PreparedStatementSpec], None] | None = None


@dataclass(frozen=True)
class CopyToOptions:
    """Options for COPY TO format precedence and DML execution."""

    session_defaults: Mapping[str, object] | None = None
    table_options: Mapping[str, object] | None = None
    statement_overrides: Mapping[str, object] | None = None
    dml: DataFusionDmlOptions | None = None


@dataclass(frozen=True)
class ArrowIngestEvent:
    """Arrow ingest event payload for diagnostics."""

    name: str
    method: str
    partitioning: str | None
    batch_size: int | None
    batch_count: int | None
    row_count: int | None


try:
    from datafusion.substrait import Consumer as SubstraitConsumer
    from datafusion.substrait import Serde as SubstraitSerde
except ImportError:
    SubstraitConsumer = None
    SubstraitSerde = None

if TYPE_CHECKING:
    from datafusion.substrait import Consumer as SubstraitConsumerType
    from datafusion.substrait import Serde as SubstraitSerdeType


def _emit_cache_event(
    *,
    options: DataFusionCompileOptions,
    column_count: int,
    cache_enabled: bool,
    reason: str,
) -> None:
    hook = options.cache_event_hook
    if hook is None:
        return
    hook(
        DataFusionCacheEvent(
            cache_enabled=cache_enabled,
            cache_max_columns=options.cache_max_columns,
            column_count=column_count,
            reason=reason,
            plan_hash=options.plan_hash,
            profile_hash=options.profile_hash,
        )
    )


def _should_cache_df(
    df: DataFrame,
    *,
    options: DataFusionCompileOptions,
    reason: str | None = None,
) -> bool:
    if options.cache is not True:
        _emit_cache_event(
            options=options,
            column_count=len(df.schema().names),
            cache_enabled=False,
            reason="cache_disabled",
        )
        return False
    column_count = len(df.schema().names)
    if options.cache_max_columns is not None and column_count > options.cache_max_columns:
        suffix = reason or "max_columns_exceeded"
        _emit_cache_event(
            options=options,
            column_count=column_count,
            cache_enabled=False,
            reason=f"{suffix}_max_columns_exceeded",
        )
        return False
    _emit_cache_event(
        options=options,
        column_count=column_count,
        cache_enabled=True,
        reason=reason or "cache_enabled",
    )
    return True


def _resolve_cache_policy(
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> tuple[DataFusionCompileOptions, str | None]:
    if options.cache is not None:
        return options, "cache_configured"
    cache_key = _plan_cache_key(expr, options=options)
    if cache_key is None or options.plan_cache is None:
        return options, None
    if options.plan_cache.contains(cache_key):
        return replace(options, cache=True), "plan_cache_hit"
    return options, None


def _plan_cache_key(
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> PlanCacheKey | None:
    if options.plan_cache is None or options.profile_hash is None:
        return None
    if options.params is not None:
        return None
    policy_hash = options.sqlglot_policy_hash
    plan_hash = options.plan_hash or plan_fingerprint(
        expr,
        dialect=options.dialect,
        policy_hash=policy_hash,
    )
    return PlanCacheKey(plan_hash=plan_hash, profile_hash=options.profile_hash)


def _substrait_bytes(ctx: SessionContext, sql: str) -> bytes | None:
    if SubstraitSerde is None:
        return None
    serde = cast("SubstraitSerdeType", SubstraitSerde)
    try:
        return serde.serialize_bytes(sql, ctx)
    except (RuntimeError, TypeError, ValueError):  # pragma: no cover - defensive around FFI errors.
        return None


def _reader_from_table_like(value: TableLike | RecordBatchReaderLike) -> pa.RecordBatchReader:
    return to_reader(value)


def _fingerprint_reader(reader: pa.RecordBatchReader) -> tuple[str, int]:
    sink = pa.BufferOutputStream()
    row_count = 0
    with pa_ipc.new_stream(sink, reader.schema) as writer:
        for batch in reader:
            row_count += batch.num_rows
            writer.write_batch(batch)
    payload = sink.getvalue().to_pybytes()
    return hashlib.sha256(payload).hexdigest(), row_count


def _fingerprint_table(value: TableLike | RecordBatchReaderLike) -> tuple[str, int]:
    reader = _reader_from_table_like(value)
    return _fingerprint_reader(reader)


def _substrait_validation_payload(
    plan_bytes: bytes,
    *,
    df: DataFrame,
) -> Mapping[str, object]:
    if pa_substrait is None:
        return {
            "status": "unavailable",
            "error": "pyarrow.substrait is unavailable",
        }
    try:
        df_reader = datafusion_to_reader(df)
    except (RuntimeError, TypeError, ValueError) as exc:
        return {
            "status": "error",
            "stage": "datafusion",
            "error": str(exc),
        }
    try:
        substrait_result = pa_substrait.run_query(plan_bytes, use_threads=True)
    except (RuntimeError, TypeError, ValueError) as exc:
        return {
            "status": "error",
            "stage": "pyarrow_substrait",
            "error": str(exc),
        }
    df_hash, df_rows = _fingerprint_table(df_reader)
    substrait_hash, substrait_rows = _fingerprint_table(
        cast("TableLike | RecordBatchReaderLike", substrait_result)
    )
    match = df_rows == substrait_rows and df_hash == substrait_hash
    return {
        "status": "match" if match else "mismatch",
        "match": match,
        "datafusion_rows": df_rows,
        "datafusion_hash": df_hash,
        "substrait_rows": substrait_rows,
        "substrait_hash": substrait_hash,
    }


def validate_substrait_plan(plan_bytes: bytes, *, df: DataFrame) -> Mapping[str, object]:
    """Validate a Substrait plan by comparing PyArrow and DataFusion outputs.

    Returns
    -------
    Mapping[str, object]
        Validation payload containing hashes, row counts, and status fields.
    """
    return _substrait_validation_payload(plan_bytes, df=df)


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


def _merge_sql_policies(*policies: DataFusionSqlPolicy | None) -> DataFusionSqlPolicy:
    allow_ddl = any(policy.allow_ddl for policy in policies if policy is not None)
    allow_dml = any(policy.allow_dml for policy in policies if policy is not None)
    allow_statements = any(policy.allow_statements for policy in policies if policy is not None)
    return DataFusionSqlPolicy(
        allow_ddl=allow_ddl,
        allow_dml=allow_dml,
        allow_statements=allow_statements,
    )


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
        _maybe_collect_plan_artifacts(ctx, bound_expr, options=resolved, df=replayed)
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
        _maybe_collect_plan_artifacts(ctx, bound_expr, options=resolved, df=df)
        return df
    if resolved.params and _contains_params(expr):
        df = _df_from_sql(
            ctx,
            bound_expr,
            options=resolved,
            fallback=SqlFallbackContext(reason="params"),
        )
        _maybe_collect_plan_artifacts(ctx, bound_expr, options=resolved, df=df)
        return df
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
    _maybe_collect_plan_artifacts(ctx, bound_expr, options=resolved, df=df)
    return df


def validate_table_constraints(
    ctx: SessionContext,
    *,
    name: str,
    table: pa.Table,
    constraints: Sequence[str],
) -> list[str]:
    """Return constraint expressions violated by the table.

    Returns
    -------
    list[str]
        Constraint expressions with at least one violating row.
    """
    if not constraints:
        return []
    ctx.register_record_batches(name, [table.to_batches()])
    violations: list[str] = []
    try:
        for constraint in constraints:
            if not constraint.strip():
                continue
            df = ctx.sql(f"SELECT 1 FROM {name} WHERE NOT ({constraint}) LIMIT 1")
            if df.to_arrow_table().num_rows > 0:
                violations.append(constraint)
    finally:
        ctx.deregister_table(name)
    return violations


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
    policy = resolved.sqlglot_policy or default_sqlglot_policy()
    if resolved.dialect:
        policy = replace(policy, write_dialect=resolved.dialect)
    sg_expr = ibis_to_sqlglot(
        expr,
        backend=backend,
        params=_ibis_param_bindings(resolved.params),
    )
    sg_expr = _apply_rewrite_hook(sg_expr, options=resolved)
    if resolved.optimize:
        sg_expr = normalize_expr(
            sg_expr,
            options=NormalizeExprOptions(
                schema=resolved.schema_map,
                policy=policy,
            ),
        )
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
    resolved, cache_reason = _resolve_cache_policy(sg_expr, options=resolved)
    df = df_from_sqlglot_or_sql(
        ctx,
        sg_expr,
        options=resolved,
    )
    return df.cache() if _should_cache_df(df, options=resolved, reason=cache_reason) else df


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


async def datafusion_to_async_batches(
    df: DataFrame,
    *,
    ordering: Ordering | None = None,
) -> AsyncIterator[pa.RecordBatch]:
    """Yield RecordBatches asynchronously from a DataFusion DataFrame.

    Yields
    ------
    pyarrow.RecordBatch
        Record batches from the DataFusion result.
    """
    async_iter = getattr(df, "__aiter__", None)
    if callable(async_iter):
        async for batch in df:
            yield _to_record_batch(batch)
        return
    reader = datafusion_to_reader(df, ordering=ordering)
    for batch in reader:
        yield batch


def datafusion_view_sql(df: DataFrame) -> str | None:
    """Return SQL text for a DataFusion DataFrame when available.

    Returns
    -------
    str | None
        SQL string when the DataFrame can emit it, otherwise ``None``.
    """
    to_sql = getattr(df, "to_sql", None)
    if callable(to_sql):
        try:
            return str(to_sql())
        except (RuntimeError, TypeError, ValueError):
            return None
    sql_attr = getattr(df, "sql", None)
    if isinstance(sql_attr, str):
        return sql_attr
    return None


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


def _to_record_batch(value: object) -> pa.RecordBatch:
    if isinstance(value, pa.RecordBatch):
        return value
    to_pyarrow = getattr(value, "to_pyarrow", None)
    if callable(to_pyarrow):
        converted = to_pyarrow()
        if isinstance(converted, pa.RecordBatch):
            return converted
        if isinstance(converted, pa.Table):
            table = cast("pa.Table", converted)
            batches = list(table.to_batches())
            if batches:
                return batches[0]
            empty_arrays = [pa.array([], type=field.type) for field in table.schema]
            return pa.RecordBatch.from_arrays(empty_arrays, schema=table.schema)
    msg = "Async DataFusion batch does not expose a pyarrow.RecordBatch."
    raise TypeError(msg)


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


def _ibis_param_bindings(
    values: Mapping[str, object] | Mapping[IbisValue, object] | None,
) -> Mapping[IbisValue, object] | None:
    if not values:
        return None
    keys = list(values.keys())
    if all(isinstance(key, IbisValue) for key in keys):
        return cast("Mapping[IbisValue, object]", values)
    return None


@dataclass(frozen=True)
class SqlFallbackContext:
    """Parameters controlling SQL fallback diagnostics."""

    reason: str
    error: TranslationError | None = None
    emit_fallback: bool = True


@dataclass(frozen=True)
class DataFusionPlanArtifacts:
    """Captured DataFusion plan artifacts for diagnostics."""

    plan_hash: str | None
    sql: str
    explain: TableLike | RecordBatchReaderLike
    explain_analyze: TableLike | RecordBatchReaderLike | None
    substrait_plan: bytes | None
    substrait_validation: Mapping[str, object] | None = None
    unparsed_sql: str | None = None
    unparse_error: str | None = None
    logical_plan: str | None = None
    optimized_plan: str | None = None
    physical_plan: str | None = None
    graphviz: str | None = None
    partition_count: int | None = None
    join_operators: tuple[str, ...] | None = None

    def payload(self) -> dict[str, object]:
        """Return a payload for plan artifacts.

        Returns
        -------
        dict[str, object]
            Payload for plan artifacts.
        """
        substrait_b64 = (
            base64.b64encode(self.substrait_plan).decode("utf-8")
            if self.substrait_plan is not None
            else None
        )
        return {
            "plan_hash": self.plan_hash,
            "sql": self.sql,
            "explain": self.explain,
            "explain_analyze": self.explain_analyze,
            "substrait_b64": substrait_b64,
            "substrait_validation": (
                dict(self.substrait_validation) if self.substrait_validation is not None else None
            ),
            "unparsed_sql": self.unparsed_sql,
            "unparse_error": self.unparse_error,
            "logical_plan": self.logical_plan,
            "optimized_plan": self.optimized_plan,
            "physical_plan": self.physical_plan,
            "graphviz": self.graphviz,
            "partition_count": self.partition_count,
            "join_operators": list(self.join_operators)
            if self.join_operators is not None
            else None,
        }


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


def execute_dml(
    ctx: SessionContext,
    sql: str,
    *,
    options: DataFusionDmlOptions | None = None,
) -> DataFrame:
    """Execute a DML SQL statement using DataFusion.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame representing the statement.

    Raises
    ------
    ValueError
        Raised when the SQL statement is not DML or violates policy.
    """
    resolved = options or DataFusionDmlOptions()
    _ensure_dialect(resolved.dialect)
    expr = parse_sql_strict(sql, dialect=resolved.dialect)
    if not _contains_dml(expr):
        msg = "DML execution requires a DML statement."
        raise ValueError(msg)
    sql_policy = resolved.sql_policy or resolve_sql_policy(
        resolved.sql_policy_name,
        fallback=_default_sql_policy(),
    )
    policy = _merge_sql_policies(
        DataFusionSqlPolicy(allow_dml=True),
        resolved.session_policy,
        resolved.table_policy,
        sql_policy,
    )
    sql_options = resolved.sql_options or policy.to_sql_options()
    violations = _policy_violations(expr, policy)
    if violations:
        msg = f"DataFusion DML policy violations: {', '.join(violations)}."
        raise ValueError(msg)
    bindings = datafusion_param_bindings(resolved.params or {})
    if bindings:
        try:
            df = ctx.sql_with_options(sql, sql_options, param_values=bindings)
        except TypeError as exc:
            msg = "DataFusion does not support param_values for DML execution."
            raise ValueError(msg) from exc
    else:
        df = ctx.sql_with_options(sql, sql_options)
    if resolved.record_hook is not None:
        resolved.record_hook(
            {
                "sql": sql,
                "dialect": resolved.dialect,
                "policy_violations": list(violations),
            }
        )
    return df


def merge_format_options(
    *,
    session_defaults: Mapping[str, object] | None = None,
    table_options: Mapping[str, object] | None = None,
    statement_overrides: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Merge format options using session < table < statement precedence.

    Returns
    -------
    dict[str, object]
        Merged format options with deterministic precedence.
    """
    merged: dict[str, object] = {}
    for options in (session_defaults, table_options, statement_overrides):
        if not options:
            continue
        merged.update({key: value for key, value in options.items() if value is not None})
    return merged


def copy_to_path(
    ctx: SessionContext,
    *,
    sql: str,
    path: str,
    options: CopyToOptions | None = None,
) -> DataFrame:
    """Execute COPY TO with deterministic options precedence.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFrame representing the COPY statement.
    """
    resolved = options or CopyToOptions()
    merged = merge_format_options(
        session_defaults=resolved.session_defaults,
        table_options=resolved.table_options,
        statement_overrides=resolved.statement_overrides,
    )
    clause = _copy_options_clause(merged)
    statement = f"COPY ({sql}) TO {_sql_literal(path)}{clause}"
    return execute_dml(ctx, statement, options=resolved.dml)


def prepare_statement(
    ctx: SessionContext,
    *,
    name: str,
    sql: str,
    options: PreparedStatementOptions,
) -> PreparedStatementSpec:
    """Prepare a SQL statement for reuse with EXECUTE.

    Parameters
    ----------
    ctx:
        DataFusion session context.
    name:
        Prepared statement name.
    sql:
        SQL text containing positional parameters.
    options:
        Prepared statement options, including parameter types and policy overrides.

    Returns
    -------
    PreparedStatementSpec
        Prepared statement metadata for diagnostics.
    """
    policy = _merge_sql_policies(DataFusionSqlPolicy(allow_statements=True), options.sql_options)
    opts = policy.to_sql_options()
    spec = PreparedStatementSpec(
        name=name,
        sql=sql,
        param_types=tuple(options.param_types),
    )
    prepare_sql = f"PREPARE {name}({', '.join(spec.param_types)}) AS {sql}"
    ctx.sql_with_options(prepare_sql, opts)
    if options.record_hook is not None:
        options.record_hook(spec)
    return spec


def execute_prepared_statement(
    ctx: SessionContext,
    *,
    name: str,
    params: Sequence[object],
    sql_options: DataFusionSqlPolicy | None = None,
) -> DataFrame:
    """Execute a prepared statement with literal parameters.

    Parameters
    ----------
    ctx:
        DataFusion session context.
    name:
        Prepared statement name.
    params:
        Parameter values for EXECUTE.
    sql_options:
        Optional SQL policy overrides for statement execution.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFrame for the prepared statement execution.
    """
    policy = _merge_sql_policies(DataFusionSqlPolicy(allow_statements=True), sql_options)
    opts = policy.to_sql_options()
    arg_sql = ", ".join(_sql_literal(param) for param in params)
    return ctx.sql_with_options(f"EXECUTE {name}({arg_sql})", opts)


def _sql_literal(value: object) -> str:
    """Return a SQL literal for the provided value.

    Returns
    -------
    str
        SQL literal representation of the value.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _copy_options_clause(options: Mapping[str, object]) -> str:
    """Return a COPY OPTIONS clause for provided key/value pairs.

    Returns
    -------
    str
        SQL OPTIONS clause or empty string.
    """
    if not options:
        return ""
    formatted: list[str] = []
    for key, value in options.items():
        literal = _option_literal(value)
        if literal is None:
            continue
        formatted.append(f"{_sql_literal(str(key))} {literal}")
    if not formatted:
        return ""
    return f" OPTIONS ({', '.join(formatted)})"


def _option_literal(value: object) -> str | None:
    """Return a SQL literal for supported option values.

    Returns
    -------
    str | None
        SQL literal or None when the value is unsupported.
    """
    if isinstance(value, bool):
        return _sql_literal("true" if value else "false")
    if isinstance(value, (int, float)):
        return _sql_literal(str(value))
    if isinstance(value, str):
        return _sql_literal(value)
    return None


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
    rows = _explain_reader(explain_df)
    if options.explain_hook is not None:
        options.explain_hook(sql, rows)


def _maybe_collect_plan_artifacts(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
    df: DataFrame,
) -> None:
    if (
        not options.capture_plan_artifacts
        and options.plan_artifacts_hook is None
        and not options.substrait_validation
    ):
        return
    if options.params and _contains_params(expr):
        return
    try:
        artifacts = collect_plan_artifacts(ctx, expr, options=options, df=df)
    except (RuntimeError, TypeError, ValueError):
        return
    if options.plan_artifacts_hook is not None:
        options.plan_artifacts_hook(artifacts.payload())


def _explain_reader(df: DataFrame) -> pa.RecordBatchReader:
    return datafusion_to_reader(df)


def _collect_reader(df: DataFrame) -> pa.RecordBatchReader:
    return datafusion_to_reader(df)


def _df_plan_details(df: DataFrame) -> dict[str, object]:
    logical = _plan_display(_df_plan(df, "logical_plan"), display_method="display_indent_schema")
    optimized = _plan_display(
        _df_plan(df, "optimized_logical_plan"), display_method="display_indent_schema"
    )
    physical = _plan_display(_df_plan(df, "execution_plan"), display_method="display_indent")
    if physical is None:
        physical = _plan_display(_df_plan(df, "physical_plan"), display_method="display_indent")
    graphviz = _plan_graphviz(_df_plan(df, "optimized_logical_plan"))
    partition_count = _plan_partition_count(
        _df_plan(df, "execution_plan") or _df_plan(df, "physical_plan")
    )
    join_operators = _join_operator_evidence(physical)
    return {
        "logical_plan": logical,
        "optimized_plan": optimized,
        "physical_plan": physical,
        "graphviz": graphviz,
        "partition_count": partition_count,
        "join_operators": join_operators,
    }


def _unparse_plan(plan: object | None) -> tuple[str | None, str | None]:
    if plan is None:
        return None, "missing_plan"
    if DataFusionUnparser is None or DataFusionDialect is None or DataFusionLogicalPlan is None:
        return None, "unparser_unavailable"
    try:
        unparser = DataFusionUnparser(DataFusionDialect.default())
        plan_obj = cast("DataFusionLogicalPlan", plan)
        return str(unparser.plan_to_sql(plan_obj)), None
    except (RuntimeError, TypeError, ValueError) as exc:
        return None, str(exc)


def _df_plan(df: DataFrame, method_name: str) -> object | None:
    method = getattr(df, method_name, None)
    if not callable(method):
        return None
    try:
        return method()
    except (RuntimeError, TypeError, ValueError):
        return None


def _plan_display(plan: object | None, *, display_method: str) -> str | None:
    if plan is None:
        return None
    if isinstance(plan, str):
        return plan
    method = getattr(plan, display_method, None)
    if callable(method):
        try:
            return str(method())
        except (RuntimeError, TypeError, ValueError):
            return None
    return str(plan)


def _plan_graphviz(plan: object | None) -> str | None:
    if plan is None:
        return None
    method = getattr(plan, "display_graphviz", None)
    if not callable(method):
        return None
    try:
        return str(method())
    except (RuntimeError, TypeError, ValueError):
        return None


def _plan_partition_count(plan: object | None) -> int | None:
    if plan is None:
        return None
    count = getattr(plan, "partition_count", None)
    if count is None:
        return None
    if isinstance(count, bool):
        return None
    if isinstance(count, (int, float)):
        return int(count)
    try:
        return int(count)
    except (TypeError, ValueError):
        return None


def _join_operator_evidence(plan: str | None) -> tuple[str, ...] | None:
    if not plan:
        return None
    matches = re.findall(r"\b([A-Za-z]+Join)\b", plan)
    if not matches:
        return None
    seen: set[str] = set()
    operators: list[str] = []
    for op in matches:
        if op in seen:
            continue
        seen.add(op)
        operators.append(op)
    return tuple(operators)


def collect_plan_artifacts(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
    df: DataFrame | None = None,
) -> DataFusionPlanArtifacts:
    """Collect plan artifacts for diagnostics.

    Returns
    -------
    DataFusionPlanArtifacts
        Plan artifacts derived from EXPLAIN and Substrait serialization.
    """
    _ensure_dialect(options.dialect)
    sql = expr.sql(dialect=options.dialect, unsupported_level=ErrorLevel.RAISE)
    plan_df = df
    if plan_df is None:
        plan_df = ctx.sql(sql)
    explain_rows = _collect_reader(ctx.sql(f"EXPLAIN {sql}"))
    analyze_rows = None
    if options.explain_analyze:
        analyze_rows = _collect_reader(ctx.sql(f"EXPLAIN ANALYZE {sql}"))
    substrait_plan = _substrait_bytes(ctx, sql)
    substrait_validation: Mapping[str, object] | None = None
    if substrait_plan is not None and options.substrait_validation and plan_df is not None:
        substrait_validation = _substrait_validation_payload(
            substrait_plan,
            df=plan_df,
        )
    plan_details = _df_plan_details(plan_df)
    unparse_plan = _df_plan(plan_df, "optimized_logical_plan") or _df_plan(
        plan_df,
        "logical_plan",
    )
    unparsed_sql, unparse_error = _unparse_plan(unparse_plan)
    policy_hash = options.sqlglot_policy_hash
    plan_hash = options.plan_hash or plan_fingerprint(
        expr,
        dialect=options.dialect,
        policy_hash=policy_hash,
    )
    return DataFusionPlanArtifacts(
        plan_hash=plan_hash,
        sql=sql,
        explain=explain_rows,
        explain_analyze=analyze_rows,
        substrait_plan=substrait_plan,
        substrait_validation=substrait_validation,
        unparsed_sql=unparsed_sql,
        unparse_error=unparse_error,
        logical_plan=cast("str | None", plan_details.get("logical_plan")),
        optimized_plan=cast("str | None", plan_details.get("optimized_plan")),
        physical_plan=cast("str | None", plan_details.get("physical_plan")),
        graphviz=cast("str | None", plan_details.get("graphviz")),
        partition_count=cast("int | None", plan_details.get("partition_count")),
        join_operators=cast("tuple[str, ...] | None", plan_details.get("join_operators")),
    )


def execute_sql(
    ctx: SessionContext,
    *,
    sql: str,
    options: DataFusionCompileOptions,
) -> pa.RecordBatchReader:
    """Execute SQL text with DataFusion and return a record batch reader.

    Returns
    -------
    pyarrow.RecordBatchReader
        Record batch reader over the SQL results.
    """
    _ensure_dialect(options.dialect)
    bindings = datafusion_param_bindings(options.params or {})
    policy = options.sql_policy or _default_sql_policy()
    sql_options = options.sql_options or policy.to_sql_options()
    df = ctx.sql_with_options(sql, sql_options, param_values=bindings)
    return _collect_reader(df)


def register_memtable(
    ctx: SessionContext,
    *,
    name: str,
    batches: Sequence[pa.RecordBatch],
) -> None:
    """Register a MemTable with DataFusion using best-available APIs.

    Raises
    ------
    RuntimeError
        Raised when MemTable registration is unavailable.
    """
    register_batches = getattr(ctx, "register_record_batches", None)
    if callable(register_batches):
        register_batches(name, list(batches))
        return
    memtable_cls = getattr(datafusion, "MemTable", None)
    if memtable_cls is None:
        msg = "DataFusion MemTable registration is unavailable."
        raise RuntimeError(msg)
    table = memtable_cls.from_batches(list(batches))
    register_table = getattr(ctx, "register_table", None)
    if callable(register_table):
        register_table(name, table)
        return
    msg = "DataFusion SessionContext missing table registration methods."
    raise RuntimeError(msg)


def slice_memtable_batches(
    table: TableLike | RecordBatchReaderLike,
    *,
    batch_size: int | None,
) -> list[pa.RecordBatch]:
    """Slice a table into batches for MemTable registration.

    Parameters
    ----------
    table:
        Arrow table or record batch reader.
    batch_size:
        Optional max batch size for slicing.

    Returns
    -------
    list[pyarrow.RecordBatch]
        Record batches sized for registration.
    """
    if isinstance(table, RecordBatchReaderLike):
        resolved_table = cast("pa.Table", table.read_all())
    else:
        resolved_table = cast("pa.Table", table)
    if batch_size is None or batch_size <= 0:
        return list(resolved_table.to_batches())
    return list(resolved_table.to_batches(max_chunksize=batch_size))


def register_memtable_from_table(
    ctx: SessionContext,
    *,
    name: str,
    table: TableLike | RecordBatchReaderLike,
    batch_size: int | None = None,
) -> None:
    """Register a MemTable from a table-like input."""
    if isinstance(table, RecordBatchReaderLike):
        register_memtable_from_reader(ctx, name=name, reader=table, batch_size=batch_size)
        return
    batches = slice_memtable_batches(table, batch_size=batch_size)
    register_memtable(ctx, name=name, batches=batches)


def register_memtable_from_reader(
    ctx: SessionContext,
    *,
    name: str,
    reader: RecordBatchReaderLike,
    batch_size: int | None = None,
) -> None:
    """Register a MemTable from a record batch reader when supported."""
    from_arrow = getattr(ctx, "from_arrow", None)
    if callable(from_arrow) and (batch_size is None or batch_size <= 0):
        try:
            from_arrow(reader, name=name)
        except (TypeError, ValueError):
            pass
        else:
            return
    batches = slice_memtable_batches(reader, batch_size=batch_size)
    register_memtable(ctx, name=name, batches=batches)


def _is_pydict_input(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    if not value:
        return True
    return all(isinstance(key, str) for key in value)


def _is_pylist_input(value: object) -> bool:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return False
    if not value:
        return True
    sample = value[0]
    return isinstance(sample, Mapping)


def _emit_arrow_ingest(
    ingest_hook: Callable[[Mapping[str, object]], None] | None,
    event: ArrowIngestEvent,
) -> None:
    if ingest_hook is None:
        return
    ingest_hook(
        {
            "name": event.name,
            "method": event.method,
            "partitioning": event.partitioning,
            "batch_size": event.batch_size,
            "batch_count": event.batch_count,
            "row_count": event.row_count,
        }
    )


def datafusion_from_arrow(
    ctx: SessionContext,
    *,
    name: str,
    value: object,
    batch_size: int | None = None,
    ingest_hook: Callable[[Mapping[str, object]], None] | None = None,
) -> DataFrame:
    """Register Arrow-like input and return a DataFusion DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFrame for the registered table.
    """
    if _is_pydict_input(value):
        from_pydict = getattr(ctx, "from_pydict", None)
        pydict = cast("Mapping[str, object]", value)
        if callable(from_pydict) and batch_size is None:
            df = cast("DataFrame", from_pydict(dict(pydict), name=name))
            _emit_arrow_ingest(
                ingest_hook,
                ArrowIngestEvent(
                    name=name,
                    method="from_pydict",
                    partitioning="datafusion_native",
                    batch_size=None,
                    batch_count=None,
                    row_count=None,
                ),
            )
            return df
        table = pa.Table.from_pydict(dict(pydict))
        batches = slice_memtable_batches(table, batch_size=batch_size)
        register_memtable(ctx, name=name, batches=batches)
        _emit_arrow_ingest(
            ingest_hook,
            ArrowIngestEvent(
                name=name,
                method="record_batches",
                partitioning="record_batches",
                batch_size=batch_size,
                batch_count=len(batches),
                row_count=table.num_rows,
            ),
        )
        return ctx.table(name)
    if _is_pylist_input(value):
        from_pylist = getattr(ctx, "from_pylist", None)
        if callable(from_pylist) and batch_size is None:
            rows = cast("Sequence[Mapping[str, object]]", value)
            df = cast("DataFrame", from_pylist(list(rows), name=name))
            _emit_arrow_ingest(
                ingest_hook,
                ArrowIngestEvent(
                    name=name,
                    method="from_pylist",
                    partitioning="datafusion_native",
                    batch_size=None,
                    batch_count=None,
                    row_count=len(rows),
                ),
            )
            return df
        rows = cast("Sequence[Mapping[str, object]]", value)
        table = pa.Table.from_pylist(list(rows))
        batches = slice_memtable_batches(table, batch_size=batch_size)
        register_memtable(ctx, name=name, batches=batches)
        _emit_arrow_ingest(
            ingest_hook,
            ArrowIngestEvent(
                name=name,
                method="record_batches",
                partitioning="record_batches",
                batch_size=batch_size,
                batch_count=len(batches),
                row_count=table.num_rows,
            ),
        )
        return ctx.table(name)
    table = coerce_table_like(value)
    from_arrow = getattr(ctx, "from_arrow", None)
    if callable(from_arrow) and batch_size is None:
        try:
            df = cast("DataFrame", from_arrow(table, name=name))
        except (TypeError, ValueError):
            df = None
        if df is not None:
            row_count = cast("pa.Table", table).num_rows if isinstance(table, pa.Table) else None
            _emit_arrow_ingest(
                ingest_hook,
                ArrowIngestEvent(
                    name=name,
                    method="from_arrow",
                    partitioning="datafusion_native",
                    batch_size=None,
                    batch_count=None,
                    row_count=row_count,
                ),
            )
            return df
    batches = slice_memtable_batches(table, batch_size=batch_size)
    register_memtable(ctx, name=name, batches=batches)
    row_count = sum(batch.num_rows for batch in batches)
    _emit_arrow_ingest(
        ingest_hook,
        ArrowIngestEvent(
            name=name,
            method="record_batches",
            partitioning="record_batches",
            batch_size=batch_size,
            batch_count=len(batches),
            row_count=row_count,
        ),
    )
    return ctx.table(name)


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


def rehydrate_plan_artifacts(
    ctx: SessionContext, *, payload: Mapping[str, object]
) -> DataFrame | None:
    """Rehydrate a DataFusion DataFrame from plan artifact payloads.

    Returns
    -------
    datafusion.dataframe.DataFrame | None
        DataFrame reconstructed from Substrait artifacts, or ``None`` when missing.

    Raises
    ------
    TypeError
        Raised when the payload does not contain a base64 string.
    ValueError
        Raised when the Substrait payload cannot be decoded.
    """
    substrait_b64 = payload.get("substrait_b64")
    if not substrait_b64:
        return None
    if not isinstance(substrait_b64, str):
        msg = "Substrait payload must be base64-encoded string."
        raise TypeError(msg)
    try:
        plan_bytes = base64.b64decode(substrait_b64)
    except (ValueError, TypeError) as exc:
        msg = "Invalid base64 payload for Substrait artifacts."
        raise ValueError(msg) from exc
    return replay_substrait_bytes(ctx, plan_bytes)


__all__ = [
    "CopyToOptions",
    "DataFusionCompileOptions",
    "DataFusionDmlOptions",
    "DataFusionFallbackEvent",
    "DataFusionPlanArtifacts",
    "DataFusionSqlPolicy",
    "IbisCompilerBackend",
    "SqlFallbackContext",
    "collect_plan_artifacts",
    "compile_sqlglot_expr",
    "copy_to_path",
    "datafusion_from_arrow",
    "datafusion_partitioned_readers",
    "datafusion_to_async_batches",
    "datafusion_to_reader",
    "datafusion_to_table",
    "datafusion_view_sql",
    "df_from_sql",
    "execute_dml",
    "execute_prepared_statement",
    "execute_sql",
    "ibis_plan_to_datafusion",
    "ibis_plan_to_reader",
    "ibis_plan_to_table",
    "ibis_to_datafusion",
    "merge_format_options",
    "prepare_statement",
    "register_memtable",
    "register_memtable_from_reader",
    "register_memtable_from_table",
    "rehydrate_plan_artifacts",
    "replay_substrait_bytes",
    "slice_memtable_batches",
    "sqlglot_to_datafusion",
    "validate_substrait_plan",
    "validate_table_constraints",
]
