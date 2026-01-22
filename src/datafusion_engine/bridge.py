"""Bridge helpers for Ibis/SQLGlot execution in DataFusion."""

from __future__ import annotations

import base64
import hashlib
import logging
import re
import time
from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, TypedDict, cast

import pyarrow as pa
import pyarrow.ipc as pa_ipc

try:
    import pyarrow.substrait as pa_substrait
except ImportError:
    pa_substrait = None
try:
    from datafusion.unparser import Dialect as DataFusionDialect
    from datafusion.unparser import Unparser as DataFusionUnparser
except ImportError:
    DataFusionUnparser = None
    DataFusionDialect = None
try:
    from datafusion.plan import LogicalPlan as DataFusionLogicalPlan
except ImportError:
    DataFusionLogicalPlan = None
from datafusion import (
    DataFrameWriteOptions,
    ParquetWriterOptions,
    SessionContext,
    SQLOptions,
    col,
)
from datafusion.dataframe import DataFrame
from datafusion.expr import SortExpr
from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike, coerce_table_like
from arrowdsl.core.ordering import Ordering, OrderingLevel
from arrowdsl.core.streaming import to_reader
from arrowdsl.schema.build import table_from_row_dicts
from arrowdsl.schema.metadata import ordering_metadata_spec
from datafusion_engine.compile_options import (
    DataFusionCacheEvent,
    DataFusionCompileOptions,
    DataFusionDmlOptions,
    DataFusionSqlPolicy,
    resolve_sql_policy,
)
from datafusion_engine.df_builder import TranslationError, df_from_sqlglot
from datafusion_engine.schema_registry import has_schema, schema_for
from engine.plan_cache import PlanCacheEntry, PlanCacheKey
from ibis_engine.params_bridge import datafusion_param_bindings
from ibis_engine.plan import IbisPlan
from ibis_engine.substrait_bridge import (
    record_substrait_gap,
    try_ibis_to_substrait_bytes,
)
from obs.diagnostics import PreparedStatementSpec
from schema_spec.policies import DataFusionWritePolicy
from sqlglot_tools.bridge import IbisCompilerBackend, ibis_to_sqlglot
from sqlglot_tools.compat import ErrorLevel, Expression, exp
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    default_sqlglot_policy,
    normalize_expr,
    parse_sql_strict,
    plan_fingerprint,
    preflight_sql,
    register_datafusion_dialect,
    rewrite_expr,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MaterializationPolicy:
    """Policy controlling when full materialization is allowed.

    Attributes
    ----------
    allow_full_materialization : bool
        Whether to allow calling to_arrow_table() for full materialization.
    max_rows_for_table : int | None
        Maximum row count allowed for table materialization. None means no limit.
    debug_mode : bool
        Whether to allow table materialization in debug/diagnostic contexts.
    """

    allow_full_materialization: bool = False
    max_rows_for_table: int | None = None
    debug_mode: bool = False


@dataclass(frozen=True)
class DualLaneCompilationResult:
    """Result of dual-lane compilation attempt.

    Attributes
    ----------
    df : DataFrame
        Compiled DataFusion DataFrame.
    lane : str
        Compilation lane used: 'substrait' or 'sql'.
    substrait_bytes : bytes | None
        Substrait plan bytes when Substrait lane was used, otherwise ``None``.
    fallback_reason : str | None
        Reason for SQL fallback when Substrait lane failed, otherwise ``None``.
    """

    df: DataFrame
    lane: str
    substrait_bytes: bytes | None = None
    fallback_reason: str | None = None


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
    file_format: str | None = None
    partition_by: Sequence[str] = ()
    dml: DataFusionDmlOptions | None = None
    allow_file_output: bool = False


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
    from datafusion.plan import LogicalPlan as DataFusionLogicalPlanType
    from datafusion.substrait import Consumer as SubstraitConsumerType
    from datafusion.substrait import Serde as SubstraitSerdeType
else:
    DataFusionLogicalPlanType = object


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
        schema_map_hash=options.schema_map_hash,
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
        compilation_lane="sql",
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


def _sql_options_for_options(options: DataFusionCompileOptions) -> SQLOptions:
    policy = options.sql_policy or resolve_sql_policy(
        options.sql_policy_name,
        fallback=_default_sql_policy(),
    )
    return options.sql_options or policy.to_sql_options()


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

    Uses DataFusion SQL when forced or when parameters are present.

    Raises
    ------
    TranslationError
        Raised when the SQLGlot expression cannot be translated.

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
    if resolved.params and _contains_params(expr):
        df = _df_from_sql(ctx, bound_expr, options=resolved)
        _maybe_collect_plan_artifacts(ctx, bound_expr, options=resolved, df=df)
        return df
    try:
        df = df_from_sqlglot(ctx, bound_expr)
    except TranslationError as exc:
        msg = f"DataFusion SQLGlot translation failed: {exc}"
        raise TranslationError(msg) from exc
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
    sql_options = _default_sql_policy().to_sql_options()
    try:
        for constraint in constraints:
            if not constraint.strip():
                continue
            df = ctx.sql_with_options(
                f"SELECT 1 FROM {name} WHERE NOT ({constraint}) LIMIT 1",
                sql_options,
            )
            if _df_has_rows(df):
                violations.append(constraint)
    finally:
        ctx.deregister_table(name)
    return violations


def _df_has_rows(df: DataFrame) -> bool:
    reader = datafusion_to_reader(df)
    for batch in reader:
        return batch.num_rows > 0
    return False


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
    _maybe_enforce_preflight(sg_expr, options=resolved)
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


def ibis_to_datafusion_dual_lane(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    options: DataFusionCompileOptions | None = None,
) -> DualLaneCompilationResult:
    """Compile an Ibis expression using dual-lane strategy.

    This function attempts Substrait compilation first when enabled, falling back
    to SQL generation if Substrait compilation fails. The result includes metadata
    about which compilation lane was used.

    Parameters
    ----------
    expr : IbisTable
        Ibis table expression to compile.
    backend : IbisCompilerBackend
        Backend providing SQLGlot compilation support.
    ctx : SessionContext
        DataFusion session context for execution.
    options : DataFusionCompileOptions | None, optional
        Compilation options controlling dual-lane behavior.

    Returns
    -------
    DualLaneCompilationResult
        Result containing the compiled DataFrame and compilation lane metadata.

    Examples
    --------
    >>> from datafusion import SessionContext
    >>> from ibis_engine.backends import get_backend
    >>> from datafusion_engine.bridge import ibis_to_datafusion_dual_lane
    >>> from datafusion_engine.compile_options import DataFusionCompileOptions
    >>> ctx = SessionContext()
    >>> backend = get_backend()
    >>> expr = backend.table("my_table").select("col1", "col2")
    >>> opts = DataFusionCompileOptions(prefer_substrait=True)
    >>> result = ibis_to_datafusion_dual_lane(expr, backend=backend, ctx=ctx, options=opts)
    >>> print(result.lane)  # 'substrait' or 'sql'
    """
    resolved = options or DataFusionCompileOptions()
    fallback_reason = "Substrait lane disabled (prefer_substrait=False)"

    if resolved.prefer_substrait:
        substrait_result, fallback_reason = _attempt_substrait_lane(
            expr=expr,
            backend=backend,
            ctx=ctx,
            options=resolved,
        )
        if substrait_result is not None:
            return substrait_result

    # SQL fallback lane
    df = ibis_to_datafusion(expr, backend=backend, ctx=ctx, options=resolved)

    return DualLaneCompilationResult(
        df=df,
        lane="sql",
        substrait_bytes=None,
        fallback_reason=fallback_reason if resolved.prefer_substrait else None,
    )


def _attempt_substrait_lane(
    *,
    expr: IbisTable,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    options: DataFusionCompileOptions,
) -> tuple[DualLaneCompilationResult | None, str]:
    fallback_reason = "Substrait lane disabled (prefer_substrait=False)"
    try:
        plan_bytes = try_ibis_to_substrait_bytes(expr, diagnostics_sink=None)
    except (RuntimeError, TypeError, ValueError) as exc:
        fallback_reason = f"Substrait lane error: {exc}"
        logger.info("Substrait lane error, falling back to SQL: %s", fallback_reason)
        _record_substrait_gap_if_enabled(expr=expr, options=options, reason=fallback_reason)
        return None, fallback_reason

    if plan_bytes is None:
        fallback_reason = "Substrait compilation not supported for this expression"
        logger.debug("Substrait lane unavailable, using SQL: %s", fallback_reason)
        return None, fallback_reason

    try:
        df = replay_substrait_bytes(ctx, plan_bytes)
    except (RuntimeError, TypeError, ValueError) as exc:
        fallback_reason = f"Substrait replay failed: {exc}"
        logger.info("Substrait lane failed, falling back to SQL: %s", fallback_reason)
        _record_substrait_gap_if_enabled(expr=expr, options=options, reason=fallback_reason)
        return None, fallback_reason

    if options.substrait_validation:
        validation_result = validate_substrait_plan(plan_bytes, df=df)
        if validation_result.get("status") == "mismatch":
            logger.warning("Substrait validation mismatch detected: %s", validation_result)

    sg_expr = compile_sqlglot_expr(expr, backend=backend, options=options)
    resolved_cached, cache_reason = _resolve_cache_policy(sg_expr, options=options)
    cached_df = (
        df.cache() if _should_cache_df(df, options=resolved_cached, reason=cache_reason) else df
    )
    return (
        DualLaneCompilationResult(
            df=cached_df,
            lane="substrait",
            substrait_bytes=plan_bytes,
            fallback_reason=None,
        ),
        fallback_reason,
    )


def _record_substrait_gap_if_enabled(
    *,
    expr: IbisTable,
    options: DataFusionCompileOptions,
    reason: str,
) -> None:
    if not options.record_substrait_gaps:
        return
    record_substrait_gap(
        expr_type=type(expr).__name__,
        reason=reason,
        sink=None,
    )


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
    policy: MaterializationPolicy | None = None,
) -> TableLike:
    """Materialize a DataFusion DataFrame with optional ordering metadata.

    Full materialization is gated by policy checks to enforce streaming-first
    design. Use datafusion_to_reader() for streaming access.

    Parameters
    ----------
    df : DataFrame
        DataFusion DataFrame to materialize.
    ordering : Ordering | None
        Optional ordering metadata to apply to the result.
    policy : MaterializationPolicy | None
        Policy controlling materialization behavior. If None, uses permissive defaults.

    Returns
    -------
    TableLike
        Arrow table with ordering metadata applied when provided.

    Raises
    ------
    ValueError
        Raised when materialization is blocked by policy.
    """
    resolved_policy = policy or MaterializationPolicy(allow_full_materialization=True)
    if not resolved_policy.allow_full_materialization and not resolved_policy.debug_mode:
        msg = (
            "Full materialization is disabled. Use datafusion_to_reader() for streaming access "
            "or enable debug_mode in MaterializationPolicy."
        )
        raise ValueError(msg)
    if resolved_policy.max_rows_for_table is not None:
        reader = datafusion_to_reader(df, ordering=ordering)
        row_count = sum(batch.num_rows for batch in reader)
        if row_count > resolved_policy.max_rows_for_table:
            msg = (
                f"Row count {row_count} exceeds max_rows_for_table limit "
                f"({resolved_policy.max_rows_for_table}). Use datafusion_to_reader() instead."
            )
            raise ValueError(msg)
        reader = datafusion_to_reader(df, ordering=ordering)
        table = reader.read_all()
    else:
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

    Prefers the __arrow_c_stream__ protocol when available to enable
    zero-copy streaming from DataFusion.

    Returns
    -------
    pyarrow.RecordBatchReader
        Record batch reader for the DataFusion results.
    """
    if hasattr(df, "__arrow_c_stream__"):
        try:
            reader = pa.RecordBatchReader.from_stream(df)
            return _apply_ordering(reader, ordering=ordering)
        except (TypeError, ValueError, AttributeError):
            pass
    stream = getattr(df, "execute_stream", None)
    if callable(stream):
        try:
            reader = stream()
            return _apply_ordering(reader, ordering=ordering)
        except (TypeError, ValueError, RuntimeError):
            pass
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


def _maybe_enforce_preflight(
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> None:
    """Enforce preflight qualification when enabled.

    Parameters
    ----------
    expr
        SQLGlot expression to validate.
    options
        Compilation options with preflight enforcement flag.

    Raises
    ------
    ValueError
        Raised when preflight enforcement is enabled and validation fails.
    """
    if not options.enforce_preflight:
        return
    policy = options.sqlglot_policy or default_sqlglot_policy()
    sql_text = expr.sql(dialect=options.dialect, unsupported_level=ErrorLevel.RAISE)
    result = preflight_sql(
        sql_text,
        schema=options.schema_map,
        dialect=options.dialect,
        strict=True,
        policy=policy,
    )
    if result.errors:
        msg = f"Preflight validation failed: {'; '.join(result.errors)}"
        raise ValueError(msg)


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


def _param_mode(values: Mapping[str, object] | Mapping[IbisValue, object] | None) -> str:
    if not values:
        return "none"
    keys = list(values.keys())
    if all(isinstance(key, str) for key in keys):
        return "named"
    if all(isinstance(key, IbisValue) for key in keys):
        return "ibis"
    return "mixed"


def _validated_param_bindings(
    values: Mapping[str, object] | Mapping[IbisValue, object] | None,
    *,
    allowlist: Sequence[str] | None,
) -> dict[str, object]:
    bindings = datafusion_param_bindings(values or {})
    for name in bindings:
        if not name.isidentifier():
            msg = f"SQL parameter name {name!r} is not a valid identifier."
            raise ValueError(msg)
    if not allowlist:
        return bindings
    allowed = set(allowlist)
    unknown = sorted(name for name in bindings if name not in allowed)
    if unknown:
        msg = f"SQL parameter names not allowlisted: {', '.join(unknown)}."
        raise ValueError(msg)
    return bindings


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
    run_id: str | None = None

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
            "run_id": self.run_id,
        }

    def structured_explain_payload(self) -> dict[str, object]:
        """Return a structured payload suitable for diagnostics table builders.

        Returns
        -------
        dict[str, object]
            Payload structured for datafusion_plan_artifacts_table.
        """
        substrait_b64 = (
            base64.b64encode(self.substrait_plan).decode("utf-8")
            if self.substrait_plan is not None
            else None
        )
        return {
            "event_time_unix_ms": int(time.time() * 1000),
            "run_id": self.run_id,
            "plan_hash": self.plan_hash,
            "sql": self.sql,
            "explain": self.explain,
            "explain_analyze": self.explain_analyze,
            "substrait_b64": substrait_b64,
            "substrait_validation": self.substrait_validation,
            "unparsed_sql": self.unparsed_sql,
            "unparse_error": self.unparse_error,
            "logical_plan": self.logical_plan,
            "optimized_plan": self.optimized_plan,
            "physical_plan": self.physical_plan,
            "graphviz": self.graphviz,
            "partition_count": self.partition_count,
            "join_operators": self.join_operators,
        }


def _df_from_sql(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> DataFrame:
    _ensure_dialect(options.dialect)
    sql = expr.sql(dialect=options.dialect, unsupported_level=ErrorLevel.RAISE)
    bindings = _validated_param_bindings(
        options.params,
        allowlist=options.param_identifier_allowlist,
    )
    policy = options.sql_policy or resolve_sql_policy(
        options.sql_policy_name,
        fallback=_default_sql_policy(),
    )
    sql_options = options.sql_options or policy.to_sql_options()
    violations = _policy_violations(expr, policy)
    if violations:
        if options.enforce_sql_policy:
            msg = f"DataFusion SQL policy violations: {', '.join(violations)}."
            raise ValueError(msg)
        if options.sql_options is None or options.sql_policy is not None or options.sql_policy_name:
            logger.warning(
                "DataFusion SQL policy violations detected: %s",
                ", ".join(violations),
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
    bindings = _validated_param_bindings(
        resolved.params,
        allowlist=resolved.param_identifier_allowlist,
    )
    param_mode = _param_mode(resolved.params)
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
                "sql_policy_name": resolved.sql_policy_name,
                "param_mode": param_mode,
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
    statement = copy_to_statement(sql, path=path, options=options)
    resolved = options or CopyToOptions()
    return execute_dml(ctx, statement, options=resolved.dml)


def copy_to_statement(
    sql: str,
    *,
    path: str,
    options: CopyToOptions | None = None,
) -> str:
    """Return a COPY statement for the provided query and path.

    Parameters
    ----------
    sql
        SQL query to wrap in the COPY statement.
    path
        Output path for the COPY destination.
    options
        COPY statement options and DML configuration.

    Returns
    -------
    str
        COPY statement assembled from the provided options.

    Raises
    ------
    ValueError
        Raised when COPY output is disabled.
    """
    resolved = options or CopyToOptions()
    if not resolved.allow_file_output:
        msg = "COPY TO file outputs are disabled; use Delta writes instead."
        raise ValueError(msg)
    merged = merge_format_options(
        session_defaults=resolved.session_defaults,
        table_options=resolved.table_options,
        statement_overrides=resolved.statement_overrides,
    )
    clause = _copy_options_clause(merged)
    format_clause = _copy_format_clause(resolved.file_format)
    partition_clause = _copy_partition_clause(resolved.partition_by)
    return f"COPY ({sql}) TO {_sql_literal(path)}{format_clause}{partition_clause}{clause}"


def _copy_format_clause(file_format: str | None) -> str:
    if not file_format:
        return ""
    return f" STORED AS {file_format.upper()}"


def _copy_partition_clause(partition_by: Sequence[str]) -> str:
    if not partition_by:
        return ""
    cols = ", ".join(_sql_identifier(name) for name in partition_by)
    return f" PARTITIONED BY ({cols})"


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


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
) -> DataFrame:
    """Execute a SQLGlot expression using DataFusion SQL execution.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame representing the expression.
    """
    return _df_from_sql(ctx, expr, options=options)


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
    sql_options = _sql_options_for_options(options)
    explain_df = ctx.sql_with_options(f"{prefix} {sql}", sql_options)
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
        artifacts = collect_plan_artifacts(ctx, expr, options=options, df=df, run_id=options.run_id)
    except (RuntimeError, TypeError, ValueError):
        return
    if options.plan_artifacts_hook is not None:
        options.plan_artifacts_hook(artifacts.structured_explain_payload())


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
    if DataFusionUnparser is None or DataFusionDialect is None:
        return None, "unparser_unavailable"
    try:
        unparser = DataFusionUnparser(DataFusionDialect.default())
        plan_obj = cast("DataFusionLogicalPlanType", plan)
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
    run_id: str | None = None,
) -> DataFusionPlanArtifacts:
    """Collect plan artifacts for diagnostics.

    Parameters
    ----------
    ctx:
        DataFusion session context.
    expr:
        SQLGlot expression to collect artifacts for.
    options:
        Compilation options controlling artifact collection.
    df:
        Optional pre-compiled DataFrame to avoid re-execution.
    run_id:
        Optional correlation ID for run tracking.

    Returns
    -------
    DataFusionPlanArtifacts
        Plan artifacts derived from EXPLAIN and Substrait serialization.
    """
    _ensure_dialect(options.dialect)
    sql = expr.sql(dialect=options.dialect, unsupported_level=ErrorLevel.RAISE)
    sql_options = _sql_options_for_options(options)
    plan_df = df
    if plan_df is None:
        plan_df = ctx.sql_with_options(sql, sql_options)
    explain_rows = _collect_reader(ctx.sql_with_options(f"EXPLAIN {sql}", sql_options))
    analyze_rows = None
    if options.explain_analyze:
        analyze_rows = _collect_reader(ctx.sql_with_options(f"EXPLAIN ANALYZE {sql}", sql_options))
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
        schema_map_hash=options.schema_map_hash,
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
        run_id=run_id,
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
    expr = parse_sql_strict(sql, dialect=options.dialect)
    df = _df_from_sql(ctx, expr, options=options)
    return _collect_reader(df)


def datafusion_write_options(
    policy: DataFusionWritePolicy | None,
) -> tuple[DataFrameWriteOptions | None, ParquetWriterOptions | None]:
    """Return DataFusion write options for a write policy.

    Returns
    -------
    tuple[DataFrameWriteOptions | None, ParquetWriterOptions | None]
        DataFrame + Parquet writer option instances.
    """
    if policy is None:
        return None, None
    sort_exprs = _datafusion_sort_exprs(policy.sort_by)
    write_options = DataFrameWriteOptions(
        partition_by=list(policy.partition_by) if policy.partition_by else None,
        single_file_output=policy.single_file_output,
        sort_by=sort_exprs or None,
    )
    parquet_kwargs: _ParquetWriterOptionsKwargs = {}
    if policy.parquet_compression is not None:
        parquet_kwargs["compression"] = policy.parquet_compression
    if policy.parquet_statistics_enabled is not None:
        parquet_kwargs["statistics_enabled"] = policy.parquet_statistics_enabled
    if policy.parquet_row_group_size is not None:
        parquet_kwargs["max_row_group_size"] = int(policy.parquet_row_group_size)
    parquet_options = (
        ParquetWriterOptions(**parquet_kwargs) if parquet_kwargs else ParquetWriterOptions()
    )
    return write_options, parquet_options


def datafusion_write_parquet(
    df: DataFrame,
    *,
    path: str,
    policy: DataFusionWritePolicy | None = None,
) -> dict[str, object]:
    """Write a DataFusion DataFrame to Parquet using policy options.

    Returns
    -------
    dict[str, object]
        Payload describing the write policy applied.
    """
    write_options, parquet_options = datafusion_write_options(policy)
    if parquet_options is None:
        df.write_parquet(path, write_options=write_options)
    else:
        df.write_parquet_with_options(path, parquet_options, write_options=write_options)
    return {
        "path": str(path),
        "write_policy": policy.payload() if policy is not None else None,
        "parquet_options": _parquet_options_payload(parquet_options),
    }


def _datafusion_sort_exprs(sort_by: Sequence[str]) -> list[SortExpr]:
    return [col(name).sort(ascending=True, nulls_first=True) for name in sort_by]


def _parquet_options_payload(options: ParquetWriterOptions | None) -> dict[str, object] | None:
    if options is None:
        return None
    return {
        "compression": getattr(options, "compression", None),
        "statistics_enabled": getattr(options, "statistics_enabled", None),
        "max_row_group_size": getattr(options, "max_row_group_size", None),
    }


class _ParquetWriterOptionsKwargs(TypedDict, total=False):
    compression: str | None
    statistics_enabled: str | None
    max_row_group_size: int


def _record_batches(
    table: TableLike | RecordBatchReaderLike,
    *,
    batch_size: int | None,
) -> list[pa.RecordBatch]:
    if isinstance(table, RecordBatchReaderLike):
        resolved_table = cast("pa.Table", table.read_all())
    else:
        resolved_table = cast("pa.Table", table)
    if batch_size is None or batch_size <= 0:
        return list(resolved_table.to_batches())
    return list(resolved_table.to_batches(max_chunksize=batch_size))


def _is_pydict_input(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    if not value:
        return True
    return all(isinstance(key, str) for key in value)


def _is_row_mapping_sequence(value: object) -> bool:
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

    Raises
    ------
    TypeError
        Raised when the DataFusion SessionContext lacks ingestion support.

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
        batches = _record_batches(table, batch_size=batch_size)
        register_batches = getattr(ctx, "register_record_batches", None)
        if not callable(register_batches):
            msg = "DataFusion SessionContext missing register_record_batches."
            raise TypeError(msg)
        register_batches(name, batches)
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
    if _is_row_mapping_sequence(value):
        rows = cast("Sequence[Mapping[str, object]]", value)
        value = table_from_row_dicts(rows)
    requested_schema = schema_for(name) if has_schema(name) else None
    table = coerce_table_like(value, requested_schema=requested_schema)
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
    batches = _record_batches(table, batch_size=batch_size)
    register_batches = getattr(ctx, "register_record_batches", None)
    if not callable(register_batches):
        msg = "DataFusion SessionContext missing register_record_batches."
        raise TypeError(msg)
    register_batches(name, batches)
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
    "DataFusionPlanArtifacts",
    "DataFusionSqlPolicy",
    "DualLaneCompilationResult",
    "IbisCompilerBackend",
    "MaterializationPolicy",
    "collect_plan_artifacts",
    "compile_sqlglot_expr",
    "copy_to_path",
    "copy_to_statement",
    "datafusion_from_arrow",
    "datafusion_partitioned_readers",
    "datafusion_to_async_batches",
    "datafusion_to_reader",
    "datafusion_to_table",
    "datafusion_view_sql",
    "datafusion_write_options",
    "datafusion_write_parquet",
    "df_from_sql",
    "execute_dml",
    "execute_prepared_statement",
    "execute_sql",
    "ibis_plan_to_datafusion",
    "ibis_plan_to_reader",
    "ibis_plan_to_table",
    "ibis_to_datafusion",
    "ibis_to_datafusion_dual_lane",
    "merge_format_options",
    "prepare_statement",
    "rehydrate_plan_artifacts",
    "replay_substrait_bytes",
    "sqlglot_to_datafusion",
    "validate_substrait_plan",
    "validate_table_constraints",
]
