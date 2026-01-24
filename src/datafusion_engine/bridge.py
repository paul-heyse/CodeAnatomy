"""Bridge helpers for Ibis/SQLGlot execution in DataFusion."""

from __future__ import annotations

import base64
import contextlib
import hashlib
import logging
import re
import time
import uuid
from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal, TypedDict, cast

import msgspec
import pyarrow as pa
import pyarrow.ipc as pa_ipc
from sqlglot.errors import SqlglotError

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
    ParquetColumnOptions,
    ParquetWriterOptions,
    SessionContext,
    SQLOptions,
    col,
)
from datafusion.dataframe import DataFrame
from datafusion.expr import SortExpr
from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.interop import (
    RecordBatchReaderLike,
    TableLike,
    coerce_table_like,
    concat_readers,
)
from arrowdsl.core.ordering import Ordering, OrderingLevel
from arrowdsl.core.streaming import to_reader
from arrowdsl.schema.build import table_from_row_dicts
from arrowdsl.schema.metadata import ordering_metadata_spec
from datafusion_engine.compile_options import (
    DataFusionCacheEvent,
    DataFusionCompileOptions,
    DataFusionDmlOptions,
    DataFusionSqlPolicy,
    DataFusionSubstraitFallbackEvent,
    ExplainRows,
    resolve_sql_policy,
)
from datafusion_engine.compile_pipeline import CompilationPipeline, CompileOptions
from datafusion_engine.param_binding import resolve_param_bindings
from datafusion_engine.registry_bridge import (
    apply_projection_overrides,
    apply_projection_scan_overrides,
)
from datafusion_engine.schema_registry import has_schema, schema_for
from datafusion_engine.sql_policy_engine import SQLPolicyProfile
from datafusion_engine.table_provider_capsule import TableProviderCapsule
from datafusion_engine.write_pipeline import (
    WriteFormat,
    WriteMode,
    WritePipeline,
    WriteRequest,
    parquet_policy_from_datafusion,
)
from engine.plan_cache import PlanCacheEntry, PlanCacheKey
from ibis_engine.param_tables import scalar_param_signature
from ibis_engine.plan import IbisPlan
from ibis_engine.substrait_bridge import (
    record_substrait_gap,
    try_ibis_to_substrait_bytes,
)
from obs.diagnostics import PreparedStatementSpec
from schema_spec.policies import DataFusionWritePolicy
from serde_msgspec import dumps_msgpack
from sqlglot_tools.bridge import (
    AstExecutionResult,
    IbisCompilerBackend,
    execute_sqlglot_ast,
    ibis_to_datafusion_ast_path,
    ibis_to_sqlglot,
)
from sqlglot_tools.compat import ErrorLevel, Expression, exp, parse_one
from sqlglot_tools.lineage import (
    LineageExtractionOptions,
    LineagePayload,
    canonical_ast_fingerprint,
    extract_lineage_payload,
)
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    PreflightOptions,
    SchemaMapping,
    SqlGlotPolicy,
    ast_to_artifact,
    bind_params,
    build_insert,
    build_select,
    emit_preflight_diagnostics,
    normalize_expr,
    parse_sql_strict,
    plan_fingerprint,
    preflight_sql,
    register_datafusion_dialect,
    resolve_sqlglot_policy,
    rewrite_expr,
    serialize_ast_artifact,
    sqlglot_emit,
    sqlglot_policy_snapshot_for,
    sqlglot_sql,
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

    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from schema_spec.policies import ParquetColumnPolicy
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
    ctx: SessionContext,
    options: DataFusionCompileOptions,
) -> tuple[DataFusionCompileOptions, str | None]:
    if options.cache is not None:
        return options, "cache_configured"
    cache_key = _plan_cache_key(expr, ctx=ctx, options=options)
    if cache_key is None or options.plan_cache is None:
        return options, None
    if options.plan_cache.contains(cache_key):
        return replace(options, cache=True), "plan_cache_hit"
    return options, None


def _plan_cache_key(
    expr: Expression,
    *,
    ctx: SessionContext,
    options: DataFusionCompileOptions,
) -> PlanCacheKey | None:
    if options.plan_cache is None or options.profile_hash is None:
        return None
    if options.params is not None or options.named_params is not None:
        return None
    policy_hash = _sqlglot_policy_hash(options)
    plan_hash = options.plan_hash or plan_fingerprint(
        expr,
        dialect=options.dialect,
        policy_hash=policy_hash,
        schema_map_hash=options.schema_map_hash,
    )
    _ensure_dialect(options.dialect)
    try:
        sql = _emit_sql(expr, options=options)
    except (RuntimeError, TypeError, ValueError):
        return None
    plan_bytes = _substrait_bytes(ctx, sql)
    if plan_bytes is None:
        return None
    substrait_hash = hashlib.sha256(plan_bytes).hexdigest()
    return PlanCacheKey(
        plan_hash=plan_hash,
        profile_hash=options.profile_hash,
        substrait_hash=substrait_hash,
    )


def _plan_cache_key_from_substrait(
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
    plan_bytes: bytes,
) -> PlanCacheKey | None:
    if options.plan_cache is None or options.profile_hash is None:
        return None
    if options.params is not None or options.named_params is not None:
        return None
    policy_hash = _sqlglot_policy_hash(options)
    plan_hash = options.plan_hash or plan_fingerprint(
        expr,
        dialect=options.dialect,
        policy_hash=policy_hash,
        schema_map_hash=options.schema_map_hash,
    )
    substrait_hash = hashlib.sha256(plan_bytes).hexdigest()
    return PlanCacheKey(
        plan_hash=plan_hash,
        profile_hash=options.profile_hash,
        substrait_hash=substrait_hash,
    )


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
    if options.sql_options is not None:
        return options.sql_options
    if options.runtime_profile is not None:
        return options.runtime_profile.sql_options()
    policy = options.sql_policy or resolve_sql_policy(
        options.sql_policy_name,
        fallback=_default_sql_policy(),
    )
    return policy.to_sql_options()


def _ensure_dialect(name: str) -> None:
    if name == "datafusion_ext":
        register_datafusion_dialect()


def _sql_policy_profile_from_options(
    options: DataFusionCompileOptions,
) -> SQLPolicyProfile:
    if options.sql_policy_profile is not None:
        return options.sql_policy_profile
    if options.sqlglot_policy is not None:
        base = SQLPolicyProfile(
            read_dialect=options.sqlglot_policy.read_dialect,
            write_dialect=options.sqlglot_policy.write_dialect,
            optimizer_rules=tuple(options.sqlglot_policy.rules),
            normalize_distance_limit=options.sqlglot_policy.normalization_distance,
            expand_stars=options.sqlglot_policy.expand_stars,
            validate_qualify_columns=options.sqlglot_policy.validate_qualify_columns,
            identify_mode=options.sqlglot_policy.identify,
        )
    else:
        base = SQLPolicyProfile(
            read_dialect=options.dialect,
            write_dialect=options.dialect,
        )
    if not options.optimize:
        base = replace(
            base,
            optimizer_rules=(),
            pushdown_predicates=False,
            pushdown_projections=False,
        )
    return base


def _compilation_rewrite_hook(
    *,
    ctx: SessionContext,
    options: DataFusionCompileOptions,
) -> Callable[[Expression], Expression]:
    def _hook(expr: Expression) -> Expression:
        rewritten = _apply_rewrite_hook(expr, options=options)
        _maybe_enforce_preflight(rewritten, options=options)
        rewritten = _apply_dynamic_projection(rewritten, options=options)
        _apply_projection_overrides_for_expr(ctx, rewritten, options=options)
        return rewritten

    return _hook


def _compilation_pipeline(
    ctx: SessionContext,
    *,
    options: DataFusionCompileOptions,
) -> CompilationPipeline:
    profile = _sql_policy_profile_from_options(options)
    compile_options = CompileOptions(
        prefer_substrait=options.prefer_substrait,
        prefer_ast_execution=options.prefer_ast_execution,
        profile=profile,
        schema=options.schema_map,
        enable_rewrites=True,
        rewrite_hook=_compilation_rewrite_hook(ctx=ctx, options=options),
    )
    return CompilationPipeline(ctx, compile_options)


def _sqlglot_emit_policy(options: DataFusionCompileOptions) -> SqlGlotPolicy:
    base = options.sqlglot_policy or resolve_sqlglot_policy(name="datafusion_compile")
    return replace(
        base,
        read_dialect=options.dialect,
        write_dialect=options.dialect,
        unsupported_level=ErrorLevel.RAISE,
    )


def _sqlglot_policy_hash(
    options: DataFusionCompileOptions,
    *,
    policy: SqlGlotPolicy | None = None,
) -> str | None:
    if options.sqlglot_policy_hash is not None:
        return options.sqlglot_policy_hash
    resolved_policy = policy or _sqlglot_emit_policy(options)
    return sqlglot_policy_snapshot_for(resolved_policy).policy_hash


def _emit_sql(expr: Expression, *, options: DataFusionCompileOptions) -> str:
    policy = _sqlglot_emit_policy(options)
    return sqlglot_emit(expr, policy=policy)


def _emit_internal_sql(expr: Expression) -> str:
    return _emit_sql(expr, options=DataFusionCompileOptions())


def _parse_sql_expr(sql: str) -> Expression | None:
    try:
        return parse_one(sql)
    except (SqlglotError, TypeError, ValueError):
        return None


def df_from_sqlglot_or_sql(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions | None = None,
) -> DataFrame:
    """Translate a SQLGlot expression into a DataFusion DataFrame.

    Uses the unified compilation pipeline for DataFusion execution.

    Raises
    ------
    ValueError
        Raised when named parameters violate read-only SQL policy.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame representing the expression.
    """
    resolved = options or DataFusionCompileOptions()
    named_params = _validated_named_params(
        resolved.named_params,
        allowlist=resolved.param_identifier_allowlist,
    )
    if named_params:
        read_only_policy = DataFusionSqlPolicy()
        violations = _policy_violations(expr, read_only_policy)
        if violations:
            msg = f"Named parameter SQL must be read-only: {', '.join(violations)}."
            raise ValueError(msg)
    pipeline = _compilation_pipeline(ctx, options=resolved)
    compiled = pipeline.compile_ast(expr)
    resolved, cache_reason = _resolve_cache_policy(compiled.sqlglot_ast, ctx=ctx, options=resolved)
    bindings = _validated_param_bindings(
        resolved.params,
        allowlist=resolved.param_identifier_allowlist,
    )
    sql_options = (
        _sql_options_for_named_params(resolved)
        if named_params
        else _sql_options_for_options(resolved)
    )
    df = pipeline.execute(
        compiled,
        params=bindings or None,
        named_params=named_params or None,
        sql_options=sql_options,
    )
    _maybe_explain(ctx, compiled.sqlglot_ast, options=resolved)
    _maybe_collect_plan_artifacts(ctx, compiled.sqlglot_ast, options=resolved, df=df)
    return df.cache() if _should_cache_df(df, options=resolved, reason=cache_reason) else df


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
            constraint_expr = _parse_sql_expr(constraint)
            if constraint_expr is None:
                query = f"SELECT 1 FROM {name} WHERE NOT ({constraint}) LIMIT 1"
            else:
                query_expr = build_select(
                    [exp.Literal.number(1)],
                    from_=name,
                    where=exp.not_(constraint_expr),
                    limit=1,
                )
                query = _emit_internal_sql(query_expr)
            df = ctx.sql_with_options(query, sql_options)
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

    Raises
    ------
    ValueError
        Raised when named parameters violate read-only SQL policy.
    """
    resolved = options or DataFusionCompileOptions()
    named_params = _validated_named_params(
        resolved.named_params,
        allowlist=resolved.param_identifier_allowlist,
    )
    if named_params:
        read_only_policy = DataFusionSqlPolicy()
        violations = _policy_violations(expr, read_only_policy)
        if violations:
            msg = f"Named parameter SQL must be read-only: {', '.join(violations)}."
            raise ValueError(msg)
    pipeline = _compilation_pipeline(ctx, options=resolved)
    compiled = pipeline.compile_ast(expr)
    resolved, cache_reason = _resolve_cache_policy(compiled.sqlglot_ast, ctx=ctx, options=resolved)
    bindings = _validated_param_bindings(
        resolved.params,
        allowlist=resolved.param_identifier_allowlist,
    )
    sql_options = (
        _sql_options_for_named_params(resolved)
        if named_params
        else _sql_options_for_options(resolved)
    )
    df = pipeline.execute(
        compiled,
        params=bindings or None,
        named_params=named_params or None,
        sql_options=sql_options,
    )
    _maybe_explain(ctx, compiled.sqlglot_ast, options=resolved)
    _maybe_collect_plan_artifacts(ctx, compiled.sqlglot_ast, options=resolved, df=df)
    return df.cache() if _should_cache_df(df, options=resolved, reason=cache_reason) else df


def compile_sqlglot_expr(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext | None = None,
    options: DataFusionCompileOptions | None = None,
) -> Expression:
    """Compile an Ibis expression into a SQLGlot expression.

    Parameters
    ----------
    expr:
        Ibis expression to compile.
    backend:
        Backend providing SQLGlot compilation support.
    ctx:
        Optional DataFusion session context for schema-aware canonicalization.
    options:
        Optional compilation options controlling rewrites and normalization.

    Returns
    -------
    sqlglot.Expression
        SQLGlot expression for the Ibis input.
    """
    resolved = options or DataFusionCompileOptions()
    if resolved.ibis_expr is None:
        resolved = replace(resolved, ibis_expr=expr)
    if ctx is None:
        sg_expr = ibis_to_sqlglot(
            expr,
            backend=backend,
            params=_ibis_param_bindings(resolved.params),
        )
        sg_expr = _apply_rewrite_hook(sg_expr, options=resolved)
        _maybe_enforce_preflight(sg_expr, options=resolved)
        return _apply_dynamic_projection(sg_expr, options=resolved)
    pipeline = _compilation_pipeline(ctx, options=resolved)
    compiled = pipeline.compile_ibis(expr, backend=backend)
    return compiled.sqlglot_ast


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
    pipeline = _compilation_pipeline(ctx, options=resolved)
    compiled = pipeline.compile_ibis(expr, backend=backend)
    resolved, cache_reason = _resolve_cache_policy(compiled.sqlglot_ast, ctx=ctx, options=resolved)
    bindings = _validated_param_bindings(
        resolved.params,
        allowlist=resolved.param_identifier_allowlist,
    )
    named_params = _validated_named_params(
        resolved.named_params,
        allowlist=resolved.param_identifier_allowlist,
    )
    df = pipeline.execute(
        compiled,
        params=bindings or None,
        named_params=named_params or None,
        sql_options=_sql_options_for_options(resolved),
    )
    _maybe_explain(ctx, compiled.sqlglot_ast, options=resolved)
    _maybe_collect_plan_artifacts(ctx, compiled.sqlglot_ast, options=resolved, df=df)
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
    >>> from datafusion_engine.runtime import DataFusionRuntimeProfile
    >>> from ibis_engine.backends import get_backend
    >>> from datafusion_engine.bridge import ibis_to_datafusion_dual_lane
    >>> from datafusion_engine.compile_options import DataFusionCompileOptions
    >>> ctx = DataFusionRuntimeProfile().ephemeral_context()
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
        _record_substrait_fallback(expr=expr, options=options, reason=fallback_reason)
        _record_substrait_gap_if_enabled(expr=expr, options=options, reason=fallback_reason)
        return None, fallback_reason

    if plan_bytes is None:
        fallback_reason = "Substrait compilation not supported for this expression"
        logger.debug("Substrait lane unavailable, using SQL: %s", fallback_reason)
        _record_substrait_fallback(expr=expr, options=options, reason=fallback_reason)
        return None, fallback_reason

    try:
        df = replay_substrait_bytes(ctx, plan_bytes)
    except (RuntimeError, TypeError, ValueError) as exc:
        fallback_reason = f"Substrait replay failed: {exc}"
        logger.info("Substrait lane failed, falling back to SQL: %s", fallback_reason)
        _record_substrait_fallback(expr=expr, options=options, reason=fallback_reason)
        _record_substrait_gap_if_enabled(expr=expr, options=options, reason=fallback_reason)
        return None, fallback_reason

    if options.substrait_validation:
        validation_result = validate_substrait_plan(plan_bytes, df=df)
        if validation_result.get("status") == "mismatch":
            logger.warning("Substrait validation mismatch detected: %s", validation_result)

    sg_expr = compile_sqlglot_expr(expr, backend=backend, ctx=ctx, options=options)
    resolved_cached, cache_reason = _resolve_cache_policy(sg_expr, ctx=ctx, options=options)
    cached_df = (
        df.cache() if _should_cache_df(df, options=resolved_cached, reason=cache_reason) else df
    )
    _maybe_store_substrait_lane(
        sg_expr,
        plan_bytes,
        options=options,
        lane="substrait",
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


def _record_substrait_fallback(
    *,
    expr: IbisTable,
    options: DataFusionCompileOptions,
    reason: str,
    plan_hash: str | None = None,
) -> None:
    hook = options.substrait_fallback_hook
    if hook is None:
        return
    hook(
        DataFusionSubstraitFallbackEvent(
            reason=reason,
            expr_type=type(expr).__name__,
            plan_hash=plan_hash,
            profile_hash=options.profile_hash,
            run_id=options.run_id,
        )
    )


def _maybe_store_substrait_lane(
    expr: Expression,
    plan_bytes: bytes,
    *,
    options: DataFusionCompileOptions,
    lane: str,
) -> None:
    cache_key = _plan_cache_key_from_substrait(expr, options=options, plan_bytes=plan_bytes)
    if cache_key is None or options.plan_cache is None:
        return
    if options.plan_cache.contains(cache_key):
        return
    entry = PlanCacheEntry(
        plan_hash=cache_key.plan_hash,
        profile_hash=cache_key.profile_hash,
        substrait_hash=cache_key.substrait_hash,
        plan_bytes=plan_bytes,
        compilation_lane=lane,
    )
    options.plan_cache.put(entry)


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
    reader = datafusion_to_reader(df, ordering=ordering)
    if resolved_policy.max_rows_for_table is not None:
        row_count = sum(batch.num_rows for batch in reader)
        if row_count > resolved_policy.max_rows_for_table:
            msg = (
                f"Row count {row_count} exceeds max_rows_for_table limit "
                f"({resolved_policy.max_rows_for_table}). Use datafusion_to_reader() instead."
            )
            raise ValueError(msg)
        reader = datafusion_to_reader(df, ordering=ordering)
    table = reader.read_all()
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

    Prefers partitioned streaming when available, falling back to the
    __arrow_c_stream__ protocol for zero-copy streaming.

    Returns
    -------
    pyarrow.RecordBatchReader
        Record batch reader for the DataFusion results.
    """
    partitioned = datafusion_partitioned_readers(df)
    if partitioned:
        try:
            reader = concat_readers(partitioned)
            return _apply_ordering(reader, ordering=ordering)
        except ValueError:
            pass
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


def datafusion_read_table(
    ctx: SessionContext,
    table: object,
    *,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> DataFrame:
    """Return a DataFusion DataFrame from a table provider without registration.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the table provider.
    """
    df = ctx.read_table(table)
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return df
    runtime_profile.diagnostics_sink.record_artifact(
        "datafusion_read_table_v1",
        {
            "event_time_unix_ms": int(time.time() * 1000),
            "table_type": type(table).__name__,
        },
    )
    return df


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


def _apply_dynamic_projection(
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> Expression:
    if not options.dynamic_projection:
        return expr
    projection_map = _projection_requirements(expr)
    if not projection_map:
        return expr
    return _rewrite_tables_with_projections(expr, projection_map=projection_map)


def _apply_projection_overrides_for_expr(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> None:
    if not options.dynamic_projection:
        return
    projection_map = _projection_requirements(expr)
    if not projection_map:
        return
    apply_projection_scan_overrides(
        ctx,
        projection_map=projection_map,
        runtime_profile=options.runtime_profile,
    )
    apply_projection_overrides(
        ctx,
        projection_map=projection_map,
        sql_options=_sql_options_for_options(options),
    )


def _projection_requirements(expr: Expression) -> dict[str, tuple[str, ...]]:
    alias_map = _alias_to_table_map(expr)
    required: dict[str, set[str]] = {}
    for column in expr.find_all(exp.Column):
        table = column.table
        name = column.name
        if not table or not name:
            continue
        table_name = alias_map.get(table, table)
        if table_name is None:
            continue
        required.setdefault(table_name, set()).add(name)
    return {name: tuple(sorted(cols)) for name, cols in required.items()}


def _projection_map_payload(
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> bytes | None:
    if not options.dynamic_projection:
        return None
    projection_map = _projection_requirements(expr)
    if not projection_map:
        return None
    payload = {name: list(columns) for name, columns in projection_map.items()}
    return dumps_msgpack(payload)


def _lineage_schema_map(
    schema_map: SchemaMapping | None,
) -> Mapping[str, Mapping[str, str]] | None:
    if schema_map is None:
        return None
    normalized: dict[str, dict[str, str]] = {}
    for table, columns in schema_map.items():
        if not isinstance(columns, Mapping):
            continue
        if not all(isinstance(value, str) for value in columns.values()):
            continue
        normalized[table] = {str(key): str(value) for key, value in columns.items()}
    return normalized or None


@dataclass(frozen=True)
class _PlanArtifactsDetails:
    df: DataFrame
    explain_rows: ExplainRows
    analyze_rows: ExplainRows | None
    substrait_plan: bytes | None
    substrait_validation: Mapping[str, object] | None
    plan_details: Mapping[str, object]
    unparsed_sql: str | None
    unparse_error: str | None


def _collect_plan_artifacts_details(
    ctx: SessionContext,
    *,
    sql: str,
    options: DataFusionCompileOptions,
    df: DataFrame | None,
) -> _PlanArtifactsDetails:
    sql_options = _sql_options_for_options(options)
    plan_df = df or ctx.sql_with_options(sql, sql_options)
    explain_rows = _collect_reader(ctx.sql_with_options(f"EXPLAIN {sql}", sql_options))
    analyze_rows = None
    if options.explain_analyze:
        analyze_rows = _collect_reader(ctx.sql_with_options(f"EXPLAIN ANALYZE {sql}", sql_options))
    substrait_plan = _substrait_bytes(ctx, sql)
    substrait_validation = None
    if substrait_plan is not None and options.substrait_validation:
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
    return _PlanArtifactsDetails(
        df=plan_df,
        explain_rows=explain_rows,
        analyze_rows=analyze_rows,
        substrait_plan=substrait_plan,
        substrait_validation=substrait_validation,
        plan_details=plan_details,
        unparsed_sql=unparsed_sql,
        unparse_error=unparse_error,
    )


def _alias_to_table_map(expr: Expression) -> dict[str, str | None]:
    alias_map: dict[str, str | None] = {}
    for table in expr.find_all(exp.Table):
        alias = table.args.get("alias")
        alias_name = _table_alias_name(alias)
        if alias_name:
            alias_map[alias_name] = table.name
    for subquery in expr.find_all(exp.Subquery):
        alias = subquery.args.get("alias")
        alias_name = _table_alias_name(alias)
        if alias_name:
            alias_map.setdefault(alias_name, None)
    return alias_map


def _table_alias_name(alias: Expression | str | None) -> str | None:
    alias_expr: Expression | str | None = alias
    if isinstance(alias_expr, exp.TableAlias):
        alias_expr = alias_expr.this
    if isinstance(alias_expr, exp.Identifier):
        return alias_expr.this
    if isinstance(alias_expr, str):
        return alias_expr
    return None


def _rewrite_tables_with_projections(
    expr: Expression,
    *,
    projection_map: Mapping[str, tuple[str, ...]],
) -> Expression:
    def _rewrite(node: Expression) -> Expression:
        if not isinstance(node, exp.Table):
            return node
        table_name = node.name
        columns = projection_map.get(table_name)
        if not columns:
            return node
        base = node.copy()
        alias = base.args.get("alias")
        base.set("alias", None)
        projections = [exp.column(name).as_(name) for name in columns]
        select = exp.select(*projections).from_(base)
        return exp.Subquery(this=select, alias=alias)

    return expr.transform(_rewrite)


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
    if not options.enforce_preflight and options.sql_ingest_hook is None:
        return
    policy = _sqlglot_emit_policy(options)
    sql_text = _emit_sql(expr, options=options)
    result = preflight_sql(
        sql_text,
        options=PreflightOptions(
            schema=options.schema_map,
            dialect=options.dialect,
            strict=True,
            policy=policy,
        ),
    )
    diagnostics = emit_preflight_diagnostics(result)
    if options.run_id is not None:
        diagnostics["run_id"] = options.run_id
    if options.sql_ingest_hook is not None:
        options.sql_ingest_hook(diagnostics)
    if result.errors and options.enforce_preflight:
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
    return bool(expr.find(exp.Placeholder)) or bool(expr.find(exp.Parameter))


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
    bindings = resolve_param_bindings(values)
    has_scalar = bool(bindings.param_values)
    has_tables = bool(bindings.named_tables)
    if has_scalar and has_tables:
        return "mixed"
    if has_tables:
        return "table"
    if has_scalar:
        return "scalar"
    return "none"


def _validated_param_bindings(
    values: Mapping[str, object] | Mapping[IbisValue, object] | None,
    *,
    allowlist: Sequence[str] | None,
) -> dict[str, object]:
    bindings = resolve_param_bindings(values, allowlist=allowlist)
    if bindings.named_tables:
        msg = "Table-like parameters must be passed via named params."
        raise ValueError(msg)
    return bindings.param_values


def _named_param_type(value: object) -> str:
    if isinstance(value, DataFrame):
        return "DataFrame"
    if isinstance(value, TableProviderCapsule):
        return "TableProviderCapsule"
    if isinstance(value, RecordBatchReaderLike):
        return "RecordBatchReaderLike"
    if isinstance(value, TableLike):
        return "TableLike"
    return type(value).__name__


def _validated_named_params(
    params: Mapping[str, object] | None,
    *,
    allowlist: Sequence[str] | None,
) -> Mapping[str, object]:
    bindings = resolve_param_bindings(params, allowlist=allowlist)
    if bindings.param_values:
        msg = "Named parameters must be table-like."
        raise ValueError(msg)
    return bindings.named_tables


def _emit_named_param_diagnostics(
    options: DataFusionCompileOptions,
    *,
    params: Mapping[str, object],
) -> None:
    if not params:
        return
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "named_param_count": len(params),
        "named_params": {name: _named_param_type(value) for name, value in params.items()},
        "dialect": options.dialect,
        "run_id": options.run_id,
    }
    if options.sql_ingest_hook is not None:
        options.sql_ingest_hook(payload)
        return
    if options.runtime_profile is None or options.runtime_profile.diagnostics_sink is None:
        return
    options.runtime_profile.diagnostics_sink.record_artifact(
        "datafusion_named_params_v1",
        payload,
    )


def _sql_options_for_named_params(options: DataFusionCompileOptions) -> SQLOptions:
    if options.sql_options is not None:
        base = options.sql_options
    elif options.runtime_profile is not None:
        base = options.runtime_profile.sql_options()
    else:
        base = SQLOptions()
    allow_ddl = False
    allow_dml = False
    allow_statements = False
    return (
        base.with_allow_ddl(allow_ddl)
        .with_allow_dml(allow_dml)
        .with_allow_statements(allow_statements)
    )



@dataclass(frozen=True)
class DataFusionPlanArtifacts:
    """Captured DataFusion plan artifacts for diagnostics."""

    plan_hash: str | None
    sql: str
    explain: TableLike | RecordBatchReaderLike
    explain_analyze: TableLike | RecordBatchReaderLike | None
    substrait_plan: bytes | None
    normalized_sql: str | None = None
    substrait_validation: Mapping[str, object] | None = None
    sqlglot_ast: bytes | None = None
    ibis_decompile: str | None = None
    ibis_sql: str | None = None
    ibis_sql_pretty: str | None = None
    ibis_graphviz: str | None = None
    ibis_compiled_sql: str | None = None
    ibis_compiled_sql_hash: str | None = None
    ibis_compile_params: str | None = None
    ibis_compile_limit: int | None = None
    read_dialect: str | None = None
    write_dialect: str | None = None
    canonical_fingerprint: str | None = None
    lineage_tables: tuple[str, ...] | None = None
    lineage_columns: tuple[str, ...] | None = None
    lineage_scopes: tuple[str, ...] | None = None
    param_signature: str | None = None
    projection_map: bytes | None = None
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
            "normalized_sql": self.normalized_sql,
            "explain": self.explain,
            "explain_analyze": self.explain_analyze,
            "substrait_b64": substrait_b64,
            "substrait_validation": (
                dict(self.substrait_validation) if self.substrait_validation is not None else None
            ),
            "sqlglot_ast": self.sqlglot_ast,
            "ibis_decompile": self.ibis_decompile,
            "ibis_sql": self.ibis_sql,
            "ibis_sql_pretty": self.ibis_sql_pretty,
            "ibis_graphviz": self.ibis_graphviz,
            "ibis_compiled_sql": self.ibis_compiled_sql,
            "ibis_compiled_sql_hash": self.ibis_compiled_sql_hash,
            "ibis_compile_params": self.ibis_compile_params,
            "ibis_compile_limit": self.ibis_compile_limit,
            "read_dialect": self.read_dialect,
            "write_dialect": self.write_dialect,
            "canonical_fingerprint": self.canonical_fingerprint,
            "lineage_tables": list(self.lineage_tables)
            if self.lineage_tables is not None
            else None,
            "lineage_columns": list(self.lineage_columns)
            if self.lineage_columns is not None
            else None,
            "lineage_scopes": list(self.lineage_scopes)
            if self.lineage_scopes is not None
            else None,
            "param_signature": self.param_signature,
            "projection_map": self.projection_map,
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
            "normalized_sql": self.normalized_sql,
            "explain": self.explain,
            "explain_analyze": self.explain_analyze,
            "substrait_b64": substrait_b64,
            "substrait_validation": self.substrait_validation,
            "sqlglot_ast": self.sqlglot_ast,
            "ibis_decompile": self.ibis_decompile,
            "ibis_sql": self.ibis_sql,
            "ibis_sql_pretty": self.ibis_sql_pretty,
            "ibis_graphviz": self.ibis_graphviz,
            "ibis_compiled_sql": self.ibis_compiled_sql,
            "ibis_compiled_sql_hash": self.ibis_compiled_sql_hash,
            "ibis_compile_params": self.ibis_compile_params,
            "ibis_compile_limit": self.ibis_compile_limit,
            "read_dialect": self.read_dialect,
            "write_dialect": self.write_dialect,
            "canonical_fingerprint": self.canonical_fingerprint,
            "lineage_tables": list(self.lineage_tables)
            if self.lineage_tables is not None
            else None,
            "lineage_columns": list(self.lineage_columns)
            if self.lineage_columns is not None
            else None,
            "lineage_scopes": list(self.lineage_scopes)
            if self.lineage_scopes is not None
            else None,
            "param_signature": self.param_signature,
            "projection_map": self.projection_map,
            "unparsed_sql": self.unparsed_sql,
            "unparse_error": self.unparse_error,
            "logical_plan": self.logical_plan,
            "optimized_plan": self.optimized_plan,
            "physical_plan": self.physical_plan,
            "graphviz": self.graphviz,
            "partition_count": self.partition_count,
            "join_operators": self.join_operators,
        }


def _bind_params_for_ast(
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> Expression:
    bindings = _validated_param_bindings(
        options.params,
        allowlist=options.param_identifier_allowlist,
    )
    if not bindings:
        msg = "SQL parameters require bindings for AST execution."
        raise ValueError(msg)
    try:
        return bind_params(expr, params=bindings)
    except (KeyError, TypeError, ValueError) as exc:
        msg = "SQL parameter binding failed for AST execution."
        raise ValueError(msg) from exc


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
    if resolved.named_params:
        msg = "Named parameters are not supported for DML execution."
        raise ValueError(msg)
    _ensure_dialect(resolved.dialect)
    expr = parse_sql_strict(
        sql,
        dialect=resolved.dialect,
        preserve_params=resolved.params is not None,
    )
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


def _write_format(value: str | None) -> WriteFormat:
    if value is None:
        return WriteFormat.PARQUET
    normalized = value.strip().lower()
    mapping = {
        "parquet": WriteFormat.PARQUET,
        "csv": WriteFormat.CSV,
        "json": WriteFormat.JSON,
        "arrow": WriteFormat.ARROW,
    }
    resolved = mapping.get(normalized)
    if resolved is None:
        msg = f"Unsupported write format: {value!r}."
        raise ValueError(msg)
    return resolved


def _write_pipeline_profile_for_dml(
    options: DataFusionDmlOptions | None,
) -> SQLPolicyProfile:
    dialect = options.dialect if options is not None else "datafusion_ext"
    _ensure_dialect(dialect)
    return SQLPolicyProfile(read_dialect=dialect, write_dialect=dialect)


def _sql_options_for_dml(
    options: DataFusionDmlOptions | None,
) -> SQLOptions | None:
    if options is None:
        return None
    if options.sql_options is not None:
        return options.sql_options
    policy = options.sql_policy or resolve_sql_policy(
        options.sql_policy_name,
        fallback=_default_sql_policy(),
    )
    merged = _merge_sql_policies(
        DataFusionSqlPolicy(allow_dml=True, allow_statements=True),
        options.session_policy,
        options.table_policy,
        policy,
    )
    return merged.to_sql_options()


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

    Raises
    ------
    ValueError
        If file output is disallowed by the COPY options.
    """
    resolved = options or CopyToOptions()
    if not resolved.allow_file_output:
        msg = "COPY TO file outputs are disabled; use Delta writes instead."
        raise ValueError(msg)
    profile = _write_pipeline_profile_for_dml(resolved.dml)
    request = WriteRequest(
        source=sql,
        destination=path,
        format=_write_format(resolved.file_format),
        mode=WriteMode.ERROR,
        partition_by=tuple(resolved.partition_by),
        format_options=(
            merge_format_options(
                session_defaults=resolved.session_defaults,
                table_options=resolved.table_options,
                statement_overrides=resolved.statement_overrides,
            )
            or None
        ),
    )
    pipeline = WritePipeline(
        ctx,
        profile,
        sql_options=_sql_options_for_dml(resolved.dml),
    )
    copy_sql, df = pipeline.copy_dataframe(request)
    if resolved.dml is not None and resolved.dml.record_hook is not None:
        param_mode = _param_mode(resolved.dml.params)
        resolved.dml.record_hook(
            {
                "sql": copy_sql,
                "dialect": profile.write_dialect,
                "policy_violations": [],
                "sql_policy_name": resolved.dml.sql_policy_name,
                "param_mode": param_mode,
            }
        )
    return df


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
    from sqlglot_tools.ddl_builders import build_copy_to_ast
    from sqlglot_tools.optimizer import parse_sql_strict, resolve_sqlglot_policy, sqlglot_emit

    policy = resolve_sqlglot_policy(name="datafusion_dml")
    query = parse_sql_strict(sql, dialect=policy.read_dialect)
    copy_expr = build_copy_to_ast(
        query=query,
        path=path,
        file_format=resolved.file_format,
        options=merged,
        partition_by=tuple(resolved.partition_by),
    )
    return sqlglot_emit(copy_expr, policy=policy)


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
    """Execute a SQLGlot expression with AST-first compilation.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame representing the expression.
    """
    return df_from_sqlglot_or_sql(ctx, expr, options=options)


def _maybe_explain(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> None:
    if not options.capture_explain and options.explain_hook is None:
        return
    _ensure_dialect(options.dialect)
    sql = _emit_sql(expr, options=options)
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


@dataclass(frozen=True)
class _PlanArtifactInputs:
    resolved_expr: Expression
    sql: str
    param_signature: str | None
    projection_map: bytes | None
    policy: SqlGlotPolicy
    details: _PlanArtifactsDetails
    plan_hash: str


def _collect_plan_artifact_inputs(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
    df: DataFrame | None,
) -> _PlanArtifactInputs:
    _ensure_dialect(options.dialect)
    bindings = _validated_param_bindings(
        options.params,
        allowlist=options.param_identifier_allowlist,
    )
    param_signature = scalar_param_signature(bindings) if bindings else None
    projection_map = _projection_map_payload(expr, options=options)
    policy = _sqlglot_emit_policy(options)
    resolved_expr = expr
    if bindings and _contains_params(expr):
        try:
            resolved_expr = bind_params(expr, params=bindings)
        except (KeyError, TypeError, ValueError):
            resolved_expr = expr
    sql = _emit_sql(resolved_expr, options=options)
    details = _collect_plan_artifacts_details(ctx, sql=sql, options=options, df=df)
    policy_hash = _sqlglot_policy_hash(options, policy=policy)
    plan_hash = options.plan_hash or plan_fingerprint(
        resolved_expr,
        dialect=options.dialect,
        policy_hash=policy_hash,
        schema_map_hash=options.schema_map_hash,
    )
    return _PlanArtifactInputs(
        resolved_expr=resolved_expr,
        sql=sql,
        param_signature=param_signature,
        projection_map=projection_map,
        policy=policy,
        details=details,
        plan_hash=plan_hash,
    )


def _collect_sqlglot_ast_artifact(
    resolved_expr: Expression,
    *,
    sql: str,
    policy: SqlGlotPolicy,
) -> bytes | None:
    try:
        return serialize_ast_artifact(ast_to_artifact(resolved_expr, sql=sql, policy=policy))
    except (RuntimeError, TypeError, ValueError, msgspec.EncodeError):
        return None


def _collect_lineage_payload(
    resolved_expr: Expression,
    *,
    options: DataFusionCompileOptions,
    policy: SqlGlotPolicy,
) -> LineagePayload | None:
    try:
        return extract_lineage_payload(
            resolved_expr,
            options=LineageExtractionOptions(
                schema=_lineage_schema_map(options.schema_map),
                dialect=policy.write_dialect,
            ),
        )
    except (RuntimeError, TypeError, ValueError):
        return None


def _collect_normalized_sql(
    resolved_expr: Expression,
    *,
    sql: str,
    options: DataFusionCompileOptions,
    policy: SqlGlotPolicy,
) -> str | None:
    try:
        normalize_options = NormalizeExprOptions(
            schema=options.schema_map,
            policy=policy,
            enable_rewrites=options.enable_rewrites,
            rewrite_hook=options.rewrite_hook,
            sql=sql,
        )
        normalized_expr = normalize_expr(resolved_expr, options=normalize_options)
        return sqlglot_sql(normalized_expr, policy=policy)
    except (RuntimeError, TypeError, ValueError):
        return None


def _collect_ibis_artifacts(
    options: DataFusionCompileOptions,
) -> Mapping[str, object] | None:
    ibis_expr = options.ibis_expr
    if ibis_expr is None:
        return None
    from ibis.expr.types import Table, Value

    if not isinstance(ibis_expr, (Table, Value)):
        return None
    from ibis_engine.sql_bridge import ibis_plan_artifacts

    return ibis_plan_artifacts(
        ibis_expr,
        dialect=options.dialect,
        params=options.params,
    )


def _payload_str(payload: Mapping[str, object] | None, key: str) -> str | None:
    if payload is None:
        return None
    value = payload.get(key)
    if value is None:
        return None
    return str(value)


def _payload_int(payload: Mapping[str, object] | None, key: str) -> int | None:
    if payload is None:
        return None
    value = payload.get(key)
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, (float, str, bytes, bytearray)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


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
    inputs = _collect_plan_artifact_inputs(ctx, expr, options=options, df=df)
    sqlglot_ast = _collect_sqlglot_ast_artifact(
        inputs.resolved_expr,
        sql=inputs.sql,
        policy=inputs.policy,
    )
    lineage_payload = _collect_lineage_payload(
        inputs.resolved_expr,
        options=options,
        policy=inputs.policy,
    )
    canonical_fingerprint = (
        lineage_payload.canonical_fingerprint
        if lineage_payload is not None
        else canonical_ast_fingerprint(inputs.resolved_expr)
    )
    normalized_sql = _collect_normalized_sql(
        inputs.resolved_expr,
        sql=inputs.sql,
        options=options,
        policy=inputs.policy,
    )
    ibis_payload = _collect_ibis_artifacts(options)
    return DataFusionPlanArtifacts(
        plan_hash=inputs.plan_hash,
        sql=inputs.sql,
        normalized_sql=normalized_sql,
        explain=inputs.details.explain_rows,
        explain_analyze=inputs.details.analyze_rows,
        substrait_plan=inputs.details.substrait_plan,
        substrait_validation=inputs.details.substrait_validation,
        sqlglot_ast=sqlglot_ast,
        ibis_decompile=_payload_str(ibis_payload, "ibis_decompile"),
        ibis_sql=_payload_str(ibis_payload, "ibis_sql"),
        ibis_sql_pretty=_payload_str(ibis_payload, "ibis_sql_pretty"),
        ibis_graphviz=_payload_str(ibis_payload, "ibis_graphviz"),
        ibis_compiled_sql=_payload_str(ibis_payload, "ibis_compiled_sql"),
        ibis_compiled_sql_hash=_payload_str(ibis_payload, "ibis_compiled_sql_hash"),
        ibis_compile_params=_payload_str(ibis_payload, "ibis_compile_params"),
        ibis_compile_limit=_payload_int(ibis_payload, "ibis_compile_limit"),
        read_dialect=inputs.policy.read_dialect,
        write_dialect=inputs.policy.write_dialect,
        canonical_fingerprint=canonical_fingerprint,
        lineage_tables=lineage_payload.tables if lineage_payload is not None else None,
        lineage_columns=lineage_payload.columns if lineage_payload is not None else None,
        lineage_scopes=lineage_payload.scopes if lineage_payload is not None else None,
        param_signature=inputs.param_signature,
        projection_map=inputs.projection_map,
        unparsed_sql=inputs.details.unparsed_sql,
        unparse_error=inputs.details.unparse_error,
        logical_plan=cast("str | None", inputs.details.plan_details.get("logical_plan")),
        optimized_plan=cast("str | None", inputs.details.plan_details.get("optimized_plan")),
        physical_plan=cast("str | None", inputs.details.plan_details.get("physical_plan")),
        graphviz=cast("str | None", inputs.details.plan_details.get("graphviz")),
        partition_count=cast("int | None", inputs.details.plan_details.get("partition_count")),
        join_operators=cast(
            "tuple[str, ...] | None",
            inputs.details.plan_details.get("join_operators"),
        ),
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

    Raises
    ------
    ValueError
        Raised when named parameters are invalid or violate read-only policy.
    """
    _ensure_dialect(options.dialect)
    named_params = _validated_named_params(
        options.named_params,
        allowlist=options.param_identifier_allowlist,
    )
    bindings = _validated_param_bindings(
        options.params,
        allowlist=options.param_identifier_allowlist,
    )
    expr = parse_sql_strict(
        sql,
        dialect=options.dialect,
        preserve_params=bool(bindings),
    )
    resolved = replace(options, params=bindings or None, named_params=named_params or None)
    if named_params:
        read_only_policy = DataFusionSqlPolicy()
        violations = _policy_violations(expr, read_only_policy)
        if violations:
            msg = f"Named parameter SQL must be read-only: {', '.join(violations)}."
            raise ValueError(msg)
        resolved = replace(
            resolved,
            sql_options=_sql_options_for_named_params(resolved),
            sql_policy=read_only_policy,
            sql_policy_name=None,
        )
        _emit_named_param_diagnostics(resolved, params=named_params)
    df = df_from_sqlglot_or_sql(ctx, expr, options=resolved)
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
    if policy.parquet_bloom_filter_on_write is not None:
        parquet_kwargs["bloom_filter_on_write"] = policy.parquet_bloom_filter_on_write
    if policy.parquet_dictionary_enabled is not None:
        parquet_kwargs["dictionary_enabled"] = policy.parquet_dictionary_enabled
    if policy.parquet_encoding is not None:
        parquet_kwargs["encoding"] = policy.parquet_encoding
    if policy.parquet_skip_arrow_metadata is not None:
        parquet_kwargs["skip_arrow_metadata"] = policy.parquet_skip_arrow_metadata
    column_options = _column_options(policy.parquet_column_options)
    if column_options:
        parquet_kwargs["column_specific_options"] = column_options
    parquet_options = (
        ParquetWriterOptions(**parquet_kwargs) if parquet_kwargs else ParquetWriterOptions()
    )
    return write_options, parquet_options


def datafusion_write_parquet(
    df: DataFrame,
    *,
    path: str,
    policy: DataFusionWritePolicy | None = None,
    ctx: SessionContext | None = None,
) -> dict[str, object]:
    """Write a DataFusion DataFrame to Parquet using policy options.

    Returns
    -------
    dict[str, object]
        Payload describing the write policy applied.
    """
    if ctx is None:
        return _direct_parquet_write(df, path=path, policy=policy)
    sql = datafusion_view_sql(df)
    if sql is None:
        return _direct_parquet_write(df, path=path, policy=policy)
    return _pipeline_write_parquet(
        ctx,
        df=df,
        sql=sql,
        path=path,
        policy=policy,
    )


def _direct_parquet_write(
    df: DataFrame,
    *,
    path: str,
    policy: DataFusionWritePolicy | None,
) -> dict[str, object]:
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


def _available_column_names(df: DataFrame) -> set[str] | None:
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        return set(schema.names)
    return None


def _policy_columns(
    names: Sequence[str] | None,
    *,
    available: set[str] | None,
) -> tuple[str, ...]:
    if not names:
        return ()
    if available is None:
        return tuple(names)
    return tuple(name for name in names if name in available)


def _build_sorted_source(
    sql: str,
    *,
    sort_by: tuple[str, ...],
    profile: SQLPolicyProfile,
) -> str | exp.Expression:
    if not sort_by:
        return sql
    try:
        parsed = parse_one(sql, dialect=profile.read_dialect)
    except SqlglotError:
        return sql
    subquery = exp.Subquery(this=parsed)
    source_expr = exp.select("*").from_(subquery)
    order_exprs = [exp.Ordered(this=exp.column(name)) for name in sort_by]
    source_expr.set("order", exp.Order(expressions=order_exprs))
    return source_expr


def _pipeline_write_parquet(
    ctx: SessionContext,
    *,
    df: DataFrame,
    sql: str,
    path: str,
    policy: DataFusionWritePolicy | None,
) -> dict[str, object]:
    profile = _write_pipeline_profile_for_dml(None)
    available = _available_column_names(df)
    partition_by = _policy_columns(
        policy.partition_by if policy is not None else None,
        available=available,
    )
    sort_by = _policy_columns(
        policy.sort_by if policy is not None else None,
        available=available,
    )
    source = _build_sorted_source(sql, sort_by=sort_by, profile=profile)
    parquet_policy = parquet_policy_from_datafusion(policy)
    request = WriteRequest(
        source=source,
        destination=path,
        format=WriteFormat.PARQUET,
        mode=WriteMode.OVERWRITE,
        partition_by=partition_by,
        parquet_policy=parquet_policy,
        single_file_output=policy.single_file_output if policy is not None else None,
    )
    pipeline = WritePipeline(ctx, profile)
    pipeline.write(request, prefer_streaming=True)
    return {
        "path": str(path),
        "write_policy": policy.payload() if policy is not None else None,
        "parquet_options": (
            parquet_policy.to_copy_options() if parquet_policy is not None else None
        ),
    }


def _datafusion_sort_exprs(sort_by: Sequence[str]) -> list[SortExpr]:
    return [col(name).sort(ascending=True, nulls_first=True) for name in sort_by]


def _parquet_options_payload(options: ParquetWriterOptions | None) -> dict[str, object] | None:
    if options is None:
        return None
    column_options = getattr(options, "column_specific_options", None)
    column_payload = (
        {name: _parquet_column_options_payload(option) for name, option in column_options.items()}
        if isinstance(column_options, Mapping)
        else None
    )
    return {
        "compression": getattr(options, "compression", None),
        "statistics_enabled": getattr(options, "statistics_enabled", None),
        "max_row_group_size": getattr(options, "max_row_group_size", None),
        "bloom_filter_on_write": getattr(options, "bloom_filter_on_write", None),
        "dictionary_enabled": getattr(options, "dictionary_enabled", None),
        "encoding": getattr(options, "encoding", None),
        "skip_arrow_metadata": getattr(options, "skip_arrow_metadata", None),
        "column_specific_options": column_payload,
    }


class _ParquetWriterOptionsKwargs(TypedDict, total=False):
    compression: str | None
    statistics_enabled: str | None
    max_row_group_size: int
    bloom_filter_on_write: bool
    dictionary_enabled: bool
    encoding: str
    skip_arrow_metadata: bool
    column_specific_options: dict[str, ParquetColumnOptions]


class _ParquetColumnOptionsKwargs(TypedDict, total=False):
    compression: str | None
    dictionary_enabled: bool
    statistics_enabled: str | None
    bloom_filter_enabled: bool
    bloom_filter_fpp: float
    bloom_filter_ndv: int
    encoding: str


def _column_options(
    options: Mapping[str, ParquetColumnPolicy] | None,
) -> dict[str, ParquetColumnOptions] | None:
    if not options:
        return None
    resolved: dict[str, ParquetColumnOptions] = {}
    for name, spec in options.items():
        kwargs: _ParquetColumnOptionsKwargs = {}
        if spec.compression is not None:
            kwargs["compression"] = spec.compression
        if spec.dictionary_enabled is not None:
            kwargs["dictionary_enabled"] = spec.dictionary_enabled
        if spec.statistics_enabled is not None:
            kwargs["statistics_enabled"] = spec.statistics_enabled
        if spec.bloom_filter_enabled is not None:
            kwargs["bloom_filter_enabled"] = spec.bloom_filter_enabled
        if spec.bloom_filter_fpp is not None:
            kwargs["bloom_filter_fpp"] = spec.bloom_filter_fpp
        if spec.bloom_filter_ndv is not None:
            kwargs["bloom_filter_ndv"] = spec.bloom_filter_ndv
        if spec.encoding is not None:
            kwargs["encoding"] = spec.encoding
        resolved[name] = ParquetColumnOptions(**kwargs)
    return resolved


def _parquet_column_options_payload(options: ParquetColumnOptions) -> dict[str, object]:
    return {
        "compression": getattr(options, "compression", None),
        "dictionary_enabled": getattr(options, "dictionary_enabled", None),
        "statistics_enabled": getattr(options, "statistics_enabled", None),
        "bloom_filter_enabled": getattr(options, "bloom_filter_enabled", None),
        "bloom_filter_fpp": getattr(options, "bloom_filter_fpp", None),
        "bloom_filter_ndv": getattr(options, "bloom_filter_ndv", None),
        "encoding": getattr(options, "encoding", None),
    }


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


@dataclass(frozen=True)
class DeltaInsertOptions:
    """Options for DataFusion INSERT INTO Delta tables."""

    mode: Literal["append", "overwrite"] = "append"
    table_name: str | None = None
    constraints: Sequence[str] = ()


@dataclass(frozen=True)
class DeltaInsertResult:
    """Result of DataFusion INSERT operation."""

    table_name: str
    mode: str
    rows_affected: int | None = None


def datafusion_insert_delta(
    ctx: SessionContext,
    table_name: str,
    source_sql: str,
    *,
    options: DeltaInsertOptions | None = None,
) -> DeltaInsertResult:
    """Execute INSERT INTO Delta table via DataFusion.

    This uses DataFusion's DML support against DeltaTableProvider for
    append/overwrite operations without materializing to Arrow first.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context with Delta table registered.
    table_name : str
        Name of registered Delta table to insert into.
    source_sql : str
        SQL SELECT query providing the data to insert.
    options : DeltaInsertOptions | None
        Insert options (mode, etc).

    Returns
    -------
    DeltaInsertResult
        Result with table name and mode.

    Raises
    ------
    ValueError
        Raised when the insert mode is unsupported.
    """
    resolved = options or DeltaInsertOptions()
    if resolved.mode not in {"overwrite", "append"}:
        msg = f"Unsupported Delta INSERT mode: {resolved.mode!r}."
        raise ValueError(msg)
    source_expr = _parse_sql_expr(source_sql)
    if source_expr is None:
        if resolved.mode == "overwrite":
            sql = f"INSERT OVERWRITE {table_name} {source_sql}"
        else:
            sql = f"INSERT INTO {table_name} {source_sql}"
    else:
        insert_expr = build_insert(
            source_expr,
            table_name=table_name,
            overwrite=resolved.mode == "overwrite",
        )
        sql = _emit_internal_sql(insert_expr)

    if resolved.constraints:
        _validate_sql_constraints(
            ctx,
            source_sql=source_sql,
            constraints=resolved.constraints,
        )

    # Use DML-enabled SQL options
    allow_dml = True
    sql_options = SQLOptions().with_allow_dml(allow_dml)

    # Execute the INSERT
    df = ctx.sql_with_options(sql, sql_options)
    batches = df.collect()

    # Try to extract rows affected from result
    rows_affected = None
    if batches:
        first_batch = batches[0]
        if first_batch.num_columns > 0 and first_batch.num_rows > 0:
            # DataFusion returns count in first column
            with contextlib.suppress(IndexError, TypeError, ValueError):
                rows_affected = int(first_batch.column(0)[0].as_py())

    return DeltaInsertResult(
        table_name=table_name,
        mode=resolved.mode,
        rows_affected=rows_affected,
    )


def _validate_sql_constraints(
    ctx: SessionContext,
    *,
    source_sql: str,
    constraints: Sequence[str],
) -> None:
    if not constraints:
        return
    sql_options = SQLOptions()
    for constraint in constraints:
        if not constraint.strip():
            continue
        source_expr = _parse_sql_expr(source_sql)
        constraint_expr = _parse_sql_expr(constraint)
        if source_expr is None or constraint_expr is None:
            query = f"SELECT 1 FROM ({source_sql}) AS input WHERE NOT ({constraint}) LIMIT 1"
        else:
            subquery = exp.Subquery(
                this=source_expr,
                alias=exp.TableAlias(this=exp.to_identifier("input")),
            )
            query_expr = build_select(
                [exp.Literal.number(1)],
                from_=subquery,
                where=exp.not_(constraint_expr),
                limit=1,
            )
            query = _emit_internal_sql(query_expr)
        df = ctx.sql_with_options(query, sql_options)
        if _df_has_rows(df):
            msg = f"Delta constraint violated: {constraint}"
            raise ValueError(msg)


def datafusion_insert_from_dataframe(
    ctx: SessionContext,
    table_name: str,
    source_df: DataFrame,
    *,
    options: DeltaInsertOptions | None = None,
) -> DeltaInsertResult:
    """Execute INSERT INTO Delta table from a DataFusion DataFrame.

    This registers the source DataFrame as a temporary view and executes
    INSERT INTO against it.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context with Delta table registered.
    table_name : str
        Name of registered Delta table to insert into.
    source_df : DataFrame
        DataFrame to insert.
    options : DeltaInsertOptions | None
        Insert options (mode, etc).

    Returns
    -------
    DeltaInsertResult
        Result with table name, mode, and rows affected.

    Raises
    ------
    TypeError
        Raised when the SessionContext cannot register record batches.
    """
    resolved = options or DeltaInsertOptions()

    # Register source as temp view using Arrow materialization
    temp_name = f"__insert_source_{_uuid_short()}"
    batches = source_df.collect()
    register_batches = getattr(ctx, "register_record_batches", None)
    if not callable(register_batches):
        msg = "DataFusion SessionContext missing register_record_batches."
        raise TypeError(msg)
    register_batches(temp_name, batches)

    try:
        source_expr = build_select([exp.Star()], from_=temp_name)
        source_sql = _emit_internal_sql(source_expr)
        result = datafusion_insert_delta(
            ctx,
            table_name,
            source_sql,
            options=resolved,
        )
    finally:
        # Deregister temp view
        with contextlib.suppress(RuntimeError, TypeError, ValueError):
            ctx.deregister_table(temp_name)

    return result


def _uuid_short() -> str:
    """Generate a short UUID for temporary table names.

    Returns
    -------
    str
        Short hexadecimal UUID segment.
    """
    return uuid.uuid4().hex[:8]


def df_from_sqlglot_ast(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions | None = None,
) -> DataFrame:
    """Create DataFrame from SQLGlot AST without string serialization.

    This executes the AST via the Ibis DataFusion backend and materializes
    results into a DataFusion DataFrame for downstream use.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context.
    expr : Expression
        SQLGlot expression to execute.
    options : DataFusionCompileOptions | None
        Compilation options controlling dialect and SQL generation.

    Returns
    -------
    DataFrame
        DataFusion DataFrame for the expression.

    Raises
    ------
    RuntimeError
        Raised when the Ibis DataFusion backend is unavailable.
    ValueError
        Raised when the AST execution returns no column metadata.
    """
    resolved = options or DataFusionCompileOptions()
    _ensure_dialect(resolved.dialect)
    try:
        import ibis
    except ImportError as exc:
        msg = "AST execution requires the Ibis DataFusion backend."
        raise RuntimeError(msg) from exc
    backend = ibis.datafusion.connect(ctx=ctx)
    result = execute_sqlglot_ast(
        cast("IbisCompilerBackend", backend),
        expr,
        dialect=resolved.dialect,
    )
    columns = result.columns
    if not columns:
        msg = "AST execution returned no column metadata."
        raise ValueError(msg)
    rows = result.rows or []
    if rows:
        row_dicts = [dict(zip(columns, row, strict=True)) for row in rows]
        table = table_from_row_dicts(row_dicts)
    else:
        table = pa.table({name: [] for name in columns})
    return datafusion_from_arrow(ctx, name=f"__ast_result_{_uuid_short()}", value=table)


__all__ = [
    "AstExecutionResult",
    "CopyToOptions",
    "DataFusionCompileOptions",
    "DataFusionDmlOptions",
    "DataFusionPlanArtifacts",
    "DataFusionSqlPolicy",
    "DeltaInsertOptions",
    "DeltaInsertResult",
    "DualLaneCompilationResult",
    "IbisCompilerBackend",
    "MaterializationPolicy",
    "collect_plan_artifacts",
    "compile_sqlglot_expr",
    "copy_to_path",
    "copy_to_statement",
    "datafusion_from_arrow",
    "datafusion_insert_delta",
    "datafusion_insert_from_dataframe",
    "datafusion_partitioned_readers",
    "datafusion_read_table",
    "datafusion_to_async_batches",
    "datafusion_to_reader",
    "datafusion_to_table",
    "datafusion_view_sql",
    "datafusion_write_options",
    "datafusion_write_parquet",
    "df_from_sql",
    "df_from_sqlglot_ast",
    "execute_dml",
    "execute_prepared_statement",
    "execute_sql",
    "execute_sqlglot_ast",
    "ibis_plan_to_datafusion",
    "ibis_plan_to_reader",
    "ibis_plan_to_table",
    "ibis_to_datafusion",
    "ibis_to_datafusion_ast_path",
    "ibis_to_datafusion_dual_lane",
    "merge_format_options",
    "prepare_statement",
    "rehydrate_plan_artifacts",
    "replay_substrait_bytes",
    "sqlglot_to_datafusion",
    "validate_substrait_plan",
    "validate_table_constraints",
]
