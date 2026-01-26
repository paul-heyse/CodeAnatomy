"""Execution helper utilities for DataFusion compilation and diagnostics."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import logging
import re
import time
import uuid
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, TypedDict, cast

import msgspec
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
    ParquetColumnOptions,
    ParquetWriterOptions,
    SessionContext,
    SQLOptions,
)
from datafusion.dataframe import DataFrame
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.interop import (
    RecordBatchReaderLike,
    TableLike,
)
from arrowdsl.core.streaming import to_reader
from datafusion_engine.compile_options import (
    DataFusionCacheEvent,
    DataFusionCompileOptions,
    DataFusionDmlOptions,
    DataFusionSqlPolicy,
    ExplainRows,
    resolve_sql_policy,
)
from datafusion_engine.compile_pipeline import CompilationPipeline, CompileOptions
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.param_binding import resolve_param_bindings
from datafusion_engine.registry_bridge import (
    apply_projection_overrides,
    apply_projection_scan_overrides,
)
from datafusion_engine.sql_policy_engine import SQLPolicyProfile
from datafusion_engine.sql_safety import ExecutionPolicy
from engine.plan_cache import PlanCacheKey
from ibis_engine.param_tables import scalar_param_signature
from schema_spec.policies import DataFusionWritePolicy
from serde_msgspec import dumps_msgpack
from sqlglot_tools.bridge import IbisCompilerBackend
from sqlglot_tools.compat import ErrorLevel, Expression, exp
from sqlglot_tools.lineage import (
    LineageExtractionOptions,
    LineagePayload,
    canonical_ast_fingerprint,
    extract_lineage_payload,
)
from sqlglot_tools.optimizer import (
    PreflightOptions,
    SchemaMapping,
    SqlGlotPolicy,
    ast_policy_fingerprint,
    ast_to_artifact,
    bind_params,
    build_select,
    emit_preflight_diagnostics,
    parse_sql_strict,
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
class CopyToOptions:
    """Options for COPY TO format precedence and DML execution."""

    session_defaults: Mapping[str, object] | None = None
    table_options: Mapping[str, object] | None = None
    statement_overrides: Mapping[str, object] | None = None
    file_format: str | None = None
    partition_by: Sequence[str] = ()
    dml: DataFusionDmlOptions | None = None
    allow_file_output: bool = False


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


def _policy_hash_for_options(options: DataFusionCompileOptions) -> str:
    if options.sqlglot_policy_hash is not None:
        return options.sqlglot_policy_hash
    if options.sqlglot_policy is not None:
        policy = options.sqlglot_policy
    elif options.sql_policy_profile is not None:
        policy = options.sql_policy_profile.to_sqlglot_policy()
    else:
        policy = resolve_sqlglot_policy(name="datafusion_compile")
    return sqlglot_policy_snapshot_for(policy).policy_hash


def _plan_hash_for_expr(expr: Expression, *, options: DataFusionCompileOptions) -> str:
    ast_fingerprint = canonical_ast_fingerprint(expr)
    policy_hash = _policy_hash_for_options(options)
    return ast_policy_fingerprint(ast_fingerprint=ast_fingerprint, policy_hash=policy_hash)


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
    if not options.diagnostics_allow_sql:
        return None
    ast_fingerprint = canonical_ast_fingerprint(expr)
    policy_hash = _policy_hash_for_options(options)
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
        ast_fingerprint=ast_fingerprint,
        policy_hash=policy_hash,
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
    from datafusion_engine.streaming_executor import StreamingExecutionResult

    try:
        df_reader = StreamingExecutionResult(df=df).to_arrow_stream()
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


def _execution_policy_from_sql_policy(policy: DataFusionSqlPolicy) -> ExecutionPolicy:
    return ExecutionPolicy(
        allow_ddl=policy.allow_ddl,
        allow_dml=policy.allow_dml,
        allow_statements=policy.allow_statements,
    )


def _execution_policy_for_options(options: DataFusionCompileOptions) -> ExecutionPolicy:
    policy = options.sql_policy or resolve_sql_policy(
        options.sql_policy_name,
        fallback=_default_sql_policy(),
    )
    return _execution_policy_from_sql_policy(policy)


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
        base = SQLPolicyProfile(policy=options.sqlglot_policy)
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


def _emit_sql(expr: Expression, *, options: DataFusionCompileOptions) -> str:
    policy = _sqlglot_emit_policy(options)
    return sqlglot_emit(expr, policy=policy)


def _emit_internal_sql(expr: Expression) -> str:
    return _emit_sql(expr, options=DataFusionCompileOptions())


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
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_record_batches(name, [table.to_batches()])
    violations: list[str] = []
    sql_options = _default_sql_policy().to_sql_options()
    policy = resolve_sqlglot_policy(name="datafusion_compile")
    options = DataFusionCompileOptions()
    pipeline = _compilation_pipeline(ctx, options=options)
    try:
        for constraint in constraints:
            if not constraint.strip():
                continue
            constraint_expr = parse_sql_strict(constraint, dialect=policy.read_dialect)
            query_expr = build_select(
                [exp.Literal.number(1)],
                from_=name,
                where=exp.not_(constraint_expr),
                limit=1,
            )
            compiled = pipeline.compile_ast(query_expr)
            df = pipeline.execute(compiled, sql_options=sql_options)
            if _df_has_rows(df):
                violations.append(constraint)
    finally:
        adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(name)
    return violations


def _df_has_rows(df: DataFrame) -> bool:
    from datafusion_engine.streaming_executor import StreamingExecutionResult

    reader = StreamingExecutionResult(df=df).to_arrow_stream()
    for batch in reader:
        return batch.num_rows > 0
    return False


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
    df: DataFrame | None
    explain_rows: ExplainRows | None
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
    substrait_plan_override: bytes | None = None,
) -> _PlanArtifactsDetails:
    _ = ctx
    plan_df = df
    explain_rows: ExplainRows | None = None
    analyze_rows: ExplainRows | None = None
    substrait_plan = substrait_plan_override
    substrait_validation = None
    if substrait_plan is not None and options.substrait_validation and plan_df is not None:
        substrait_validation = _substrait_validation_payload(
            substrait_plan,
            df=plan_df,
        )
    plan_details = _df_plan_details(plan_df) if plan_df is not None else {}
    unparsed_sql = None
    unparse_error = None
    if plan_df is not None:
        unparse_plan = _df_plan(plan_df, "optimized_logical_plan") or _df_plan(
            plan_df,
            "logical_plan",
        )
        unparsed_sql, unparse_error = _unparse_plan(unparse_plan)
    elif options.diagnostics_allow_sql:
        unparsed_sql = sql
        unparse_error = "plan_df_missing"
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
    explain: TableLike | RecordBatchReaderLike | None
    explain_analyze: TableLike | RecordBatchReaderLike | None
    substrait_plan: bytes | None
    policy_hash: str | None = None
    ast_fingerprint: str | None = None
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
            "policy_hash": self.policy_hash,
            "ast_fingerprint": self.ast_fingerprint,
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
            "policy_hash": self.policy_hash,
            "ast_fingerprint": self.ast_fingerprint,
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
    bindings = resolve_param_bindings(
        options.params,
        allowlist=options.param_identifier_allowlist,
    )
    if bindings.named_tables:
        msg = "AST execution does not support table-like parameters."
        raise ValueError(msg)
    if not bindings.param_values:
        msg = "SQL parameters require bindings for AST execution."
        raise ValueError(msg)
    try:
        return bind_params(expr, params=bindings.param_values)
    except (KeyError, TypeError, ValueError) as exc:
        msg = "SQL parameter binding failed for AST execution."
        raise ValueError(msg) from exc


def _maybe_explain(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> None:
    if not options.capture_explain and options.explain_hook is None:
        return
    _ = (ctx, expr, options)
    return


def _maybe_collect_plan_artifacts(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
    df: DataFrame,
    substrait_plan_override: bytes | None = None,
) -> None:
    if (
        not options.capture_plan_artifacts
        and options.plan_artifacts_hook is None
        and not options.substrait_validation
    ):
        return
    resolved = options
    if substrait_plan_override is not None:
        resolved = replace(options, substrait_plan_override=substrait_plan_override)
    try:
        artifacts = collect_plan_artifacts(
            ctx, expr, options=resolved, df=df, run_id=options.run_id
        )
    except (RuntimeError, TypeError, ValueError):
        return
    if options.plan_artifacts_hook is not None:
        options.plan_artifacts_hook(artifacts.structured_explain_payload())


def _semantic_plan_hash(expr: Expression, *, options: DataFusionCompileOptions) -> str:
    return _plan_hash_for_expr(expr, options=options)


def _semantic_diff_base_expr(
    ctx: SessionContext,
    *,
    options: DataFusionCompileOptions,
) -> Expression | None:
    if options.semantic_diff_base_expr is None and options.semantic_diff_base_sql is None:
        return None
    from datafusion_engine.execution_facade import DataFusionExecutionFacade

    base_options = replace(options, dynamic_projection=False)
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=base_options.runtime_profile)
    if options.semantic_diff_base_expr is not None:
        plan = facade.compile(options.semantic_diff_base_expr, options=base_options)
        return plan.compiled.sqlglot_ast
    if options.semantic_diff_base_sql is None:
        return None
    from sqlglot.errors import ParseError

    from sqlglot_tools.optimizer import parse_sql_strict, register_datafusion_dialect

    try:
        register_datafusion_dialect()
        base_expr = parse_sql_strict(options.semantic_diff_base_sql, dialect=base_options.dialect)
    except (ParseError, TypeError, ValueError) as exc:
        msg = "Semantic diff base SQL parse failed."
        raise ValueError(msg) from exc
    plan = facade.compile(base_expr, options=base_options)
    return plan.compiled.sqlglot_ast


def _maybe_collect_semantic_diff(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions,
) -> None:
    hook = options.semantic_diff_hook
    if hook is None:
        return
    base_expr = _semantic_diff_base_expr(ctx, options=options)
    if base_expr is None:
        return
    from datafusion_engine.semantic_diff import ChangeCategory, SemanticDiff

    diff = SemanticDiff.compute(base_expr, expr)
    plan_hash = options.plan_hash or _semantic_plan_hash(expr, options=options)
    base_hash = _semantic_plan_hash(base_expr, options=options)
    row_multiplying = diff.overall_category == ChangeCategory.ROW_MULTIPLYING
    breaking = diff.overall_category in {ChangeCategory.BREAKING, ChangeCategory.ROW_MULTIPLYING}
    change_count = len(
        [change for change in diff.changes if change.category != ChangeCategory.NONE]
    )
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "run_id": options.run_id,
        "plan_hash": plan_hash,
        "base_plan_hash": base_hash,
        "category": diff.overall_category.name.lower(),
        "changed": diff.overall_category != ChangeCategory.NONE,
        "breaking": breaking,
        "row_multiplying": row_multiplying,
        "change_count": change_count,
    }
    hook(payload)


def _explain_reader(df: DataFrame) -> pa.RecordBatchReader:
    from datafusion_engine.streaming_executor import StreamingExecutionResult

    return StreamingExecutionResult(df=df).to_arrow_stream()


def _collect_reader(df: DataFrame) -> pa.RecordBatchReader:
    from datafusion_engine.streaming_executor import StreamingExecutionResult

    return StreamingExecutionResult(df=df).to_arrow_stream()


def datafusion_to_reader(df: DataFrame) -> pa.RecordBatchReader:
    """Return a RecordBatchReader for a DataFusion DataFrame.

    Parameters
    ----------
    df : datafusion.dataframe.DataFrame
        DataFusion DataFrame to stream.

    Returns
    -------
    pyarrow.RecordBatchReader
        Arrow stream for the DataFusion result.
    """
    return _collect_reader(df)


async def datafusion_to_async_batches(df: DataFrame) -> AsyncIterator[pa.RecordBatch]:
    """Yield DataFusion record batches asynchronously.

    Parameters
    ----------
    df : datafusion.dataframe.DataFrame
        DataFusion DataFrame to stream.

    Yields
    ------
    pyarrow.RecordBatch
        Record batches from the DataFusion result.
    """
    reader = datafusion_to_reader(df)
    for batch in reader:
        yield batch
        await asyncio.sleep(0)


def df_from_sqlglot_or_sql(
    ctx: SessionContext,
    expr: Expression,
    *,
    options: DataFusionCompileOptions | None = None,
) -> DataFrame:
    """Compile SQLGlot input and return a DataFusion DataFrame.

    Parameters
    ----------
    ctx : datafusion.SessionContext
        DataFusion session context used for compilation and execution.
    expr : sqlglot.Expression
        SQLGlot expression to compile.
    options : DataFusionCompileOptions | None
        Optional compile options to override defaults.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the compiled expression.

    Raises
    ------
    TypeError
        Raised when expr is not a SQLGlot expression.
    """
    resolved = options or DataFusionCompileOptions()
    pipeline = _compilation_pipeline(ctx, options=resolved)
    if isinstance(expr, exp.Expression):
        compiled = pipeline.compile_ast(expr)
    else:
        msg = "Expected SQLGlot expression."
        raise TypeError(msg)
    return pipeline.execute(
        compiled,
        params=resolved.params,
        named_params=resolved.named_params,
        sql_options=resolved.sql_options,
    )


def datafusion_write_options(
    policy: DataFusionWritePolicy | None,
) -> tuple[DataFrameWriteOptions, ParquetWriterOptions]:
    """Build DataFusion write option objects from a policy.

    Parameters
    ----------
    policy : DataFusionWritePolicy | None
        DataFusion write policy to convert.

    Returns
    -------
    tuple[DataFrameWriteOptions, ParquetWriterOptions]
        DataFusion write options and Parquet writer options.
    """
    resolved = policy or DataFusionWritePolicy()
    write_options = DataFrameWriteOptions(
        partition_by=list(resolved.partition_by),
        sort_by=None,
        single_file_output=resolved.single_file_output,
    )
    column_options: dict[str, ParquetColumnOptions] = {}
    if resolved.parquet_column_options:
        for name, option in resolved.parquet_column_options.items():
            column_options[name] = ParquetColumnOptions(
                compression=option.compression,
                dictionary_enabled=option.dictionary_enabled,
                statistics_enabled=option.statistics_enabled,
                bloom_filter_enabled=option.bloom_filter_enabled,
                bloom_filter_fpp=option.bloom_filter_fpp,
                bloom_filter_ndv=option.bloom_filter_ndv,
                encoding=option.encoding,
            )
    parquet_options = ParquetWriterOptions(
        compression=resolved.parquet_compression or "zstd",
        statistics_enabled=resolved.parquet_statistics_enabled or "page",
        max_row_group_size=resolved.parquet_row_group_size or 1_000_000,
        bloom_filter_on_write=resolved.parquet_bloom_filter_on_write or False,
        dictionary_enabled=resolved.parquet_dictionary_enabled
        if resolved.parquet_dictionary_enabled is not None
        else True,
        encoding=resolved.parquet_encoding,
        skip_arrow_metadata=resolved.parquet_skip_arrow_metadata or False,
        column_specific_options=column_options or None,
    )
    return write_options, parquet_options


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
    bindings = resolve_param_bindings(
        options.params,
        allowlist=options.param_identifier_allowlist,
    )
    param_signature = (
        scalar_param_signature(bindings.param_values) if bindings.param_values else None
    )
    projection_map = _projection_map_payload(expr, options=options)
    policy = _sqlglot_emit_policy(options)
    resolved_expr = expr
    if bindings.param_values and _contains_params(expr):
        try:
            resolved_expr = bind_params(expr, params=bindings.param_values)
        except (KeyError, TypeError, ValueError):
            resolved_expr = expr
    sql = _emit_sql(resolved_expr, options=options)
    details = _collect_plan_artifacts_details(
        ctx,
        sql=sql,
        options=options,
        df=df,
        substrait_plan_override=options.substrait_plan_override,
    )
    plan_hash = options.plan_hash or _plan_hash_for_expr(resolved_expr, options=options)
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
    policy: SqlGlotPolicy,
) -> str | None:
    try:
        return sqlglot_sql(resolved_expr, policy=policy)
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
    from ibis_engine.plan_artifacts import ibis_plan_artifacts

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
    policy_hash = _policy_hash_for_options(options)
    normalized_sql = _collect_normalized_sql(
        inputs.resolved_expr,
        policy=inputs.policy,
    )
    ibis_payload = _collect_ibis_artifacts(options)
    return DataFusionPlanArtifacts(
        plan_hash=inputs.plan_hash,
        policy_hash=policy_hash,
        ast_fingerprint=canonical_fingerprint,
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


def _uuid_short() -> str:
    """Generate a short UUID for temporary table names.

    Returns
    -------
    str
        Short hexadecimal UUID segment.
    """
    return uuid.uuid4().hex[:8]


__all__ = [
    "CopyToOptions",
    "DataFusionCompileOptions",
    "DataFusionDmlOptions",
    "DataFusionPlanArtifacts",
    "DataFusionSqlPolicy",
    "IbisCompilerBackend",
    "MaterializationPolicy",
    "collect_plan_artifacts",
    "datafusion_to_async_batches",
    "datafusion_to_reader",
    "datafusion_write_options",
    "df_from_sqlglot_or_sql",
    "rehydrate_plan_artifacts",
    "replay_substrait_bytes",
    "validate_substrait_plan",
    "validate_table_constraints",
]
