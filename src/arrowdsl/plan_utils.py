"""Plan-lane helper utilities for plan construction and finalize."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

import pyarrow as pa
import pyarrow.dataset as ds

from arrowdsl.compute.ids import HashSpec, hash_projection
from arrowdsl.compute.macros import CoalesceExpr, ColumnOrNullExpr
from arrowdsl.compute.predicates import InSet
from arrowdsl.core.context import DeterminismTier, ExecutionContext, OrderingLevel
from arrowdsl.core.interop import (
    ComputeExpression,
    DataTypeLike,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    ensure_expression,
    pc,
)
from arrowdsl.finalize.finalize import Contract
from arrowdsl.plan.joins import code_unit_meta_config, left_join, path_meta_config
from arrowdsl.plan.plan import Plan, PlanRunResult
from arrowdsl.plan.query import QuerySpec
from arrowdsl.plan.scan_io import DatasetSource, PlanSource, plan_from_source
from arrowdsl.plan.schema_utils import plan_output_columns
from arrowdsl.schema.dictionary import normalize_dictionaries
from arrowdsl.schema.schema import (
    CastErrorPolicy,
    align_to_schema,
    empty_table,
    encoding_columns_from_metadata,
    encoding_projection,
    projection_for_schema,
)
from arrowdsl.schema.structs import flatten_struct_field


def _plan_columns(plan: Plan, *, ctx: ExecutionContext | None = None) -> Sequence[str]:
    columns = plan_output_columns(plan)
    if columns is not None:
        return columns
    if ctx is None:
        msg = "plan columns require ctx when inference is unavailable."
        raise ValueError(msg)
    return list(plan.schema(ctx=ctx).names)


def query_for_schema(schema: SchemaLike) -> QuerySpec:
    """Return a QuerySpec projecting the schema columns.

    Returns
    -------
    QuerySpec
        QuerySpec with base columns set to the schema names.
    """
    return QuerySpec.simple(*schema.names)


def dataset_query_for_file_ids(
    file_ids: Sequence[str],
    *,
    schema: SchemaLike | None = None,
    columns: Sequence[str] | None = None,
) -> QuerySpec:
    """Return a QuerySpec filtering to the provided file ids.

    Parameters
    ----------
    file_ids:
        File ids to include.
    schema:
        Optional schema used to build the projection.
    columns:
        Optional explicit projection columns.

    Returns
    -------
    QuerySpec
        QuerySpec with file_id predicates for plan and pushdown lanes.

    Raises
    ------
    ValueError
        Raised when neither columns nor schema are provided.
    """
    if columns is None:
        if schema is None:
            msg = "dataset_query_for_file_ids requires columns or schema."
            raise ValueError(msg)
        columns = list(schema.names)
    predicate = InSet("file_id", tuple(file_ids))
    return QuerySpec.simple(
        *columns,
        predicate=predicate,
        pushdown_predicate=predicate,
    )


def column_or_null_expr(
    name: str,
    dtype: DataTypeLike,
    *,
    available: set[str],
    cast: bool = False,
    safe: bool = False,
) -> ComputeExpression:
    """Return a field expression or typed null when missing.

    Returns
    -------
    ComputeExpression
        Expression for the field or a typed null literal.
    """
    spec = ColumnOrNullExpr(
        name=name,
        dtype=dtype,
        available=frozenset(available),
        cast=cast,
        safe=safe,
    )
    return spec.to_expression()


def coalesce_expr(
    cols: Sequence[str],
    *,
    dtype: DataTypeLike,
    available: set[str],
    cast: bool = False,
    safe: bool = False,
) -> ComputeExpression:
    """Return a coalesced expression over available columns.

    Returns
    -------
    ComputeExpression
        Coalesced expression or typed null when none are available.
    """
    expr_specs: list[ColumnOrNullExpr] = []
    for col in cols:
        if col not in available:
            continue
        expr_specs.append(
            ColumnOrNullExpr(
                name=col,
                dtype=dtype,
                cast=cast,
                safe=safe,
            )
        )
    if not expr_specs:
        return ensure_expression(pc.scalar(pa.scalar(None, type=dtype)))
    if len(expr_specs) == 1:
        return expr_specs[0].to_expression()
    return CoalesceExpr(exprs=tuple(expr_specs)).to_expression()


def project_columns(
    plan: Plan,
    *,
    base: Sequence[str],
    rename: Mapping[str, str] | None = None,
    extras: Sequence[tuple[ComputeExpression, str]] | None = None,
    ctx: ExecutionContext,
) -> Plan:
    """Project a plan to a set of columns with optional renames.

    Returns
    -------
    Plan
        Plan projecting the requested columns.
    """
    rename = rename or {}
    available = set(_plan_columns(plan, ctx=ctx))
    names = [name for name in base if name in available]
    exprs = [pc.field(name) for name in names]
    out_names = [rename.get(name, name) for name in names]
    if extras:
        for expr, name in extras:
            exprs.append(expr)
            out_names.append(name)
    return plan.project(exprs, out_names, ctx=ctx)


def apply_hash_projection(
    plan: Plan,
    *,
    hash_spec: HashSpec,
    ctx: ExecutionContext,
    available: Sequence[str] | None = None,
) -> Plan:
    """Apply a hash projection to a plan.

    Returns
    -------
    Plan
        Plan with the hash column added.
    """
    if available is None:
        available = _plan_columns(plan, ctx=ctx)
    expr, name = hash_projection(hash_spec, available=available)
    return plan.project([expr], [name], ctx=ctx)


def project_to_schema(
    plan: Plan,
    *,
    schema: SchemaLike,
    ctx: ExecutionContext,
    keep_extra_columns: bool = False,
) -> Plan:
    """Project a plan to the schema, filling missing columns with typed nulls.

    Returns
    -------
    Plan
        Plan aligned to the schema.
    """
    available = set(_plan_columns(plan, ctx=ctx))
    names: list[str] = []
    exprs: list[ComputeExpression] = []
    for field in schema:
        names.append(field.name)
        exprs.append(
            column_or_null_expr(
                field.name,
                field.type,
                available=available,
                cast=True,
                safe=False,
            )
        )
    if keep_extra_columns:
        for name in _plan_columns(plan, ctx=ctx):
            if name in names:
                continue
            names.append(name)
            exprs.append(pc.field(name))
    return plan.project(exprs, names, ctx=ctx)


def align_plan(
    plan: Plan,
    *,
    schema: SchemaLike,
    ctx: ExecutionContext,
    keep_extra_columns: bool = False,
    available: Sequence[str] | None = None,
) -> Plan:
    """Align a plan to a target schema via projection.

    Returns
    -------
    Plan
        Plan projecting/casting to the schema.
    """
    if available is None:
        available = _plan_columns(plan, ctx=ctx)
    exprs, names = projection_for_schema(schema, available=available, safe_cast=ctx.safe_cast)
    if keep_extra_columns:
        extras = [name for name in available if name not in names]
        for name in extras:
            names.append(name)
            exprs.append(pc.field(name))
    return plan.project(exprs, names, ctx=ctx)


def encode_plan(plan: Plan, *, columns: Sequence[str], ctx: ExecutionContext) -> Plan:
    """Return a plan with dictionary encoding applied.

    Returns
    -------
    Plan
        Plan with dictionary-encoded columns.
    """
    exprs, names = encoding_projection(columns, available=_plan_columns(plan, ctx=ctx))
    return plan.project(exprs, names, ctx=ctx)


def plan_source(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    columns: Sequence[str] | None = None,
    label: str = "",
) -> Plan:
    """Return a plan for tables, readers, or dataset-backed sources.

    Returns
    -------
    Plan
        Acero-backed plan for dataset/table sources.
    """
    return plan_from_source(source, ctx=ctx, columns=columns, label=label)


def code_unit_meta_join(
    plan: Plan,
    code_units: Plan,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Left-join code-unit metadata (file_id/path) onto a plan.

    Returns
    -------
    Plan
        Plan with code unit metadata columns appended.
    """
    config = code_unit_meta_config(
        _plan_columns(plan, ctx=ctx),
        _plan_columns(code_units, ctx=ctx),
    )
    if config is None:
        return plan
    joined = left_join(plan, code_units, config=config, ctx=ctx)
    if isinstance(joined, Plan):
        return joined
    return Plan.table_source(joined)


def path_meta_join(
    plan: Plan,
    repo_files: Plan,
    *,
    ctx: ExecutionContext,
    path_key: str = "path",
    output_suffix_for_right: str = "_repo",
) -> Plan:
    """Left-join path metadata (file_id) onto a plan.

    Returns
    -------
    Plan
        Plan with file_id metadata columns appended.
    """
    config = path_meta_config(
        _plan_columns(plan, ctx=ctx),
        _plan_columns(repo_files, ctx=ctx),
        key=path_key,
        output_suffix_for_right=output_suffix_for_right,
    )
    if config is None:
        return plan
    joined = left_join(plan, repo_files, config=config, ctx=ctx)
    out = joined if isinstance(joined, Plan) else Plan.table_source(joined)
    file_id_key = "file_id"
    right_col = (
        f"{file_id_key}{output_suffix_for_right}" if output_suffix_for_right else file_id_key
    )
    available = set(_plan_columns(out, ctx=ctx))
    if right_col not in available:
        return out
    if file_id_key in available:
        file_id_expr = ensure_expression(pc.coalesce(pc.field(file_id_key), pc.field(right_col)))
    else:
        file_id_expr = pc.field(right_col)
    out = set_or_append_column(out, name=file_id_key, expr=file_id_expr, ctx=ctx)
    if right_col != file_id_key:
        keep = [col for col in _plan_columns(out, ctx=ctx) if col != right_col]
        out = out.project([pc.field(col) for col in keep], keep, ctx=ctx)
    return out


def ensure_plan(
    source: PlanSource,
    *,
    label: str = "",
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Return a plan backed by an Acero table source.

    Returns
    -------
    Plan
        Plan for the source value.

    Raises
    ------
    ValueError
        Raised when a DatasetSource is provided without a context.
    """
    if isinstance(source, Plan):
        return source
    if ctx is None:
        if isinstance(source, (DatasetSource, ds.Dataset, ds.Scanner)):
            msg = "ensure_plan requires ctx when source is DatasetSource."
            raise ValueError(msg)
        if isinstance(source, pa.RecordBatchReader):
            reader = cast("pa.RecordBatchReader", source)
            return Plan.table_source(reader.read_all(), label=label)
        return Plan.table_source(cast("TableLike", source), label=label)
    return plan_from_source(source, ctx=ctx, label=label)


def empty_plan(schema: SchemaLike, *, label: str = "") -> Plan:
    """Return a plan backed by an empty table with the provided schema.

    Returns
    -------
    Plan
        Plan with an empty table source.
    """
    return Plan.table_source(empty_table(schema), label=label)


def set_or_append_column(
    plan: Plan,
    *,
    name: str,
    expr: ComputeExpression,
    ctx: ExecutionContext,
) -> Plan:
    """Replace or append a column with the provided expression.

    Returns
    -------
    Plan
        Updated plan with the column set.
    """
    names = list(_plan_columns(plan, ctx=ctx))
    exprs = [pc.field(col) for col in names]
    if name in names:
        idx = names.index(name)
        exprs[idx] = expr
    else:
        names.append(name)
        exprs.append(expr)
    return plan.project(exprs, names, ctx=ctx)


def finalize_plan_result(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
    schema: SchemaLike | None = None,
    keep_extra_columns: bool = False,
) -> PlanRunResult:
    """Return a reader or table plus materialization metadata.

    Returns
    -------
    PlanRunResult
        Plan output and the materialization kind.
    """
    if schema is not None:
        plan = align_plan(
            plan,
            schema=schema,
            ctx=ctx,
            keep_extra_columns=keep_extra_columns,
        )
    if prefer_reader:
        try:
            reader = plan.to_reader(ctx=ctx)
            return PlanRunResult(value=reader, kind="reader")
        except ValueError:
            pass
    table = plan.to_table(ctx=ctx)
    return PlanRunResult(value=table, kind="table")


def finalize_plan(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
    schema: SchemaLike | None = None,
    keep_extra_columns: bool = False,
) -> TableLike | RecordBatchReaderLike:
    """Return a reader when possible, otherwise materialize the plan.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when allowed, otherwise a table.
    """
    result = finalize_plan_result(
        plan,
        ctx=ctx,
        prefer_reader=prefer_reader,
        schema=schema,
        keep_extra_columns=keep_extra_columns,
    )
    value = result.value
    if isinstance(value, pa.RecordBatchReader):
        return value
    table = cast("TableLike", value)
    return normalize_dictionaries(table, combine_chunks=False)


def align_table_to_schema(
    table: TableLike,
    *,
    schema: SchemaLike,
    safe_cast: bool = True,
    keep_extra_columns: bool = False,
    on_error: CastErrorPolicy = "unsafe",
) -> TableLike:
    """Align a table to a target schema with configurable casting behavior.

    Returns
    -------
    TableLike
        Table aligned to the provided schema.
    """
    aligned, _ = align_to_schema(
        table,
        schema=schema,
        safe_cast=safe_cast,
        keep_extra_columns=keep_extra_columns,
        on_error=on_error,
    )
    return aligned


def finalize_context_for_plan(
    plan: Plan,
    *,
    contract: Contract,
    ctx: ExecutionContext,
) -> ExecutionContext:
    """Return the finalize context with canonical ordering disabled when redundant.

    Returns
    -------
    ExecutionContext
        Execution context configured for finalize ordering.
    """
    if ctx.determinism != DeterminismTier.CANONICAL:
        return ctx
    explicit_keys = tuple((sk.column, sk.order) for sk in contract.canonical_sort)
    if plan.ordering.level == OrderingLevel.EXPLICIT and plan.ordering.keys == explicit_keys:
        return ctx.with_determinism(DeterminismTier.STABLE_SET)
    return ctx


def assert_schema_metadata(table: TableLike, *, schema: SchemaLike) -> None:
    """Raise when schema metadata does not match the target schema.

    Raises
    ------
    ValueError
        Raised when schema metadata does not match.
    """
    table_schema = pa.schema(table.schema)
    expected_schema = pa.schema(schema)
    if not table_schema.equals(expected_schema, check_metadata=True):
        msg = "Schema metadata mismatch after finalize."
        raise ValueError(msg)


__all__ = [
    "PlanSource",
    "align_plan",
    "align_table_to_schema",
    "apply_hash_projection",
    "assert_schema_metadata",
    "coalesce_expr",
    "code_unit_meta_join",
    "column_or_null_expr",
    "dataset_query_for_file_ids",
    "empty_plan",
    "encode_plan",
    "encoding_columns_from_metadata",
    "encoding_projection",
    "ensure_plan",
    "finalize_context_for_plan",
    "finalize_plan",
    "finalize_plan_result",
    "flatten_struct_field",
    "path_meta_join",
    "plan_source",
    "project_columns",
    "project_to_schema",
    "query_for_schema",
    "set_or_append_column",
]
