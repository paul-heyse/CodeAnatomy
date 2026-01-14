"""Shared plan-lane helper utilities."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, cast, overload

import pyarrow as pa
import pyarrow.dataset as ds
from ibis.expr.types import Table as IbisTable

from arrowdsl.compute.ids import HashSpec, hash_projection
from arrowdsl.compute.macros import ColumnOrNullExpr
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
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import QuerySpec
from arrowdsl.plan.runner import AdapterRunOptions, PlanRunResult, run_plan, run_plan_adapter
from arrowdsl.plan.scan_io import DatasetSource, PlanSource, plan_from_source
from arrowdsl.schema.ops import (
    align_plan,
    encode_plan,
    encode_table,
    encoding_columns_from_metadata,
    encoding_projection,
)
from arrowdsl.schema.schema import CastErrorPolicy, align_to_schema, empty_table
from arrowdsl.schema.structs import flatten_struct_field
from config import AdapterMode
from ibis_engine.expr_compiler import IbisExprRegistry
from ibis_engine.plan import IbisPlan
from ibis_engine.query_bridge import QueryBridgeResult, queryspec_to_ibis
from ibis_engine.query_compiler import IbisQuerySpec, apply_query_spec


@dataclass(frozen=True)
class FinalizePlanAdapterOptions:
    """Options for finalizing plan adapters."""

    adapter_mode: AdapterMode | None = None
    prefer_reader: bool = False
    schema: SchemaLike | None = None
    keep_extra_columns: bool = False


def query_for_schema(schema: SchemaLike) -> QuerySpec:
    """Return a QuerySpec projecting the schema columns.

    Returns
    -------
    QuerySpec
        QuerySpec with base columns set to the schema names.
    """
    return QuerySpec.simple(*schema.names)


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
    exprs: list[ComputeExpression] = []
    for col in cols:
        if col not in available:
            continue
        expr = pc.field(col)
        if cast:
            expr = ensure_expression(pc.cast(expr, dtype, safe=safe))
        else:
            expr = ensure_expression(expr)
        exprs.append(expr)
    if not exprs:
        return ensure_expression(pc.scalar(pa.scalar(None, type=dtype)))
    if len(exprs) == 1:
        return exprs[0]
    return ensure_expression(pc.coalesce(*exprs))


def project_columns(
    plan: Plan,
    *,
    base: Sequence[str],
    rename: Mapping[str, str] | None = None,
    extras: Sequence[tuple[ComputeExpression, str]] = (),
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Project columns with optional renames and appended expressions.

    Returns
    -------
    Plan
        Plan with projected columns and appended expressions.
    """
    rename = rename or {}
    names = [rename.get(name, name) for name in base]
    expressions: list[ComputeExpression] = [pc.field(name) for name in base]
    for expr, name in extras:
        expressions.append(expr)
        names.append(name)
    return plan.project(expressions, names, ctx=ctx)


def apply_hash_projection(
    plan: Plan,
    *,
    specs: Sequence[HashSpec],
    available: Sequence[str] | None = None,
    required: Mapping[str, Sequence[str]] | None = None,
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Apply hash projections to a plan, appending hash ID columns.

    Returns
    -------
    Plan
        Plan with appended hash columns.

    Raises
    ------
    ValueError
        Raised when no available columns are provided and no context is supplied.
    """
    if available is None:
        if ctx is None:
            msg = "apply_hash_projection requires available or ctx."
            raise ValueError(msg)
        available = plan.schema(ctx=ctx).names
    names = list(available)
    expr_map: dict[str, ComputeExpression] = {name: pc.field(name) for name in names}
    for spec in specs:
        out_col = spec.out_col or f"{spec.prefix}_id"
        req = required.get(out_col) if required else None
        expr, out_name = hash_projection(spec, available=names, required=req)
        expr_map[out_name] = expr
        if out_name not in names:
            names.append(out_name)
    expressions = [expr_map[name] for name in names]
    return plan.project(expressions, names, ctx=ctx)


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
    available = set(plan.schema(ctx=ctx).names)
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
        for name in plan.schema(ctx=ctx).names:
            if name in names:
                continue
            names.append(name)
            exprs.append(pc.field(name))
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


def apply_query_spec_ibis(
    table: IbisTable,
    *,
    spec: IbisQuerySpec | QuerySpec | QueryBridgeResult,
    registry: IbisExprRegistry | None = None,
) -> IbisTable:
    """Apply a query spec to a table expression.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table with projections and filters applied.

    Raises
    ------
    ValueError
        Raised when kernel-lane fallbacks are required.
    """
    if isinstance(spec, QuerySpec):
        spec = queryspec_to_ibis(spec)
    if isinstance(spec, QueryBridgeResult):
        if spec.has_kernel_fallback():
            msg = "QuerySpec requires kernel-lane fallback; apply after materialization."
            raise ValueError(msg)
        return apply_query_spec(table, spec=spec.ibis_spec, registry=registry)
    return apply_query_spec(table, spec=spec, registry=registry)


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
    config = code_unit_meta_config(plan.schema(ctx=ctx).names, code_units.schema(ctx=ctx).names)
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
        plan.schema(ctx=ctx).names,
        repo_files.schema(ctx=ctx).names,
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
    available = set(out.schema(ctx=ctx).names)
    if right_col not in available:
        return out
    if file_id_key in available:
        file_id_expr = ensure_expression(pc.coalesce(pc.field(file_id_key), pc.field(right_col)))
    else:
        file_id_expr = pc.field(right_col)
    out = set_or_append_column(out, name=file_id_key, expr=file_id_expr, ctx=ctx)
    if right_col != file_id_key:
        keep = [col for col in out.schema(ctx=ctx).names if col != right_col]
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
    names = list(plan.schema(ctx=ctx).names)
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

    Raises
    ------
    TypeError
        Raised when run_plan returns a reader instead of a table.
    """
    if schema is not None:
        if plan.decl is None:
            result = run_plan(plan, ctx=ctx, prefer_reader=False)
            if isinstance(result.value, pa.RecordBatchReader):
                msg = "Expected table result from run_plan."
                raise TypeError(msg)
            aligned = align_table_to_schema(
                cast("TableLike", result.value),
                schema=schema,
                safe_cast=ctx.safe_cast,
                keep_extra_columns=keep_extra_columns,
                on_error="unsafe" if ctx.safe_cast else "raise",
            )
            return PlanRunResult(value=aligned, kind="table")
        plan = align_plan(
            plan,
            schema=schema,
            ctx=ctx,
            keep_extra_columns=keep_extra_columns,
        )
    return run_plan(plan, ctx=ctx, prefer_reader=prefer_reader)


def finalize_plan_result_adapter(
    plan: Plan | IbisPlan,
    *,
    ctx: ExecutionContext,
    options: FinalizePlanAdapterOptions | None = None,
) -> PlanRunResult:
    """Return a reader or table for Plan/IbisPlan plus metadata.

    Returns
    -------
    PlanRunResult
        Plan output and the materialization kind.
    """
    options = options or FinalizePlanAdapterOptions()
    if isinstance(plan, IbisPlan):
        if options.schema is not None:
            aligned = align_table_to_schema(
                plan.to_table(),
                schema=options.schema,
                safe_cast=ctx.safe_cast,
                keep_extra_columns=options.keep_extra_columns,
                on_error="unsafe" if ctx.safe_cast else "raise",
            )
            return PlanRunResult(value=aligned, kind="table")
        return run_plan_adapter(
            plan,
            ctx=ctx,
            options=AdapterRunOptions(
                adapter_mode=options.adapter_mode,
                prefer_reader=options.prefer_reader,
            ),
        )
    return finalize_plan_result(
        plan,
        ctx=ctx,
        prefer_reader=options.prefer_reader,
        schema=options.schema,
        keep_extra_columns=options.keep_extra_columns,
    )


@overload
def finalize_plan(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    prefer_reader: Literal[False] = False,
    schema: SchemaLike | None = None,
    keep_extra_columns: bool = False,
) -> TableLike: ...


@overload
def finalize_plan(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    prefer_reader: Literal[True],
    schema: SchemaLike | None = None,
    keep_extra_columns: bool = False,
) -> TableLike | RecordBatchReaderLike: ...


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
    return table.unify_dictionaries()


def finalize_plan_adapter(
    plan: Plan | IbisPlan,
    *,
    ctx: ExecutionContext,
    options: FinalizePlanAdapterOptions | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Return a reader when possible, otherwise materialize the plan.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when allowed, otherwise a table.
    """
    result = finalize_plan_result_adapter(
        plan,
        ctx=ctx,
        options=options,
    )
    value = result.value
    if isinstance(value, pa.RecordBatchReader):
        return value
    table = cast("TableLike", value)
    return table.unify_dictionaries()


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
    "FinalizePlanAdapterOptions",
    "PlanSource",
    "align_plan",
    "align_table_to_schema",
    "apply_hash_projection",
    "apply_query_spec_ibis",
    "assert_schema_metadata",
    "coalesce_expr",
    "code_unit_meta_join",
    "column_or_null_expr",
    "empty_plan",
    "encode_plan",
    "encode_table",
    "encoding_columns_from_metadata",
    "encoding_projection",
    "ensure_plan",
    "finalize_context_for_plan",
    "finalize_plan",
    "finalize_plan_adapter",
    "finalize_plan_result",
    "finalize_plan_result_adapter",
    "flatten_struct_field",
    "path_meta_join",
    "plan_source",
    "project_columns",
    "project_to_schema",
    "query_for_schema",
    "set_or_append_column",
]
