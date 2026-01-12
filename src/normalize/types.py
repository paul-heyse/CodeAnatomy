"""Normalize type expressions into type nodes and edges."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from arrowdsl.compute.expr_specs import HashExprSpec, trimmed_non_empty_expr
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike, ensure_expression, pc
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.plan.plan import Plan
from arrowdsl.plan_helpers import column_or_null_expr
from arrowdsl.schema.schema import empty_table
from normalize.hash_specs import TYPE_ID_SPEC
from normalize.plan_helpers import PlanSource, materialize_plan, plan_source, project_columns
from normalize.runner import (
    ensure_canonical,
    ensure_execution_context,
    run_normalize,
    run_normalize_streamable_contract,
)
from normalize.schemas import (
    TYPE_EXPRS_CONTRACT,
    TYPE_EXPRS_NORM_SPEC,
    TYPE_EXPRS_QUERY,
    TYPE_NODES_CONTRACT,
    TYPE_NODES_QUERY,
    TYPE_NODES_SCHEMA,
    TYPE_NODES_SPEC,
)

_BASE_TYPE_EXPR_COLUMNS: tuple[tuple[str, pa.DataType], ...] = (
    ("file_id", pa.string()),
    ("path", pa.string()),
    ("bstart", pa.int64()),
    ("bend", pa.int64()),
    ("owner_def_id", pa.string()),
    ("param_name", pa.string()),
    ("expr_kind", pa.string()),
    ("expr_role", pa.string()),
    ("expr_text", pa.string()),
)


def _to_plan(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    columns: Sequence[str] | None = None,
) -> Plan:
    return plan_source(source, ctx=ctx, columns=columns)


def type_exprs_plan(
    cst_type_exprs: PlanSource,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Build a plan-lane normalized type expression table.

    Returns
    -------
    Plan
        Plan producing normalized type expression rows.
    """
    base_names = [name for name, _ in _BASE_TYPE_EXPR_COLUMNS]
    plan = _to_plan(cst_type_exprs, ctx=ctx, columns=base_names)
    available = set(plan.schema(ctx=ctx).names)
    base_exprs = [
        column_or_null_expr(name, dtype, available=available)
        for name, dtype in _BASE_TYPE_EXPR_COLUMNS
    ]
    plan = plan.project(base_exprs, base_names, ctx=ctx)

    _, non_empty = trimmed_non_empty_expr("expr_text")
    plan = plan.filter(non_empty, ctx=ctx)
    return TYPE_EXPRS_QUERY.apply_to_plan(plan, ctx=ctx)


def normalize_type_exprs_result(
    cst_type_exprs: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> FinalizeResult:
    """Normalize type expression rows into join-ready tables.

    Parameters
    ----------
    cst_type_exprs:
        CST type expression rows captured during extraction.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    FinalizeResult
        Finalize bundle with normalized type expressions.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = type_exprs_plan(cst_type_exprs, ctx=exec_ctx)
    return run_normalize(
        plan=plan,
        post=(),
        contract=TYPE_EXPRS_CONTRACT,
        ctx=exec_ctx,
        metadata_spec=TYPE_EXPRS_NORM_SPEC.metadata_spec,
    )


def normalize_type_exprs(
    cst_type_exprs: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Normalize type expression rows into join-ready tables.

    Parameters
    ----------
    cst_type_exprs:
        CST type expression rows captured during extraction.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    TableLike
        Normalized type expressions table with type ids.
    """
    return normalize_type_exprs_result(cst_type_exprs, ctx=ctx).good


def normalize_type_exprs_canonical(
    cst_type_exprs: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Normalize type expressions under canonical determinism.

    Returns
    -------
    TableLike
        Canonicalized type expressions table.
    """
    exec_ctx = ensure_canonical(ensure_execution_context(ctx))
    return normalize_type_exprs_result(cst_type_exprs, ctx=exec_ctx).good


def normalize_type_exprs_streamable(
    cst_type_exprs: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Normalize type expressions with a streamable output.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when streamable, otherwise a materialized table.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = type_exprs_plan(cst_type_exprs, ctx=exec_ctx)
    return run_normalize_streamable_contract(plan, contract=TYPE_EXPRS_CONTRACT, ctx=exec_ctx)


def type_nodes_plan_from_scip(
    scip_symbol_information: PlanSource,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Build a plan-lane type node table from SCIP symbol information.

    Returns
    -------
    Plan
        Plan producing normalized type node rows.
    """
    plan = _to_plan(scip_symbol_information, ctx=ctx, columns=["type_repr"])
    available = set(plan.schema(ctx=ctx).names)
    if "type_repr" not in available:
        return Plan.table_source(empty_table(TYPE_NODES_SCHEMA))

    trimmed, non_empty = trimmed_non_empty_expr("type_repr")
    plan = plan.filter(non_empty, ctx=ctx)
    plan = plan.project([trimmed], ["type_repr"], ctx=ctx)

    type_hash = HashExprSpec(spec=TYPE_ID_SPEC).to_expression()
    plan = project_columns(
        plan,
        base=["type_repr"],
        extras=[
            (type_hash, "type_id"),
            (pc.scalar("scip"), "type_form"),
            (pc.scalar("inferred"), "origin"),
        ],
        ctx=ctx,
    )
    return TYPE_NODES_QUERY.apply_to_plan(plan, ctx=ctx)


def type_nodes_plan_from_exprs(
    type_exprs_norm: PlanSource,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Build a plan-lane type node table from type expressions.

    Returns
    -------
    Plan
        Plan producing normalized type node rows.
    """
    plan = _to_plan(type_exprs_norm, ctx=ctx, columns=["type_id", "type_repr"])
    available = set(plan.schema(ctx=ctx).names)
    if "type_repr" not in available or "type_id" not in available:
        return Plan.table_source(empty_table(TYPE_NODES_SCHEMA))

    trimmed, non_empty = trimmed_non_empty_expr("type_repr")
    valid = ensure_expression(pc.and_(non_empty, pc.is_valid(pc.field("type_id"))))
    plan = plan.filter(valid, ctx=ctx)
    plan = plan.project([pc.field("type_id"), trimmed], ["type_id", "type_repr"], ctx=ctx)
    plan = project_columns(
        plan,
        base=["type_id", "type_repr"],
        extras=[
            (pc.scalar("annotation"), "type_form"),
            (pc.scalar("annotation"), "origin"),
        ],
        ctx=ctx,
    )
    return TYPE_NODES_QUERY.apply_to_plan(plan, ctx=ctx)


def _type_nodes_plan(
    type_exprs_norm: PlanSource,
    scip_symbol_information: PlanSource | None,
    *,
    ctx: ExecutionContext,
) -> Plan:
    scip_table: TableLike | None = None
    if scip_symbol_information is not None:
        scip_plan = _to_plan(scip_symbol_information, ctx=ctx, columns=["type_repr"])
        scip_table = materialize_plan(scip_plan, ctx=ctx)
    if (
        scip_table is not None
        and scip_table.num_rows > 0
        and "type_repr" in scip_table.column_names
    ):
        return type_nodes_plan_from_scip(scip_table, ctx=ctx)
    return type_nodes_plan_from_exprs(type_exprs_norm, ctx=ctx)


def normalize_types_result(
    type_exprs_norm: PlanSource,
    scip_symbol_information: PlanSource | None = None,
    *,
    ctx: ExecutionContext | None = None,
) -> FinalizeResult:
    """Build a type node table from normalized type expressions.

    Parameters
    ----------
    type_exprs_norm:
        Normalized type expression table.
    scip_symbol_information:
        Optional SCIP symbol information table with type details.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    FinalizeResult
        Finalize bundle with normalized type nodes.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = _type_nodes_plan(
        type_exprs_norm,
        scip_symbol_information,
        ctx=exec_ctx,
    )

    return run_normalize(
        plan=plan,
        post=(),
        contract=TYPE_NODES_CONTRACT,
        ctx=exec_ctx,
        metadata_spec=TYPE_NODES_SPEC.metadata_spec,
    )


def normalize_types(
    type_exprs_norm: PlanSource,
    scip_symbol_information: PlanSource | None = None,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Build a type node table from normalized type expressions.

    Parameters
    ----------
    type_exprs_norm:
        Normalized type expression table.
    scip_symbol_information:
        Optional SCIP symbol information table with type details.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    TableLike
        Normalized type node table.
    """
    return normalize_types_result(
        type_exprs_norm,
        scip_symbol_information=scip_symbol_information,
        ctx=ctx,
    ).good


def normalize_types_canonical(
    type_exprs_norm: PlanSource,
    scip_symbol_information: PlanSource | None = None,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Normalize type nodes under canonical determinism.

    Returns
    -------
    TableLike
        Canonicalized type node table.
    """
    exec_ctx = ensure_canonical(ensure_execution_context(ctx))
    return normalize_types_result(
        type_exprs_norm,
        scip_symbol_information=scip_symbol_information,
        ctx=exec_ctx,
    ).good


def normalize_types_streamable(
    type_exprs_norm: PlanSource,
    scip_symbol_information: PlanSource | None = None,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Normalize type nodes with a streamable output.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when streamable, otherwise a materialized table.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = _type_nodes_plan(
        type_exprs_norm,
        scip_symbol_information,
        ctx=exec_ctx,
    )
    return run_normalize_streamable_contract(plan, contract=TYPE_NODES_CONTRACT, ctx=exec_ctx)


__all__ = [
    "normalize_type_exprs",
    "normalize_type_exprs_canonical",
    "normalize_type_exprs_result",
    "normalize_type_exprs_streamable",
    "normalize_types",
    "normalize_types_canonical",
    "normalize_types_result",
    "normalize_types_streamable",
    "type_exprs_plan",
    "type_nodes_plan_from_exprs",
    "type_nodes_plan_from_scip",
]
