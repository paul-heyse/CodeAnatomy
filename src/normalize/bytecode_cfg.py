"""Normalize bytecode CFG tables for join-ready use."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike, pc
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.plan.ops import JoinSpec
from arrowdsl.plan.plan import Plan
from normalize.contracts import CFG_BLOCKS_CONTRACT, CFG_EDGES_CONTRACT
from normalize.join_helpers import join_plan
from normalize.plan_exprs import column_or_null_expr
from normalize.plan_helpers import append_projection, apply_query_spec
from normalize.runner import ensure_canonical, ensure_execution_context, run_normalize
from normalize.schemas import (
    CFG_BLOCKS_NORM_SCHEMA,
    CFG_BLOCKS_NORM_SPEC,
    CFG_BLOCKS_QUERY,
    CFG_EDGES_NORM_SCHEMA,
    CFG_EDGES_NORM_SPEC,
    CFG_EDGES_QUERY,
)

_META_COLUMNS: tuple[tuple[str, pa.DataType], ...] = (
    ("code_unit_id", pa.string()),
    ("file_id", pa.string()),
    ("path", pa.string()),
)


def _to_plan(table: TableLike | Plan) -> Plan:
    if isinstance(table, Plan):
        return table
    return Plan.table_source(table)


def _ensure_output_columns(plan: Plan, *, schema: pa.Schema, ctx: ExecutionContext) -> Plan:
    available = list(plan.schema(ctx=ctx).names)
    missing = [name for name in schema.names if name not in available]
    if not missing:
        return plan
    extras = [
        (pc.scalar(pa.scalar(None, type=schema.field(name).type)), name)
        for name in missing
    ]
    return append_projection(plan, base=available, extras=extras, ctx=ctx)


def _code_unit_meta_plan(code_units: TableLike | Plan, *, ctx: ExecutionContext) -> Plan:
    plan = _to_plan(code_units)
    available = set(plan.schema(ctx=ctx).names)
    names = [name for name, _ in _META_COLUMNS]
    exprs = [
        column_or_null_expr(name, dtype, available=available) for name, dtype in _META_COLUMNS
    ]
    return plan.project(exprs, names, ctx=ctx)


def cfg_blocks_plan(
    py_bc_blocks: TableLike | Plan,
    py_bc_code_units: TableLike | Plan,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Normalize CFG block rows and enrich with file/path metadata.

    Returns
    -------
    Plan
        Plan producing normalized CFG block rows.
    """
    blocks = _to_plan(py_bc_blocks)
    blocks_available = set(blocks.schema(ctx=ctx).names)
    code_units = _to_plan(py_bc_code_units)
    code_available = set(code_units.schema(ctx=ctx).names)

    if "code_unit_id" in blocks_available and "code_unit_id" in code_available:
        meta = _code_unit_meta_plan(code_units, ctx=ctx)
        joined = join_plan(
            blocks,
            meta,
            spec=JoinSpec(
                join_type="left outer",
                left_keys=("code_unit_id",),
                right_keys=("code_unit_id",),
                left_output=tuple(blocks.schema(ctx=ctx).names),
                right_output=("file_id", "path"),
            ),
            ctx=ctx,
        )
        if not isinstance(joined, Plan):
            joined = Plan.table_source(joined)
    else:
        joined = blocks

    joined = _ensure_output_columns(joined, schema=CFG_BLOCKS_NORM_SCHEMA, ctx=ctx)
    return apply_query_spec(joined, spec=CFG_BLOCKS_QUERY, ctx=ctx)


def cfg_edges_plan(
    py_bc_code_units: TableLike | Plan,
    py_bc_cfg_edges: TableLike | Plan,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Normalize CFG edge rows and enrich with file/path metadata.

    Returns
    -------
    Plan
        Plan producing normalized CFG edge rows.
    """
    edges = _to_plan(py_bc_cfg_edges)
    edges_available = set(edges.schema(ctx=ctx).names)
    code_units = _to_plan(py_bc_code_units)
    code_available = set(code_units.schema(ctx=ctx).names)

    if "code_unit_id" in edges_available and "code_unit_id" in code_available:
        meta = _code_unit_meta_plan(code_units, ctx=ctx)
        joined = join_plan(
            edges,
            meta,
            spec=JoinSpec(
                join_type="left outer",
                left_keys=("code_unit_id",),
                right_keys=("code_unit_id",),
                left_output=tuple(edges.schema(ctx=ctx).names),
                right_output=("file_id", "path"),
            ),
            ctx=ctx,
        )
        if not isinstance(joined, Plan):
            joined = Plan.table_source(joined)
    else:
        joined = edges

    joined = _ensure_output_columns(joined, schema=CFG_EDGES_NORM_SCHEMA, ctx=ctx)
    return apply_query_spec(joined, spec=CFG_EDGES_QUERY, ctx=ctx)


def build_cfg_blocks_result(
    py_bc_blocks: TableLike,
    py_bc_code_units: TableLike,
    *,
    ctx: ExecutionContext | None = None,
) -> FinalizeResult:
    """Normalize CFG block rows and enrich with file/path metadata.

    Parameters
    ----------
    py_bc_blocks:
        Raw bytecode block table.
    py_bc_code_units:
        Bytecode code-unit table containing file/path metadata.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    FinalizeResult
        Finalize bundle with normalized CFG blocks.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = cfg_blocks_plan(py_bc_blocks, py_bc_code_units, ctx=exec_ctx)
    return run_normalize(
        plan=plan,
        post=(),
        contract=CFG_BLOCKS_CONTRACT,
        ctx=exec_ctx,
        metadata_spec=CFG_BLOCKS_NORM_SPEC.metadata_spec,
    )


def build_cfg_edges_result(
    py_bc_code_units: TableLike,
    py_bc_cfg_edges: TableLike,
    *,
    ctx: ExecutionContext | None = None,
) -> FinalizeResult:
    """Normalize CFG edges and enrich with file/path metadata.

    Parameters
    ----------
    py_bc_code_units:
        Bytecode code-unit table containing file/path metadata.
    py_bc_cfg_edges:
        Raw CFG edge table emitted by bytecode extraction.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    FinalizeResult
        Finalize bundle with normalized CFG edges.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = cfg_edges_plan(py_bc_code_units, py_bc_cfg_edges, ctx=exec_ctx)
    return run_normalize(
        plan=plan,
        post=(),
        contract=CFG_EDGES_CONTRACT,
        ctx=exec_ctx,
        metadata_spec=CFG_EDGES_NORM_SPEC.metadata_spec,
    )


def build_cfg_blocks(
    py_bc_blocks: TableLike,
    py_bc_code_units: TableLike,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Normalize CFG block rows and enrich with file/path metadata.

    Parameters
    ----------
    py_bc_blocks:
        Raw bytecode block table.
    py_bc_code_units:
        Bytecode code-unit table containing file/path metadata.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    TableLike
        Normalized CFG block table.
    """
    return build_cfg_blocks_result(py_bc_blocks, py_bc_code_units, ctx=ctx).good


def build_cfg_blocks_canonical(
    py_bc_blocks: TableLike,
    py_bc_code_units: TableLike,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Normalize CFG blocks under canonical determinism.

    Returns
    -------
    TableLike
        Canonicalized CFG block table.
    """
    exec_ctx = ensure_canonical(ensure_execution_context(ctx))
    return build_cfg_blocks_result(py_bc_blocks, py_bc_code_units, ctx=exec_ctx).good


def build_cfg_edges(
    py_bc_code_units: TableLike,
    py_bc_cfg_edges: TableLike,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Normalize CFG edges and enrich with file/path metadata.

    Parameters
    ----------
    py_bc_code_units:
        Bytecode code-unit table containing file/path metadata.
    py_bc_cfg_edges:
        Raw CFG edge table emitted by bytecode extraction.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    TableLike
        Normalized CFG edge table.
    """
    return build_cfg_edges_result(py_bc_code_units, py_bc_cfg_edges, ctx=ctx).good


def build_cfg_edges_canonical(
    py_bc_code_units: TableLike,
    py_bc_cfg_edges: TableLike,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Normalize CFG edges under canonical determinism.

    Returns
    -------
    TableLike
        Canonicalized CFG edge table.
    """
    exec_ctx = ensure_canonical(ensure_execution_context(ctx))
    return build_cfg_edges_result(py_bc_code_units, py_bc_cfg_edges, ctx=exec_ctx).good


def build_cfg(
    py_bc_code_units: TableLike,
    py_bc_cfg_edges: TableLike,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Compatibility wrapper for normalized CFG edges.

    Returns
    -------
    TableLike
        Normalized CFG edges table.
    """
    return build_cfg_edges(py_bc_code_units, py_bc_cfg_edges, ctx=ctx)


__all__ = [
    "build_cfg",
    "build_cfg_blocks",
    "build_cfg_blocks_canonical",
    "build_cfg_blocks_result",
    "build_cfg_edges",
    "build_cfg_edges_canonical",
    "build_cfg_edges_result",
    "cfg_blocks_plan",
    "cfg_edges_plan",
]
