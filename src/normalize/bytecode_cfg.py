"""Normalize bytecode CFG tables for join-ready use."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.plan.plan import Plan
from arrowdsl.plan_helpers import code_unit_meta_join, column_or_null_expr, project_to_schema
from normalize.registry_specs import (
    dataset_contract,
    dataset_input_columns,
    dataset_query,
    dataset_schema,
    dataset_spec,
)
from normalize.runner import (
    ensure_canonical,
    ensure_execution_context,
    run_normalize,
    run_normalize_streamable_contract,
)
from normalize.utils import PlanSource, plan_source

CFG_BLOCKS_NAME = "py_bc_blocks_norm_v1"
CFG_EDGES_NAME = "py_bc_cfg_edges_norm_v1"

_META_COLUMNS: tuple[tuple[str, pa.DataType], ...] = (
    ("code_unit_id", pa.string()),
    ("file_id", pa.string()),
    ("path", pa.string()),
)


def _to_plan(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    columns: Sequence[str] | None = None,
) -> Plan:
    return plan_source(source, ctx=ctx, columns=columns)


def _code_unit_meta_plan(code_units: PlanSource, *, ctx: ExecutionContext) -> Plan:
    names = [name for name, _ in _META_COLUMNS]
    plan = _to_plan(code_units, ctx=ctx, columns=names)
    available = set(plan.schema(ctx=ctx).names)
    exprs = [column_or_null_expr(name, dtype, available=available) for name, dtype in _META_COLUMNS]
    return plan.project(exprs, names, ctx=ctx)


def cfg_blocks_plan(
    py_bc_blocks: PlanSource,
    py_bc_code_units: PlanSource,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Normalize CFG block rows and enrich with file/path metadata.

    Returns
    -------
    Plan
        Plan producing normalized CFG block rows.
    """
    blocks = _to_plan(py_bc_blocks, ctx=ctx, columns=dataset_input_columns(CFG_BLOCKS_NAME))
    blocks_available = set(blocks.schema(ctx=ctx).names)
    code_units = _to_plan(py_bc_code_units, ctx=ctx, columns=[name for name, _ in _META_COLUMNS])
    code_available = set(code_units.schema(ctx=ctx).names)

    if "code_unit_id" in blocks_available and "code_unit_id" in code_available:
        meta = _code_unit_meta_plan(code_units, ctx=ctx)
        joined = code_unit_meta_join(blocks, meta, ctx=ctx)
    else:
        joined = blocks

    joined = project_to_schema(
        joined,
        schema=dataset_schema(CFG_BLOCKS_NAME),
        ctx=ctx,
        keep_extra_columns=True,
    )
    return dataset_query(CFG_BLOCKS_NAME).apply_to_plan(joined, ctx=ctx)


def cfg_edges_plan(
    py_bc_code_units: PlanSource,
    py_bc_cfg_edges: PlanSource,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Normalize CFG edge rows and enrich with file/path metadata.

    Returns
    -------
    Plan
        Plan producing normalized CFG edge rows.
    """
    edges = _to_plan(py_bc_cfg_edges, ctx=ctx, columns=dataset_input_columns(CFG_EDGES_NAME))
    edges_available = set(edges.schema(ctx=ctx).names)
    code_units = _to_plan(py_bc_code_units, ctx=ctx, columns=[name for name, _ in _META_COLUMNS])
    code_available = set(code_units.schema(ctx=ctx).names)

    if "code_unit_id" in edges_available and "code_unit_id" in code_available:
        meta = _code_unit_meta_plan(code_units, ctx=ctx)
        joined = code_unit_meta_join(edges, meta, ctx=ctx)
    else:
        joined = edges

    joined = project_to_schema(
        joined,
        schema=dataset_schema(CFG_EDGES_NAME),
        ctx=ctx,
        keep_extra_columns=True,
    )
    return dataset_query(CFG_EDGES_NAME).apply_to_plan(joined, ctx=ctx)


def build_cfg_blocks_result(
    py_bc_blocks: PlanSource,
    py_bc_code_units: PlanSource,
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
        contract=dataset_contract(CFG_BLOCKS_NAME),
        ctx=exec_ctx,
        metadata_spec=dataset_spec(CFG_BLOCKS_NAME).metadata_spec,
    )


def build_cfg_edges_result(
    py_bc_code_units: PlanSource,
    py_bc_cfg_edges: PlanSource,
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
        contract=dataset_contract(CFG_EDGES_NAME),
        ctx=exec_ctx,
        metadata_spec=dataset_spec(CFG_EDGES_NAME).metadata_spec,
    )


def build_cfg_blocks(
    py_bc_blocks: PlanSource,
    py_bc_code_units: PlanSource,
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
    py_bc_blocks: PlanSource,
    py_bc_code_units: PlanSource,
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
    py_bc_code_units: PlanSource,
    py_bc_cfg_edges: PlanSource,
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
    py_bc_code_units: PlanSource,
    py_bc_cfg_edges: PlanSource,
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


def build_cfg_blocks_streamable(
    py_bc_blocks: PlanSource,
    py_bc_code_units: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Normalize CFG blocks and return a streamable output.

    Returns
    -------
    TableLike | pa.RecordBatchReader
        Reader when streamable, otherwise a materialized table.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = cfg_blocks_plan(py_bc_blocks, py_bc_code_units, ctx=exec_ctx)
    return run_normalize_streamable_contract(
        plan,
        contract=dataset_contract(CFG_BLOCKS_NAME),
        ctx=exec_ctx,
    )


def build_cfg_edges_streamable(
    py_bc_code_units: PlanSource,
    py_bc_cfg_edges: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Normalize CFG edges and return a streamable output.

    Returns
    -------
    TableLike | pa.RecordBatchReader
        Reader when streamable, otherwise a materialized table.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = cfg_edges_plan(py_bc_code_units, py_bc_cfg_edges, ctx=exec_ctx)
    return run_normalize_streamable_contract(
        plan,
        contract=dataset_contract(CFG_EDGES_NAME),
        ctx=exec_ctx,
    )


def build_cfg_streamable(
    py_bc_code_units: PlanSource,
    py_bc_cfg_edges: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Compatibility wrapper for streamable CFG edges.

    Returns
    -------
    TableLike | pa.RecordBatchReader
        Streamable CFG edges output.
    """
    return build_cfg_edges_streamable(py_bc_code_units, py_bc_cfg_edges, ctx=ctx)


def build_cfg(
    py_bc_code_units: PlanSource,
    py_bc_cfg_edges: PlanSource,
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
    "build_cfg_blocks_streamable",
    "build_cfg_edges",
    "build_cfg_edges_canonical",
    "build_cfg_edges_result",
    "build_cfg_edges_streamable",
    "build_cfg_streamable",
    "cfg_blocks_plan",
    "cfg_edges_plan",
]
