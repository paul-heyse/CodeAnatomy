"""Plan builders for normalized bytecode CFG tables."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.plan.plan import Plan
from arrowdsl.plan_helpers import (
    code_unit_meta_join,
    column_or_null_expr,
    project_to_schema,
)
from normalize.registry_specs import dataset_input_columns, dataset_query, dataset_schema
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
    blocks = _to_plan(
        py_bc_blocks,
        ctx=ctx,
        columns=dataset_input_columns(CFG_BLOCKS_NAME),
    )
    blocks_available = set(blocks.schema(ctx=ctx).names)
    code_units = _to_plan(
        py_bc_code_units,
        ctx=ctx,
        columns=[name for name, _ in _META_COLUMNS],
    )
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
    edges = _to_plan(
        py_bc_cfg_edges,
        ctx=ctx,
        columns=dataset_input_columns(CFG_EDGES_NAME),
    )
    edges_available = set(edges.schema(ctx=ctx).names)
    code_units = _to_plan(
        py_bc_code_units,
        ctx=ctx,
        columns=[name for name, _ in _META_COLUMNS],
    )
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


__all__ = [
    "CFG_BLOCKS_NAME",
    "CFG_EDGES_NAME",
    "cfg_blocks_plan",
    "cfg_edges_plan",
]
