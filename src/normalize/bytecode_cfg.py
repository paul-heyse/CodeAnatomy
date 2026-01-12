"""Normalize bytecode CFG tables for join-ready use."""

from __future__ import annotations

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.schema import empty_table
from normalize.arrow_utils import join_code_unit_meta
from normalize.schema_infer import align_table_to_schema
from normalize.schemas import CFG_BLOCKS_NORM_SCHEMA, CFG_EDGES_NORM_SCHEMA


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
        Optional execution context (reserved for compatibility).

    Returns
    -------
    TableLike
        Normalized CFG block table.
    """
    if py_bc_blocks.num_rows == 0:
        return empty_table(CFG_BLOCKS_NORM_SCHEMA)

    out = join_code_unit_meta(py_bc_blocks, py_bc_code_units)

    _ = ctx
    return align_table_to_schema(out, CFG_BLOCKS_NORM_SCHEMA)


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
        Optional execution context (reserved for compatibility).

    Returns
    -------
    TableLike
        Normalized CFG edge table.
    """
    if py_bc_cfg_edges.num_rows == 0:
        return empty_table(CFG_EDGES_NORM_SCHEMA)

    out = join_code_unit_meta(py_bc_cfg_edges, py_bc_code_units)

    _ = ctx
    return align_table_to_schema(out, CFG_EDGES_NORM_SCHEMA)


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
