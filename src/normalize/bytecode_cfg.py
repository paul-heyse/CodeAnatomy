"""Normalize bytecode CFG tables for join-ready use."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.empty import empty_table
from arrowdsl.runtime import ExecutionContext
from normalize.schema_infer import align_table_to_schema
from schema_spec.core import ArrowFieldSpec, TableSchemaSpec

CFG_BLOCKS_NORM_SPEC = TableSchemaSpec(
    name="py_bc_blocks_norm_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="block_id", dtype=pa.string()),
        ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
        ArrowFieldSpec(name="start_offset", dtype=pa.int32()),
        ArrowFieldSpec(name="end_offset", dtype=pa.int32()),
        ArrowFieldSpec(name="kind", dtype=pa.string()),
        ArrowFieldSpec(name="file_id", dtype=pa.string()),
        ArrowFieldSpec(name="path", dtype=pa.string()),
    ],
)


CFG_EDGES_NORM_SPEC = TableSchemaSpec(
    name="py_bc_cfg_edges_norm_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="edge_id", dtype=pa.string()),
        ArrowFieldSpec(name="code_unit_id", dtype=pa.string()),
        ArrowFieldSpec(name="src_block_id", dtype=pa.string()),
        ArrowFieldSpec(name="dst_block_id", dtype=pa.string()),
        ArrowFieldSpec(name="kind", dtype=pa.string()),
        ArrowFieldSpec(name="cond_instr_id", dtype=pa.string()),
        ArrowFieldSpec(name="exc_index", dtype=pa.int32()),
        ArrowFieldSpec(name="file_id", dtype=pa.string()),
        ArrowFieldSpec(name="path", dtype=pa.string()),
    ],
)

CFG_BLOCKS_NORM_SCHEMA = CFG_BLOCKS_NORM_SPEC.to_arrow_schema()
CFG_EDGES_NORM_SCHEMA = CFG_EDGES_NORM_SPEC.to_arrow_schema()


def build_cfg_blocks(
    py_bc_blocks: pa.Table,
    py_bc_code_units: pa.Table,
    *,
    ctx: ExecutionContext | None = None,
) -> pa.Table:
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
    pa.Table
        Normalized CFG block table.
    """
    if py_bc_blocks.num_rows == 0:
        return empty_table(CFG_BLOCKS_NORM_SCHEMA)

    out = py_bc_blocks
    if (
        "code_unit_id" in py_bc_blocks.column_names
        and "code_unit_id" in py_bc_code_units.column_names
    ):
        meta_cols = [
            col
            for col in ("code_unit_id", "file_id", "path")
            if col in py_bc_code_units.column_names
        ]
        if meta_cols:
            meta = py_bc_code_units.select(meta_cols)
            out = out.join(meta, keys=["code_unit_id"], join_type="left", use_threads=True)

    _ = ctx
    return align_table_to_schema(out, CFG_BLOCKS_NORM_SCHEMA)


def build_cfg_edges(
    py_bc_code_units: pa.Table,
    py_bc_cfg_edges: pa.Table,
    *,
    ctx: ExecutionContext | None = None,
) -> pa.Table:
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
    pa.Table
        Normalized CFG edge table.
    """
    if py_bc_cfg_edges.num_rows == 0:
        return empty_table(CFG_EDGES_NORM_SCHEMA)

    out = py_bc_cfg_edges
    if (
        "code_unit_id" in py_bc_cfg_edges.column_names
        and "code_unit_id" in py_bc_code_units.column_names
    ):
        meta_cols = [
            col
            for col in ("code_unit_id", "file_id", "path")
            if col in py_bc_code_units.column_names
        ]
        if meta_cols:
            meta = py_bc_code_units.select(meta_cols)
            out = out.join(meta, keys=["code_unit_id"], join_type="left", use_threads=True)

    _ = ctx
    return align_table_to_schema(out, CFG_EDGES_NORM_SCHEMA)


def build_cfg(
    py_bc_code_units: pa.Table,
    py_bc_cfg_edges: pa.Table,
    *,
    ctx: ExecutionContext | None = None,
) -> pa.Table:
    """Compatibility wrapper for normalized CFG edges.

    Returns
    -------
    pa.Table
        Normalized CFG edges table.
    """
    return build_cfg_edges(py_bc_code_units, py_bc_cfg_edges, ctx=ctx)
