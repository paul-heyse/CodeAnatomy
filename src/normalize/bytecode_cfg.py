"""Normalize bytecode CFG tables for join-ready use."""

from __future__ import annotations

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.plan.catalog import PlanCatalog
from arrowdsl.plan.plan import Plan
from normalize.bytecode_cfg_plans import (
    CFG_BLOCKS_NAME,
    CFG_EDGES_NAME,
    cfg_blocks_plan,
    cfg_edges_plan,
)
from normalize.registry_specs import (
    dataset_contract,
    dataset_schema_policy,
    dataset_spec,
)
from normalize.runner import (
    NormalizeFinalizeSpec,
    compile_normalize_rules,
    ensure_canonical,
    ensure_execution_context,
    run_normalize,
    run_normalize_streamable_contract,
)
from normalize.utils import PlanSource


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
    plan = _plan_for_output(
        CFG_BLOCKS_NAME,
        {
            "py_bc_blocks": py_bc_blocks,
            "py_bc_code_units": py_bc_code_units,
        },
        ctx=exec_ctx,
    )
    finalize_spec = NormalizeFinalizeSpec(
        metadata_spec=dataset_spec(CFG_BLOCKS_NAME).metadata_spec,
        schema_policy=dataset_schema_policy(CFG_BLOCKS_NAME, ctx=exec_ctx),
    )
    return run_normalize(
        plan=plan,
        post=(),
        contract=dataset_contract(CFG_BLOCKS_NAME),
        ctx=exec_ctx,
        finalize_spec=finalize_spec,
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
    plan = _plan_for_output(
        CFG_EDGES_NAME,
        {
            "py_bc_cfg_edges": py_bc_cfg_edges,
            "py_bc_code_units": py_bc_code_units,
        },
        ctx=exec_ctx,
    )
    finalize_spec = NormalizeFinalizeSpec(
        metadata_spec=dataset_spec(CFG_EDGES_NAME).metadata_spec,
        schema_policy=dataset_schema_policy(CFG_EDGES_NAME, ctx=exec_ctx),
    )
    return run_normalize(
        plan=plan,
        post=(),
        contract=dataset_contract(CFG_EDGES_NAME),
        ctx=exec_ctx,
        finalize_spec=finalize_spec,
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
    plan = _plan_for_output(
        CFG_BLOCKS_NAME,
        {
            "py_bc_blocks": py_bc_blocks,
            "py_bc_code_units": py_bc_code_units,
        },
        ctx=exec_ctx,
    )
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
    plan = _plan_for_output(
        CFG_EDGES_NAME,
        {
            "py_bc_cfg_edges": py_bc_cfg_edges,
            "py_bc_code_units": py_bc_code_units,
        },
        ctx=exec_ctx,
    )
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


def _plan_for_output(
    output_name: str,
    inputs: dict[str, PlanSource],
    *,
    ctx: ExecutionContext,
) -> Plan:
    catalog = PlanCatalog(tables=dict(inputs))
    compilation = compile_normalize_rules(catalog, ctx=ctx, required_outputs=(output_name,))
    plan = compilation.plans.get(output_name)
    if plan is None:
        msg = f"Normalize rule output {output_name!r} is not available."
        raise ValueError(msg)
    return plan


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
