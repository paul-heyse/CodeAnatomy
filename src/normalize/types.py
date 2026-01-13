"""Normalize type expressions into type nodes and edges."""

from __future__ import annotations

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.plan.catalog import PlanCatalog
from arrowdsl.plan.plan import Plan
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
from normalize.types_plans import (
    TYPE_EXPRS_NAME,
    TYPE_NODES_NAME,
    type_exprs_plan,
    type_nodes_plan,
    type_nodes_plan_from_exprs,
    type_nodes_plan_from_scip,
)
from normalize.utils import PlanSource


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
    plan = _plan_for_output(
        TYPE_EXPRS_NAME,
        {"cst_type_exprs": cst_type_exprs},
        ctx=exec_ctx,
    )
    finalize_spec = NormalizeFinalizeSpec(
        metadata_spec=dataset_spec(TYPE_EXPRS_NAME).metadata_spec,
        schema_policy=dataset_schema_policy(TYPE_EXPRS_NAME, ctx=exec_ctx),
    )
    return run_normalize(
        plan=plan,
        post=(),
        contract=dataset_contract(TYPE_EXPRS_NAME),
        ctx=exec_ctx,
        finalize_spec=finalize_spec,
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
    plan = _plan_for_output(
        TYPE_EXPRS_NAME,
        {"cst_type_exprs": cst_type_exprs},
        ctx=exec_ctx,
    )
    return run_normalize_streamable_contract(
        plan, contract=dataset_contract(TYPE_EXPRS_NAME), ctx=exec_ctx
    )


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
    inputs: dict[str, PlanSource] = {"type_exprs_norm_v1": type_exprs_norm}
    if scip_symbol_information is not None:
        inputs["scip_symbol_information"] = scip_symbol_information
    plan = _plan_for_output(TYPE_NODES_NAME, inputs, ctx=exec_ctx)

    finalize_spec = NormalizeFinalizeSpec(
        metadata_spec=dataset_spec(TYPE_NODES_NAME).metadata_spec,
        schema_policy=dataset_schema_policy(TYPE_NODES_NAME, ctx=exec_ctx),
    )
    return run_normalize(
        plan=plan,
        post=(),
        contract=dataset_contract(TYPE_NODES_NAME),
        ctx=exec_ctx,
        finalize_spec=finalize_spec,
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
    inputs: dict[str, PlanSource] = {"type_exprs_norm_v1": type_exprs_norm}
    if scip_symbol_information is not None:
        inputs["scip_symbol_information"] = scip_symbol_information
    plan = _plan_for_output(TYPE_NODES_NAME, inputs, ctx=exec_ctx)
    return run_normalize_streamable_contract(
        plan, contract=dataset_contract(TYPE_NODES_NAME), ctx=exec_ctx
    )


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
    "normalize_type_exprs",
    "normalize_type_exprs_canonical",
    "normalize_type_exprs_result",
    "normalize_type_exprs_streamable",
    "normalize_types",
    "normalize_types_canonical",
    "normalize_types_result",
    "normalize_types_streamable",
    "type_exprs_plan",
    "type_nodes_plan",
    "type_nodes_plan_from_exprs",
    "type_nodes_plan_from_scip",
]
