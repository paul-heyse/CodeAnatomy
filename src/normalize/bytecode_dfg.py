"""Derive bytecode def/use events and reaching-def edges."""

from __future__ import annotations

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.plan.catalog import PlanCatalog
from arrowdsl.plan.plan import Plan
from normalize.bytecode_dfg_plans import (
    DEF_USE_NAME,
    REACHES_NAME,
    def_use_events_plan,
    reaching_defs_plan,
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


def build_def_use_events_result(
    py_bc_instructions: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> FinalizeResult:
    """Build def/use events from bytecode instruction rows.

    Parameters
    ----------
    py_bc_instructions:
        Bytecode instruction table with opname and argval data.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    FinalizeResult
        Finalize bundle with def/use events.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = _plan_for_output(
        DEF_USE_NAME,
        {"py_bc_instructions": py_bc_instructions},
        ctx=exec_ctx,
    )
    finalize_spec = NormalizeFinalizeSpec(
        metadata_spec=dataset_spec(DEF_USE_NAME).metadata_spec,
        schema_policy=dataset_schema_policy(DEF_USE_NAME, ctx=exec_ctx),
    )
    return run_normalize(
        plan=plan,
        post=(),
        contract=dataset_contract(DEF_USE_NAME),
        ctx=exec_ctx,
        finalize_spec=finalize_spec,
    )


def build_def_use_events(
    py_bc_instructions: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Build def/use events from bytecode instruction rows.

    Parameters
    ----------
    py_bc_instructions:
        Bytecode instruction table with opname and argval data.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    TableLike
        Def/use events table.
    """
    return build_def_use_events_result(py_bc_instructions, ctx=ctx).good


def build_def_use_events_canonical(
    py_bc_instructions: TableLike,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Build def/use events under canonical determinism.

    Returns
    -------
    TableLike
        Canonicalized def/use events table.
    """
    exec_ctx = ensure_canonical(ensure_execution_context(ctx))
    return build_def_use_events_result(py_bc_instructions, ctx=exec_ctx).good


def build_def_use_events_streamable(
    py_bc_instructions: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Build def/use events with a streamable output.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when streamable, otherwise a materialized table.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = _plan_for_output(
        DEF_USE_NAME,
        {"py_bc_instructions": py_bc_instructions},
        ctx=exec_ctx,
    )
    return run_normalize_streamable_contract(
        plan, contract=dataset_contract(DEF_USE_NAME), ctx=exec_ctx
    )


def run_reaching_defs_result(
    def_use_events: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> FinalizeResult:
    """Compute a best-effort reaching-defs edge table.

    This is a conservative, symbol-matching approximation that joins definitions to uses
    within the same code unit. It is deterministic and safe for early-stage analysis.

    Parameters
    ----------
    def_use_events:
        Def/use events table.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    FinalizeResult
        Finalize bundle with reaching-def edges.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = _plan_for_output(
        REACHES_NAME,
        {"py_bc_def_use_events_v1": def_use_events},
        ctx=exec_ctx,
    )
    finalize_spec = NormalizeFinalizeSpec(
        metadata_spec=dataset_spec(REACHES_NAME).metadata_spec,
        schema_policy=dataset_schema_policy(REACHES_NAME, ctx=exec_ctx),
    )
    return run_normalize(
        plan=plan,
        post=(),
        contract=dataset_contract(REACHES_NAME),
        ctx=exec_ctx,
        finalize_spec=finalize_spec,
    )


def run_reaching_defs(
    def_use_events: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Compute a best-effort reaching-defs edge table.

    This is a conservative, symbol-matching approximation that joins definitions to uses
    within the same code unit. It is deterministic and safe for early-stage analysis.

    Parameters
    ----------
    def_use_events:
        Def/use events table.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    TableLike
        Reaching-def edges table.
    """
    return run_reaching_defs_result(def_use_events, ctx=ctx).good


def run_reaching_defs_canonical(
    def_use_events: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Compute reaching-def edges under canonical determinism.

    Returns
    -------
    TableLike
        Canonicalized reaching-def edges.
    """
    exec_ctx = ensure_canonical(ensure_execution_context(ctx))
    return run_reaching_defs_result(def_use_events, ctx=exec_ctx).good


def run_reaching_defs_streamable(
    def_use_events: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Compute reaching-def edges with a streamable output.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when streamable, otherwise a materialized table.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = _plan_for_output(
        REACHES_NAME,
        {"py_bc_def_use_events_v1": def_use_events},
        ctx=exec_ctx,
    )
    return run_normalize_streamable_contract(
        plan,
        contract=dataset_contract(REACHES_NAME),
        ctx=exec_ctx,
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
    "build_def_use_events",
    "build_def_use_events_canonical",
    "build_def_use_events_result",
    "build_def_use_events_streamable",
    "def_use_events_plan",
    "reaching_defs_plan",
    "run_reaching_defs",
    "run_reaching_defs_canonical",
    "run_reaching_defs_result",
    "run_reaching_defs_streamable",
]
