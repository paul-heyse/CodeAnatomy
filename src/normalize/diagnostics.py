"""Normalize diagnostics from extraction layers."""

from __future__ import annotations

from typing import cast

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import run_plan
from normalize import diagnostics_plans
from normalize.catalog import NormalizePlanCatalog
from normalize.diagnostics_plans import (
    DIAG_CONTRACT,
    DIAG_NAME,
    DIAG_SPEC,
    DiagnosticsSources,
)
from normalize.registry_specs import dataset_schema_policy
from normalize.runner import (
    NormalizeFinalizeSpec,
    PostFn,
    compile_normalize_rules,
    ensure_canonical,
    ensure_execution_context,
    run_normalize,
    run_normalize_streamable_contract,
)
from normalize.text_index import RepoTextIndex
from normalize.utils import PlanSource


def diagnostics_plan(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
    ctx: ExecutionContext,
) -> Plan:
    """Build a plan-lane normalized diagnostics table.

    Returns
    -------
    Plan
        Plan producing normalized diagnostics rows.
    """
    return diagnostics_plans.diagnostics_plan(repo_text_index, sources=sources, ctx=ctx)


def diagnostics_post_step(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
) -> PostFn:
    """Return a post step that builds diagnostics in the plan lane.

    Returns
    -------
    PostFn
        Post step that emits a diagnostics table.
    """

    def _apply(table: TableLike, ctx: ExecutionContext) -> TableLike:
        _ = table
        plan = _plan_for_output(repo_text_index, sources=sources, ctx=ctx)
        result = run_plan(
            plan,
            ctx=ctx,
            prefer_reader=False,
            attach_ordering_metadata=True,
        )
        if isinstance(result.value, pa.RecordBatchReader):
            msg = "Expected table result from diagnostics_plan."
            raise TypeError(msg)
        return cast("TableLike", result.value)

    return _apply


def collect_diags_result(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
    ctx: ExecutionContext | None = None,
    profile: str = "default",
) -> FinalizeResult:
    """Aggregate diagnostics into a single normalized table.

    Parameters
    ----------
    repo_text_index:
        Repo text index for line/column to byte offsets.
    sources:
        Diagnostics source tables.
    ctx:
        Optional execution context for plan compilation and finalize.
    profile:
        Execution profile name used when ``ctx`` is not provided.

    Returns
    -------
    FinalizeResult
        Finalize bundle with normalized diagnostics.
    """
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    plan = _plan_for_output(repo_text_index, sources=sources, ctx=exec_ctx)
    finalize_spec = NormalizeFinalizeSpec(
        metadata_spec=DIAG_SPEC.metadata_spec,
        schema_policy=dataset_schema_policy(DIAG_NAME, ctx=exec_ctx),
    )
    return run_normalize(
        plan=plan,
        post=(),
        contract=DIAG_CONTRACT,
        ctx=exec_ctx,
        finalize_spec=finalize_spec,
    )


def collect_diags(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
    ctx: ExecutionContext | None = None,
    profile: str = "default",
) -> TableLike:
    """Aggregate diagnostics into a single normalized table.

    Parameters
    ----------
    repo_text_index:
        Repo text index for line/column to byte offsets.
    sources:
        Diagnostics source tables.
    ctx:
        Optional execution context for plan compilation and finalize.
    profile:
        Execution profile name used when ``ctx`` is not provided.

    Returns
    -------
    TableLike
        Normalized diagnostics table.
    """
    return collect_diags_result(
        repo_text_index,
        sources=sources,
        ctx=ctx,
        profile=profile,
    ).good


def collect_diags_canonical(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
    ctx: ExecutionContext | None = None,
    profile: str = "default",
) -> TableLike:
    """Aggregate diagnostics under canonical determinism.

    Returns
    -------
    TableLike
        Canonicalized diagnostics table.
    """
    exec_ctx = ensure_canonical(ensure_execution_context(ctx, profile=profile))
    return collect_diags_result(
        repo_text_index,
        sources=sources,
        ctx=exec_ctx,
        profile=profile,
    ).good


def collect_diags_streamable(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
    ctx: ExecutionContext | None = None,
    profile: str = "default",
) -> TableLike | RecordBatchReaderLike:
    """Aggregate diagnostics and return a streamable output.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when streamable, otherwise a materialized table.
    """
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    plan = _plan_for_output(repo_text_index, sources=sources, ctx=exec_ctx)
    return run_normalize_streamable_contract(
        plan,
        contract=DIAG_CONTRACT,
        ctx=exec_ctx,
        profile=profile,
    )


def _plan_for_output(
    repo_text_index: RepoTextIndex,
    *,
    sources: DiagnosticsSources,
    ctx: ExecutionContext,
) -> Plan:
    """Compile the diagnostics plan for the output contract.

    Parameters
    ----------
    repo_text_index
        Repository text index for diagnostics.
    sources
        Diagnostics input sources.
    ctx
        Execution context.

    Returns
    -------
    Plan
        Compiled diagnostics plan.

    Raises
    ------
    ValueError
        Raised when the diagnostics output plan is unavailable.
    """
    catalog = NormalizePlanCatalog(
        tables=_diagnostics_catalog_entries(sources),
        repo_text_index=repo_text_index,
    )
    compilation = compile_normalize_rules(catalog, ctx=ctx, required_outputs=(DIAG_NAME,))
    plan = compilation.plans.get(DIAG_NAME)
    if plan is None:
        msg = f"Normalize rule output {DIAG_NAME!r} is not available."
        raise ValueError(msg)
    return cast("Plan", plan)


def _diagnostics_catalog_entries(
    sources: DiagnosticsSources,
) -> dict[str, PlanSource]:
    """Build catalog entries from diagnostics sources.

    Parameters
    ----------
    sources
        Diagnostics input sources.

    Returns
    -------
    dict[str, PlanSource]
        Catalog mapping of dataset name to plan source.
    """
    mapping: dict[str, PlanSource] = {}
    if sources.cst_parse_errors is not None:
        mapping["cst_parse_errors"] = sources.cst_parse_errors
    if sources.ts_errors is not None:
        mapping["ts_errors"] = sources.ts_errors
    if sources.ts_missing is not None:
        mapping["ts_missing"] = sources.ts_missing
    if sources.scip_diagnostics is not None:
        mapping["scip_diagnostics"] = sources.scip_diagnostics
    if sources.scip_documents is not None:
        mapping["scip_documents"] = sources.scip_documents
    return mapping
