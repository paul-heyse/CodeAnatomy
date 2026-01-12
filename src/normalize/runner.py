"""Plan-lane runner utilities for normalize pipelines."""

from __future__ import annotations

from collections.abc import Callable, Iterable

from arrowdsl.core.context import DeterminismTier, ExecutionContext, RuntimeProfile
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.finalize.finalize import FinalizeOptions, FinalizeResult, finalize
from arrowdsl.plan.plan import Plan, PlanSpec
from arrowdsl.schema.schema import SchemaMetadataSpec
from normalize.encoding import encoding_policy_from_schema
from normalize.plan_helpers import finalize_plan
from schema_spec.system import ContractSpec

PostFn = Callable[[TableLike, ExecutionContext], TableLike]


def ensure_execution_context(ctx: ExecutionContext | None) -> ExecutionContext:
    """Return a normalized execution context.

    Returns
    -------
    ExecutionContext
        Provided context or a default context when missing.
    """
    if ctx is not None:
        return ctx
    return ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))


def ensure_canonical(ctx: ExecutionContext) -> ExecutionContext:
    """Return an execution context upgraded to canonical determinism.

    Returns
    -------
    ExecutionContext
        Execution context using canonical determinism.
    """
    runtime = ctx.runtime.with_determinism(DeterminismTier.CANONICAL)
    return ExecutionContext(
        runtime=runtime,
        mode=ctx.mode,
        provenance=ctx.provenance,
        safe_cast=ctx.safe_cast,
        debug=ctx.debug,
        schema_validation=ctx.schema_validation,
    )


def run_normalize(
    *,
    plan: Plan,
    post: Iterable[PostFn],
    contract: ContractSpec,
    ctx: ExecutionContext,
    metadata_spec: SchemaMetadataSpec | None = None,
) -> FinalizeResult:
    """Execute a normalize plan with post steps and finalize gate.

    Returns
    -------
    FinalizeResult
        Finalize bundle with good/errors/stats/alignment outputs.
    """
    table = PlanSpec.from_plan(plan).to_table(ctx=ctx)
    for fn in post:
        table = fn(table, ctx)
    contract_obj = contract.to_contract()
    options = FinalizeOptions(encoding_policy=encoding_policy_from_schema(contract_obj.schema))
    result = finalize(table, contract=contract_obj, ctx=ctx, options=options)
    if metadata_spec is None:
        return result
    schema_meta = dict(metadata_spec.schema_metadata)
    schema_meta[b"determinism_tier"] = ctx.determinism.value.encode("utf-8")
    merged_spec = SchemaMetadataSpec(
        schema_metadata=schema_meta,
        field_metadata=metadata_spec.field_metadata,
    )
    schema = merged_spec.apply(result.good.schema)
    good = result.good.cast(schema)
    return FinalizeResult(
        good=good,
        errors=result.errors,
        stats=result.stats,
        alignment=result.alignment,
    )


def run_normalize_reader(plan: Plan, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
    """Return a streaming reader for a normalize plan.

    Returns
    -------
    RecordBatchReaderLike
        Streaming reader for the plan.
    """
    return PlanSpec.from_plan(plan).to_reader(ctx=ctx)


def run_normalize_streamable(
    plan: Plan, *, ctx: ExecutionContext
) -> RecordBatchReaderLike | TableLike:
    """Return a reader when no pipeline breakers exist, otherwise a table.

    Notes
    -----
    This path does not apply finalize contracts (alignment, dedupe, or canonical sort).
    Use ``run_normalize`` when contract enforcement is required.

    Returns
    -------
    RecordBatchReaderLike | TableLike
        Reader when streamable, otherwise a materialized table.
    """
    return finalize_plan(plan, ctx=ctx, prefer_reader=True)


__all__ = [
    "PostFn",
    "ensure_canonical",
    "ensure_execution_context",
    "run_normalize",
    "run_normalize_reader",
    "run_normalize_streamable",
]
