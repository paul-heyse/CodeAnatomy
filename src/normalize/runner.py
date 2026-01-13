"""Plan-lane runner utilities for normalize pipelines."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import cast

from arrowdsl.core.context import (
    DeterminismTier,
    ExecutionContext,
    OrderingLevel,
    execution_context_factory,
)
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.finalize.finalize import Contract, FinalizeOptions, FinalizeResult, finalize
from arrowdsl.plan.plan import Plan, PlanSpec
from arrowdsl.plan.runner import PlanRunResult, run_plan
from arrowdsl.schema.policy import SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import SchemaMetadataSpec
from normalize.encoding import encoding_policy_from_schema
from normalize.plan_helpers import (
    PlanSource,
    finalize_plan,
    finalize_plan_result,
    plan_source,
)
from schema_spec.specs import PROVENANCE_COLS
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
    return execution_context_factory("default")


def ensure_canonical(ctx: ExecutionContext) -> ExecutionContext:
    """Return an execution context upgraded to canonical determinism.

    Returns
    -------
    ExecutionContext
        Execution context using canonical determinism.
    """
    return ctx.with_determinism(DeterminismTier.CANONICAL)


def _should_skip_canonical_sort(
    plan: Plan,
    *,
    contract: Contract,
    ctx: ExecutionContext,
) -> bool:
    if ctx.determinism != DeterminismTier.CANONICAL:
        return False
    if not contract.canonical_sort:
        return False
    if plan.ordering.level != OrderingLevel.EXPLICIT:
        return False
    expected = tuple((key.column, key.order) for key in contract.canonical_sort)
    if ctx.provenance:
        schema = plan.schema(ctx=ctx)
        for col in PROVENANCE_COLS:
            if col in schema.names:
                expected = (*expected, (col, "ascending"))
    return plan.ordering.keys == expected


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
    result = run_plan(
        plan,
        ctx=ctx,
        prefer_reader=False,
        attach_ordering_metadata=True,
    )
    table = cast("TableLike", result.value)
    for fn in post:
        table = fn(table, ctx)
    contract_obj = contract.to_contract()
    metadata = None
    if metadata_spec is not None:
        schema_meta = dict(metadata_spec.schema_metadata)
        schema_meta[b"determinism_tier"] = ctx.determinism.value.encode("utf-8")
        metadata = SchemaMetadataSpec(
            schema_metadata=schema_meta,
            field_metadata=metadata_spec.field_metadata,
        )
    schema_policy = schema_policy_factory(
        contract.table_schema,
        ctx=ctx,
        options=SchemaPolicyOptions(
            schema=contract_obj.with_versioned_schema(),
            encoding=encoding_policy_from_schema(contract_obj.schema),
            metadata=metadata,
            validation=contract_obj.validation,
        ),
    )
    options = FinalizeOptions(
        schema_policy=schema_policy,
        skip_canonical_sort=_should_skip_canonical_sort(plan, contract=contract_obj, ctx=ctx),
    )
    return finalize(table, contract=contract_obj, ctx=ctx, options=options)


def run_normalize_reader(plan: PlanSource, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
    """Return a streaming reader for a normalize plan.

    Returns
    -------
    RecordBatchReaderLike
        Streaming reader for the plan.
    """
    resolved = plan_source(plan, ctx=ctx)
    return PlanSpec.from_plan(resolved).to_reader(ctx=ctx)


def run_normalize_streamable(
    plan: PlanSource,
    *,
    ctx: ExecutionContext,
    schema: SchemaLike | None = None,
) -> RecordBatchReaderLike | TableLike:
    """Return a reader when no pipeline breakers exist, otherwise a table.

    Notes
    -----
    This path does not apply finalize contracts (alignment, dedupe, or canonical sort).
    Use ``run_normalize`` when contract enforcement is required. When ``schema`` is
    provided, the plan is projected to the schema before materialization.

    Returns
    -------
    RecordBatchReaderLike | TableLike
        Reader when streamable, otherwise a materialized table.
    """
    resolved = plan_source(plan, ctx=ctx)
    return finalize_plan(
        resolved,
        ctx=ctx,
        prefer_reader=True,
        schema=schema,
        keep_extra_columns=ctx.provenance,
    )


def run_normalize_streamable_contract(
    plan: PlanSource,
    *,
    contract: ContractSpec,
    ctx: ExecutionContext | None = None,
) -> RecordBatchReaderLike | TableLike:
    """Return a streamable output aligned to the contract schema.

    Returns
    -------
    RecordBatchReaderLike | TableLike
        Reader when streamable, otherwise a materialized table.
    """
    exec_ctx = ensure_execution_context(ctx)
    schema = contract.to_contract().schema
    return run_normalize_streamable(plan, ctx=exec_ctx, schema=schema)


def run_normalize_streamable_result(
    plan: PlanSource,
    *,
    ctx: ExecutionContext,
    schema: SchemaLike | None = None,
) -> PlanRunResult:
    """Return a reader/table plus materialization metadata.

    Returns
    -------
    PlanRunResult
        Plan output with materialization kind metadata.
    """
    resolved = plan_source(plan, ctx=ctx)
    return finalize_plan_result(
        resolved,
        ctx=ctx,
        prefer_reader=True,
        schema=schema,
        keep_extra_columns=ctx.provenance,
    )


__all__ = [
    "PostFn",
    "ensure_canonical",
    "ensure_execution_context",
    "run_normalize",
    "run_normalize_reader",
    "run_normalize_streamable",
    "run_normalize_streamable_contract",
    "run_normalize_streamable_result",
]
