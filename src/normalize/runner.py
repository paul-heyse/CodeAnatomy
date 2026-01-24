"""Normalize execution helpers for inference-first pipelines."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace

from ibis.expr.types import Value

from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import empty_table
from arrowdsl.schema.metadata import encoding_policy_from_schema, merge_metadata_specs
from arrowdsl.schema.policy import SchemaPolicy, SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import SchemaMetadataSpec
from datafusion_engine.finalize import FinalizeOptions, FinalizeResult, finalize
from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel
from ibis_engine.execution import materialize_ibis_plan
from ibis_engine.execution_factory import ibis_execution_from_ctx
from ibis_engine.plan import IbisPlan
from normalize.runtime import NormalizeRuntime
from schema_spec.system import ContractSpec

PostFn = Callable[[TableLike, ExecutionContext], TableLike]


@dataclass(frozen=True)
class NormalizeFinalizeSpec:
    """Finalize overrides for normalize pipelines."""

    metadata_spec: SchemaMetadataSpec | None = None
    schema_policy: SchemaPolicy | None = None


@dataclass(frozen=True)
class NormalizeRunOptions:
    """Execution options for normalize plans."""

    finalize_spec: NormalizeFinalizeSpec | None = None
    execution_policy: AdapterExecutionPolicy | None = None
    execution_label: ExecutionLabel | None = None
    runtime: NormalizeRuntime | None = None
    params: Mapping[Value, object] | None = None


def ensure_execution_context(
    ctx: ExecutionContext | None,
    *,
    profile: str,
) -> ExecutionContext:
    """Return an execution context, building one when missing.

    Returns
    -------
    ExecutionContext
        Execution context derived from inputs.
    """
    if ctx is not None:
        return ctx
    return execution_context_factory(profile)


def run_normalize(
    *,
    plan: IbisPlan | None,
    post: Sequence[PostFn],
    contract: ContractSpec,
    ctx: ExecutionContext,
    options: NormalizeRunOptions | None = None,
) -> FinalizeResult:
    """Execute a normalize plan and apply finalize gates.

    Returns
    -------
    FinalizeResult
        Finalized output bundle.

    Raises
    ------
    ValueError
        Raised when the normalize runtime is not configured.
    """
    options = options or NormalizeRunOptions()
    runtime = options.runtime
    if runtime is None:
        msg = "Normalize execution requires a NormalizeRuntime."
        raise ValueError(msg)
    contract_obj = contract.to_contract()
    if plan is None:
        table = empty_table(contract_obj.schema)
    else:
        execution = ibis_execution_from_ctx(
            ctx,
            backend=runtime.ibis_backend,
            params=options.params,
            execution_policy=options.execution_policy,
            execution_label=options.execution_label,
        )
        table = materialize_ibis_plan(plan, execution=execution)
        for fn in post:
            table = fn(table, ctx)
    finalize_spec = options.finalize_spec or NormalizeFinalizeSpec()
    metadata = _metadata_with_determinism(finalize_spec.metadata_spec, ctx)
    if finalize_spec.schema_policy is None:
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
    else:
        schema_policy = _merge_policy_metadata(finalize_spec.schema_policy, metadata)
    finalize_options = FinalizeOptions(schema_policy=schema_policy)
    return finalize(table, contract=contract_obj, ctx=ctx, options=finalize_options)


def _metadata_with_determinism(
    metadata_spec: SchemaMetadataSpec | None,
    ctx: ExecutionContext,
) -> SchemaMetadataSpec | None:
    if metadata_spec is None:
        return None
    schema_meta = dict(metadata_spec.schema_metadata)
    schema_meta[b"determinism_tier"] = ctx.determinism.value.encode("utf-8")
    return SchemaMetadataSpec(
        schema_metadata=schema_meta,
        field_metadata=metadata_spec.field_metadata,
    )


def _merge_policy_metadata(
    policy: SchemaPolicy,
    metadata: SchemaMetadataSpec | None,
) -> SchemaPolicy:
    if metadata is None:
        return policy
    merged = merge_metadata_specs(policy.metadata, metadata)
    return replace(policy, metadata=merged)


__all__ = [
    "NormalizeFinalizeSpec",
    "NormalizeRunOptions",
    "ensure_execution_context",
    "run_normalize",
]
