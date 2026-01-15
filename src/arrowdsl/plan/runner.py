"""Canonical plan runner and finalize helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from datafusion import SessionContext

from arrowdsl.core.context import DeterminismTier, ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.ordering_policy import apply_canonical_sort, ordering_metadata_for_plan
from arrowdsl.plan.plan import Plan, PlanRunResult, execute_plan
from arrowdsl.schema.metadata import merge_metadata_specs
from arrowdsl.schema.schema import SchemaMetadataSpec
from config import AdapterMode
from ibis_engine.plan import IbisPlan
from ibis_engine.registry import datafusion_context
from ibis_engine.runner import (
    DataFusionExecutionOptions,
    IbisPlanExecutionOptions,
)
from ibis_engine.runner import (
    materialize_plan as ibis_materialize_plan,
)
from ibis_engine.runner import (
    stream_plan as ibis_stream_plan,
)
from sqlglot_tools.bridge import IbisCompilerBackend

if TYPE_CHECKING:
    from ibis.backends import BaseBackend
    from ibis.expr.types import Value as IbisValue


@dataclass(frozen=True)
class AdapterRunOptions:
    """Options for running plan adapters."""

    adapter_mode: AdapterMode | None = None
    prefer_reader: bool = False
    metadata_spec: SchemaMetadataSpec | None = None
    attach_ordering_metadata: bool = True
    ibis_backend: BaseBackend | None = None
    ibis_params: Mapping[IbisValue, object] | None = None


def _apply_metadata_spec(
    result: TableLike | RecordBatchReaderLike,
    *,
    metadata_spec: SchemaMetadataSpec | None,
) -> TableLike | RecordBatchReaderLike:
    if metadata_spec is None:
        return result
    if not metadata_spec.schema_metadata and not metadata_spec.field_metadata:
        return result
    schema = metadata_spec.apply(result.schema)
    if isinstance(result, pa.RecordBatchReader):
        return pa.RecordBatchReader.from_batches(schema, result)
    table = cast("TableLike", result)
    return table.cast(schema)


def run_plan(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
    metadata_spec: SchemaMetadataSpec | None = None,
    attach_ordering_metadata: bool = True,
) -> PlanRunResult:
    """Materialize or stream a plan with optional metadata updates.

    Returns
    -------
    PlanRunResult
        Plan output and materialization kind.
    """
    return execute_plan(
        plan,
        ctx=ctx,
        prefer_reader=prefer_reader,
        metadata_spec=metadata_spec,
        attach_ordering_metadata=attach_ordering_metadata,
    )


def _run_ibis_plan(
    plan: IbisPlan,
    *,
    ctx: ExecutionContext,
    options: AdapterRunOptions,
) -> PlanRunResult:
    adapter_mode = options.adapter_mode or AdapterMode()
    execution: IbisPlanExecutionOptions | None = None
    if (
        adapter_mode.use_datafusion_bridge
        and options.ibis_backend is not None
        and ctx.runtime.datafusion is not None
    ):
        df_ctx = datafusion_context(options.ibis_backend) or ctx.runtime.datafusion.session_context()
        execution = IbisPlanExecutionOptions(
            params=options.ibis_params,
            datafusion=DataFusionExecutionOptions(
                backend=cast("IbisCompilerBackend", options.ibis_backend),
                ctx=cast("SessionContext", df_ctx),
                runtime_profile=ctx.runtime.datafusion,
                options=None,
                allow_fallback=True,
            ),
        )
    if options.prefer_reader and ctx.determinism != DeterminismTier.CANONICAL:
        reader = (
            ibis_stream_plan(plan, batch_size=None, execution=execution)
            if execution is not None
            else plan.to_reader(params=options.ibis_params)
        )
        combined = options.metadata_spec
        if options.attach_ordering_metadata:
            schema_for_ordering = (
                options.metadata_spec.apply(reader.schema)
                if options.metadata_spec is not None
                else reader.schema
            )
            ordering_spec = ordering_metadata_for_plan(
                plan.ordering,
                schema=schema_for_ordering,
            )
            combined = merge_metadata_specs(options.metadata_spec, ordering_spec)
        return PlanRunResult(
            value=_apply_metadata_spec(reader, metadata_spec=combined),
            kind="reader",
        )
    table = (
        ibis_materialize_plan(plan, execution=execution)
        if execution is not None
        else plan.to_table(params=options.ibis_params)
    )
    table, canonical_keys = apply_canonical_sort(table, determinism=ctx.determinism)
    combined = options.metadata_spec
    if options.attach_ordering_metadata:
        schema_for_ordering = (
            options.metadata_spec.apply(table.schema)
            if options.metadata_spec is not None
            else table.schema
        )
        ordering_spec = ordering_metadata_for_plan(
            plan.ordering,
            schema=schema_for_ordering,
            canonical_keys=canonical_keys,
        )
        combined = merge_metadata_specs(options.metadata_spec, ordering_spec)
    return PlanRunResult(
        value=_apply_metadata_spec(table, metadata_spec=combined),
        kind="table",
    )


def run_plan_adapter(
    plan: Plan | IbisPlan,
    *,
    ctx: ExecutionContext,
    options: AdapterRunOptions | None = None,
) -> PlanRunResult:
    """Materialize a plan or Ibis plan with adapter gating.

    Returns
    -------
    PlanRunResult
        Materialized plan result.

    Raises
    ------
    ValueError
        Raised when the Ibis adapter is disabled for an Ibis plan.
    """
    options = options or AdapterRunOptions()
    adapter_mode = options.adapter_mode or AdapterMode()
    if isinstance(plan, IbisPlan):
        if not adapter_mode.use_ibis_bridge:
            msg = "AdapterMode.use_ibis_bridge is disabled for IbisPlan execution."
            raise ValueError(msg)
        return _run_ibis_plan(
            plan,
            ctx=ctx,
            options=options,
        )
    return run_plan(
        plan,
        ctx=ctx,
        prefer_reader=options.prefer_reader,
        metadata_spec=options.metadata_spec,
        attach_ordering_metadata=options.attach_ordering_metadata,
    )


def run_plan_streamable(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    metadata_spec: SchemaMetadataSpec | None = None,
    attach_ordering_metadata: bool = True,
) -> TableLike | RecordBatchReaderLike:
    """Return a reader when streamable, otherwise materialize the plan.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when no pipeline breakers exist, otherwise a materialized table.
    """
    return run_plan(
        plan,
        ctx=ctx,
        prefer_reader=True,
        metadata_spec=metadata_spec,
        attach_ordering_metadata=attach_ordering_metadata,
    ).value


def run_plan_bundle(
    plans: Mapping[str, Plan],
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
    metadata_specs: Mapping[str, SchemaMetadataSpec] | None = None,
    attach_ordering_metadata: bool = True,
) -> dict[str, TableLike | RecordBatchReaderLike]:
    """Finalize a bundle of plans into tables or readers.

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Finalized plan outputs keyed by name.
    """
    outputs: dict[str, TableLike | RecordBatchReaderLike] = {}
    for name, plan in plans.items():
        spec = metadata_specs.get(name) if metadata_specs is not None else None
        result = run_plan(
            plan,
            ctx=ctx,
            prefer_reader=prefer_reader,
            metadata_spec=spec,
            attach_ordering_metadata=attach_ordering_metadata,
        )
        outputs[name] = result.value
    return outputs


def run_plan_bundle_adapter(
    plans: Mapping[str, Plan | IbisPlan],
    *,
    ctx: ExecutionContext,
    options: AdapterRunOptions | None = None,
    metadata_specs: Mapping[str, SchemaMetadataSpec] | None = None,
) -> dict[str, TableLike | RecordBatchReaderLike]:
    """Finalize a bundle of plans or Ibis plans.

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Finalized outputs keyed by dataset name.
    """
    options = options or AdapterRunOptions()
    outputs: dict[str, TableLike | RecordBatchReaderLike] = {}
    for name, plan in plans.items():
        spec = metadata_specs.get(name) if metadata_specs is not None else None
        run_options = replace(options, metadata_spec=spec)
        result = run_plan_adapter(
            plan,
            ctx=ctx,
            options=run_options,
        )
        outputs[name] = result.value
    return outputs


def materialize_plan(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    metadata_spec: SchemaMetadataSpec | None = None,
    attach_ordering_metadata: bool = True,
) -> TableLike:
    """Materialize a plan as a table.

    Returns
    -------
    TableLike
        Materialized table.

    Raises
    ------
    TypeError
        Raised when a non-table result is returned from finalize.
    """
    result = run_plan(
        plan,
        ctx=ctx,
        prefer_reader=False,
        metadata_spec=metadata_spec,
        attach_ordering_metadata=attach_ordering_metadata,
    )
    if isinstance(result.value, pa.RecordBatchReader):
        msg = "Expected table result from run_plan."
        raise TypeError(msg)
    return cast("TableLike", result.value)


def materialize_plan_adapter(
    plan: Plan | IbisPlan,
    *,
    ctx: ExecutionContext,
    options: AdapterRunOptions | None = None,
) -> TableLike:
    """Materialize a plan or Ibis plan as a table.

    Returns
    -------
    TableLike
        Materialized table output.
    """
    options = options or AdapterRunOptions()
    run_options = replace(options, prefer_reader=False)
    result = run_plan_adapter(
        plan,
        ctx=ctx,
        options=run_options,
    )
    return cast("TableLike", result.value)


def stream_plan(plan: Plan, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
    """Return a streaming reader for a plan.

    Returns
    -------
    RecordBatchReaderLike
        Streaming reader for the plan.

    Raises
    ------
    TypeError
        Raised when plan execution does not yield a streaming reader.
    """
    result = run_plan(plan, ctx=ctx, prefer_reader=True)
    if isinstance(result.value, pa.RecordBatchReader):
        return cast("RecordBatchReaderLike", result.value)
    msg = "Expected streamable plan for PlanIR execution."
    raise TypeError(msg)


__all__ = [
    "AdapterRunOptions",
    "PlanRunResult",
    "materialize_plan",
    "run_plan",
    "run_plan_adapter",
    "run_plan_bundle",
    "run_plan_bundle_adapter",
    "run_plan_streamable",
    "stream_plan",
]
