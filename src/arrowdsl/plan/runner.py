"""Canonical plan runner and finalize helpers."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from functools import cache
from typing import TYPE_CHECKING, Protocol, TypeGuard, cast

import pyarrow as pa

from arrowdsl.core.context import DeterminismTier, ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.ordering_policy import apply_canonical_sort, ordering_metadata_for_plan
from arrowdsl.plan.plan import Plan, PlanRunResult, execute_plan
from arrowdsl.schema.metadata import merge_metadata_specs
from arrowdsl.schema.schema import SchemaMetadataSpec
from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel

if TYPE_CHECKING:
    from datafusion import SessionContext
    from ibis.backends import BaseBackend
    from ibis.expr.types import Value as IbisValue

    from ibis_engine.plan import IbisPlan
    from ibis_engine.runner import DataFusionExecutionOptions, IbisPlanExecutionOptions
    from sqlglot_tools.bridge import IbisCompilerBackend


class _DatafusionContextFn(Protocol):
    def __call__(self, backend: BaseBackend | None) -> SessionContext | None: ...


class _IbisRunnerModule(Protocol):
    DataFusionExecutionOptions: type[DataFusionExecutionOptions]
    IbisPlanExecutionOptions: type[IbisPlanExecutionOptions]
    materialize_plan: Callable[..., TableLike]
    stream_plan: Callable[..., RecordBatchReaderLike]


@dataclass(frozen=True)
class AdapterRunOptions:
    """Options for running plan adapters."""

    prefer_reader: bool | None = None
    metadata_spec: SchemaMetadataSpec | None = None
    attach_ordering_metadata: bool = True
    execution_policy: AdapterExecutionPolicy | None = None
    execution_label: ExecutionLabel | None = None
    ibis_backend: BaseBackend | None = None
    ibis_params: Mapping[IbisValue, object] | None = None
    ibis_batch_size: int | None = None


def _ibis_plan_type() -> type[IbisPlan]:
    module = importlib.import_module("ibis_engine.plan")
    return cast("type[IbisPlan]", module.IbisPlan)


def _is_ibis_plan(plan: Plan | IbisPlan) -> TypeGuard[IbisPlan]:
    return isinstance(plan, _ibis_plan_type())


def _datafusion_context_fn() -> _DatafusionContextFn:
    module = importlib.import_module("ibis_engine.registry")
    return cast("_DatafusionContextFn", module.datafusion_context)


def _ibis_runner_module() -> _IbisRunnerModule:
    module = importlib.import_module("ibis_engine.runner")
    return cast("_IbisRunnerModule", module)


@dataclass(frozen=True)
class _IbisRuntime:
    datafusion_context: _DatafusionContextFn
    datafusion_options_cls: type[DataFusionExecutionOptions]
    ibis_options_cls: type[IbisPlanExecutionOptions]
    ibis_materialize_plan: Callable[..., TableLike]
    ibis_stream_plan: Callable[..., RecordBatchReaderLike]


@cache
def _ibis_runtime() -> _IbisRuntime:
    runner_module = _ibis_runner_module()
    return _IbisRuntime(
        datafusion_context=_datafusion_context_fn(),
        datafusion_options_cls=runner_module.DataFusionExecutionOptions,
        ibis_options_cls=runner_module.IbisPlanExecutionOptions,
        ibis_materialize_plan=runner_module.materialize_plan,
        ibis_stream_plan=runner_module.stream_plan,
    )


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


def _resolve_prefer_reader(*, ctx: ExecutionContext, prefer_reader: bool | None) -> bool:
    if prefer_reader is not None:
        return prefer_reader
    return ctx.determinism != DeterminismTier.CANONICAL


def _run_ibis_plan(
    plan: IbisPlan,
    *,
    ctx: ExecutionContext,
    options: AdapterRunOptions,
    prefer_reader: bool,
) -> PlanRunResult:
    runtime = _ibis_runtime()
    execution: object | None = None
    if options.ibis_backend is not None and ctx.runtime.datafusion is not None:
        allow_fallback = (
            options.execution_policy.allow_fallback
            if options.execution_policy is not None
            else True
        )
        df_ctx = (
            runtime.datafusion_context(options.ibis_backend)
            or ctx.runtime.datafusion.session_context()
        )
        execution = runtime.ibis_options_cls(
            params=options.ibis_params,
            datafusion=runtime.datafusion_options_cls(
                backend=cast("IbisCompilerBackend", options.ibis_backend),
                ctx=df_ctx,
                runtime_profile=ctx.runtime.datafusion,
                options=None,
                allow_fallback=allow_fallback,
                execution_policy=options.execution_policy,
                execution_label=options.execution_label,
            ),
        )
    if prefer_reader:
        reader = (
            runtime.ibis_stream_plan(plan, batch_size=options.ibis_batch_size, execution=execution)
            if execution is not None
            else plan.to_reader(batch_size=options.ibis_batch_size, params=options.ibis_params)
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
                determinism=ctx.determinism,
            )
            combined = merge_metadata_specs(options.metadata_spec, ordering_spec)
        return PlanRunResult(
            value=_apply_metadata_spec(reader, metadata_spec=combined),
            kind="reader",
        )
    table = (
        runtime.ibis_materialize_plan(plan, execution=execution)
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
            determinism=ctx.determinism,
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

    """
    options = options or AdapterRunOptions()
    prefer_reader = _resolve_prefer_reader(ctx=ctx, prefer_reader=options.prefer_reader)
    if _is_ibis_plan(plan):
        return _run_ibis_plan(
            plan,
            ctx=ctx,
            options=options,
            prefer_reader=prefer_reader,
        )
    return run_plan(
        cast("Plan", plan),
        ctx=ctx,
        prefer_reader=prefer_reader,
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
