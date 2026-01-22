"""Execution helpers for Ibis plans."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast, overload

import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.core.ordering import Ordering, OrderingKey, OrderingLevel
from arrowdsl.schema.metadata import (
    infer_ordering_keys,
    merge_metadata_specs,
    ordering_from_schema,
    ordering_metadata_spec,
)
from arrowdsl.schema.schema import SchemaMetadataSpec
from engine.runtime_profile import runtime_profile_snapshot

if TYPE_CHECKING:
    from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel
from ibis_engine.plan import IbisPlan
from ibis_engine.runner import (
    DataFusionExecutionOptions,
    IbisCachePolicy,
    IbisPlanExecutionOptions,
    materialize_plan,
    stream_plan,
)
from sqlglot_tools.bridge import IbisCompilerBackend


@dataclass(frozen=True)
class IbisExecutionContext:
    """Execution context for Ibis plans."""

    ctx: ExecutionContext
    execution_policy: AdapterExecutionPolicy | None = None
    execution_label: ExecutionLabel | None = None
    ibis_backend: BaseBackend | None = None
    params: Mapping[IbisValue, object] | None = None
    batch_size: int | None = None
    probe_capabilities: bool = True
    cache_policy: IbisCachePolicy | None = None

    def __post_init__(self) -> None:
        """Apply runtime defaults after initialization."""
        if self.batch_size is not None:
            return
        runtime_batch = self.ctx.runtime.scan.batch_size
        if runtime_batch is not None:
            object.__setattr__(self, "batch_size", runtime_batch)

    def plan_options(self) -> IbisPlanExecutionOptions:
        """Build plan execution options for the current context.

        Returns
        -------
        IbisPlanExecutionOptions
            Execution options including DataFusion overrides when available.
        """
        if self.ibis_backend is None or self.ctx.runtime.datafusion is None:
            return IbisPlanExecutionOptions(params=self.params)
        runtime_profile = self.ctx.runtime.datafusion
        runtime_snapshot = runtime_profile_snapshot(self.ctx.runtime)
        datafusion = DataFusionExecutionOptions(
            backend=cast("IbisCompilerBackend", self.ibis_backend),
            ctx=runtime_profile.session_context(),
            runtime_profile=runtime_profile,
            runtime_profile_hash=runtime_snapshot.profile_hash,
            execution_policy=self.execution_policy,
            execution_label=self.execution_label,
            probe_capabilities=self.probe_capabilities,
        )
        return IbisPlanExecutionOptions(
            params=self.params,
            datafusion=datafusion,
            cache_policy=self.cache_policy,
        )


def materialize_ibis_plan(plan: IbisPlan, *, execution: IbisExecutionContext) -> TableLike:
    """Materialize an Ibis plan using the execution context.

    Returns
    -------
    TableLike
        Materialized Arrow table.
    """
    table = materialize_plan(plan, execution=execution.plan_options())
    return _apply_ordering_metadata(table, plan=plan, ctx=execution.ctx)


def stream_ibis_plan(
    plan: IbisPlan,
    *,
    execution: IbisExecutionContext,
) -> RecordBatchReaderLike:
    """Return a RecordBatchReader for an Ibis plan.

    Returns
    -------
    RecordBatchReaderLike
        Streamed reader for the plan results.
    """
    reader = stream_plan(
        plan,
        batch_size=execution.batch_size,
        execution=execution.plan_options(),
    )
    return _apply_ordering_metadata(reader, plan=plan, ctx=execution.ctx)


@overload
def _apply_ordering_metadata(
    result: TableLike,
    *,
    plan: IbisPlan,
    ctx: ExecutionContext,
) -> TableLike: ...


@overload
def _apply_ordering_metadata(
    result: RecordBatchReaderLike,
    *,
    plan: IbisPlan,
    ctx: ExecutionContext,
) -> RecordBatchReaderLike: ...


def _apply_ordering_metadata(
    result: TableLike | RecordBatchReaderLike,
    *,
    plan: IbisPlan,
    ctx: ExecutionContext,
) -> TableLike | RecordBatchReaderLike:
    if isinstance(result, RecordBatchReaderLike):
        ordering_spec = _ordering_metadata_for_plan(
            plan.ordering,
            schema=result.schema,
            determinism=ctx.determinism,
        )
        combined = merge_metadata_specs(ordering_spec)
        return _apply_metadata_spec(result, metadata_spec=combined)
    table, canonical_keys = _apply_canonical_sort(result, determinism=ctx.determinism)
    ordering_spec = _ordering_metadata_for_plan(
        plan.ordering,
        schema=table.schema,
        canonical_keys=canonical_keys,
        determinism=ctx.determinism,
    )
    combined = merge_metadata_specs(ordering_spec)
    return _apply_metadata_spec(table, metadata_spec=combined)


@overload
def _apply_metadata_spec(
    result: TableLike,
    *,
    metadata_spec: SchemaMetadataSpec | None,
) -> TableLike: ...


@overload
def _apply_metadata_spec(
    result: RecordBatchReaderLike,
    *,
    metadata_spec: SchemaMetadataSpec | None,
) -> RecordBatchReaderLike: ...


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


def _ordering_keys_for_schema(schema: pa.Schema) -> tuple[OrderingKey, ...]:
    ordering = ordering_from_schema(schema)
    if ordering.keys:
        return ordering.keys
    return infer_ordering_keys(schema.names)


def _apply_canonical_sort(
    table: TableLike,
    *,
    determinism: DeterminismTier,
) -> tuple[TableLike, tuple[OrderingKey, ...]]:
    if determinism != DeterminismTier.CANONICAL:
        return table, ()
    keys = _ordering_keys_for_schema(table.schema)
    if not keys:
        return table, ()
    if not isinstance(table, pa.Table):
        return table, ()
    table_cast = cast("pa.Table", table)
    sorted_table = table_cast.sort_by(list(keys))
    return cast("TableLike", sorted_table), tuple(keys)


def _ordering_metadata_for_plan(
    ordering: Ordering,
    *,
    schema: pa.Schema,
    canonical_keys: tuple[OrderingKey, ...] | None = None,
    determinism: DeterminismTier | None = None,
) -> SchemaMetadataSpec:
    level = ordering.level
    keys: tuple[OrderingKey, ...] = ()
    if canonical_keys:
        level = OrderingLevel.EXPLICIT
        keys = canonical_keys
    elif ordering.level == OrderingLevel.EXPLICIT and ordering.keys:
        keys = ordering.keys
    elif ordering.level == OrderingLevel.IMPLICIT:
        keys = _ordering_keys_for_schema(schema)
    extra: dict[bytes, bytes] | None = None
    if determinism is not None:
        extra = {b"determinism_tier": determinism.value.encode("utf-8")}
    return ordering_metadata_spec(level, keys=keys, extra=extra)


__all__ = ["IbisExecutionContext", "materialize_ibis_plan", "stream_ibis_plan"]
