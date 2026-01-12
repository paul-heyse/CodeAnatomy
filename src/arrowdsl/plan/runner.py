"""Canonical plan runner and finalize helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, cast

import pyarrow as pa

from arrowdsl.core.context import (
    DeterminismTier,
    ExecutionContext,
    Ordering,
    OrderingKey,
    OrderingLevel,
)
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike, pc
from arrowdsl.plan.plan import Plan, PlanSpec
from arrowdsl.schema.metadata import (
    infer_ordering_keys,
    merge_metadata_specs,
    ordering_metadata_spec,
)
from arrowdsl.schema.schema import SchemaMetadataSpec


@dataclass(frozen=True)
class PlanRunResult:
    """Plan output bundled with materialization metadata."""

    value: TableLike | RecordBatchReaderLike
    kind: Literal["reader", "table"]


def _parse_ordering_keys(schema: SchemaLike) -> list[OrderingKey]:
    metadata = schema.metadata or {}
    raw = metadata.get(b"ordering_keys")
    if not raw:
        return []
    decoded = raw.decode("utf-8")
    keys: list[OrderingKey] = []
    for part in decoded.split(","):
        text = part.strip()
        if not text:
            continue
        if ":" in text:
            col, order = text.split(":", maxsplit=1)
        else:
            col, order = text, "ascending"
        keys.append((col.strip(), order.strip()))
    return keys


def _ordering_keys(schema: SchemaLike) -> list[OrderingKey]:
    keys = _parse_ordering_keys(schema)
    if keys:
        return keys
    return list(infer_ordering_keys(schema.names))


def _apply_canonical_sort(
    table: TableLike,
    *,
    ctx: ExecutionContext,
) -> tuple[TableLike, list[OrderingKey]]:
    if ctx.determinism != DeterminismTier.CANONICAL:
        return table, []
    keys = _ordering_keys(table.schema)
    if not keys:
        return table, []
    indices = pc.sort_indices(table, sort_keys=keys)
    return table.take(indices), keys


def _ordering_metadata_for_plan(
    ordering: Ordering,
    *,
    schema: SchemaLike,
    canonical_keys: Sequence[OrderingKey] | None = None,
) -> SchemaMetadataSpec:
    level = ordering.level
    keys: Sequence[OrderingKey] = ()
    if canonical_keys:
        level = OrderingLevel.EXPLICIT
        keys = canonical_keys
    elif ordering.level == OrderingLevel.EXPLICIT and ordering.keys:
        keys = ordering.keys
    elif ordering.level == OrderingLevel.IMPLICIT:
        keys = _ordering_keys(schema)
    return ordering_metadata_spec(level, keys=keys)


def _pipeline_breaker_spec(pipeline_breakers: Sequence[str]) -> SchemaMetadataSpec:
    if not pipeline_breakers:
        return SchemaMetadataSpec()
    return SchemaMetadataSpec(
        schema_metadata={b"pipeline_breakers": ",".join(pipeline_breakers).encode("utf-8")}
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
    attach_ordering_metadata: bool = False,
) -> PlanRunResult:
    """Materialize or stream a plan with optional metadata updates.

    Returns
    -------
    PlanRunResult
        Plan output and materialization kind.
    """
    spec = PlanSpec.from_plan(plan)
    if (
        prefer_reader
        and not spec.pipeline_breakers
        and ctx.determinism != DeterminismTier.CANONICAL
    ):
        reader = spec.to_reader(ctx=ctx)
        combined = metadata_spec
        if attach_ordering_metadata:
            schema_for_ordering = (
                metadata_spec.apply(reader.schema) if metadata_spec is not None else reader.schema
            )
            ordering_spec = _ordering_metadata_for_plan(
                spec.plan.ordering,
                schema=schema_for_ordering,
            )
            combined = merge_metadata_specs(metadata_spec, ordering_spec)
        return PlanRunResult(
            value=_apply_metadata_spec(reader, metadata_spec=combined),
            kind="reader",
        )
    table = spec.to_table(ctx=ctx)
    table, canonical_keys = _apply_canonical_sort(table, ctx=ctx)
    combined = metadata_spec
    if attach_ordering_metadata:
        schema_for_ordering = (
            metadata_spec.apply(table.schema) if metadata_spec is not None else table.schema
        )
        ordering_spec = _ordering_metadata_for_plan(
            spec.plan.ordering,
            schema=schema_for_ordering,
            canonical_keys=canonical_keys,
        )
        combined = merge_metadata_specs(
            metadata_spec,
            ordering_spec,
            _pipeline_breaker_spec(spec.pipeline_breakers),
        )
    return PlanRunResult(
        value=_apply_metadata_spec(table, metadata_spec=combined),
        kind="table",
    )


def run_plan_bundle(
    plans: Mapping[str, Plan],
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
    metadata_specs: Mapping[str, SchemaMetadataSpec] | None = None,
    attach_ordering_metadata: bool = False,
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


def materialize_plan(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    metadata_spec: SchemaMetadataSpec | None = None,
    attach_ordering_metadata: bool = False,
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


def stream_plan(plan: Plan, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
    """Return a streaming reader for a plan.

    Returns
    -------
    RecordBatchReaderLike
        Streaming reader for the plan.
    """
    return PlanSpec.from_plan(plan).to_reader(ctx=ctx)


__all__ = ["PlanRunResult", "materialize_plan", "run_plan", "run_plan_bundle", "stream_plan"]
