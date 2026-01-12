"""Shared table construction helpers for extractors."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import cast

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
from arrowdsl.plan_helpers import (
    flatten_struct_field,
    project_columns,
    query_for_schema,
)
from arrowdsl.schema.schema import (
    SchemaEvolutionSpec,
    SchemaMetadataSpec,
    SchemaTransform,
    empty_table,
    projection_for_schema,
)
from extract.spec_helpers import infer_ordering_keys, merge_metadata_specs


def rows_to_table(rows: Sequence[Mapping[str, object]], schema: SchemaLike) -> TableLike:
    """Build a table from row mappings or return an empty table.

    Returns
    -------
    TableLike
        Table constructed from rows or an empty table.
    """
    if not rows:
        return empty_table(schema)
    return pa.Table.from_pylist(list(rows), schema=schema)


def align_table(table: TableLike, *, schema: SchemaLike) -> TableLike:
    """Align a table to a target schema.

    Returns
    -------
    TableLike
        Aligned table.
    """
    return SchemaTransform(schema=schema).apply(table)


def iter_record_batches(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike,
    batch_size: int = 4096,
) -> Iterator[pa.RecordBatch]:
    """Yield RecordBatches from row mappings.

    Yields
    ------
    pyarrow.RecordBatch
        Record batches built from buffered rows.
    """
    buffer: list[Mapping[str, object]] = []
    for row in rows:
        buffer.append(row)
        if len(buffer) >= batch_size:
            yield pa.RecordBatch.from_pylist(buffer, schema=schema)
            buffer.clear()
    if buffer:
        yield pa.RecordBatch.from_pylist(buffer, schema=schema)


def reader_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike,
    batch_size: int = 4096,
) -> RecordBatchReaderLike:
    """Build a RecordBatchReader from row mappings.

    Returns
    -------
    pyarrow.RecordBatchReader
        Reader streaming record batches.
    """
    batches = iter_record_batches(rows, schema=schema, batch_size=batch_size)
    return pa.RecordBatchReader.from_batches(schema, batches)


def plan_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike,
    batch_size: int = 4096,
    label: str = "",
) -> Plan:
    """Create a Plan from row mappings via a RecordBatchReader.

    Returns
    -------
    Plan
        Plan backed by a record batch reader.
    """
    reader = reader_from_rows(rows, schema=schema, batch_size=batch_size)
    return Plan.from_reader(reader, label=label)


def align_plan(
    plan: Plan,
    *,
    schema: SchemaLike,
    available: Sequence[str] | None = None,
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Return a plan aligned to the target schema via projection.

    Returns
    -------
    Plan
        Plan projecting/casting columns to the schema.
    """
    if available is None:
        available = schema.names if ctx is None else plan.schema(ctx=ctx).names
    safe_cast = True if ctx is None else ctx.safe_cast
    exprs, names = projection_for_schema(schema, available=available, safe_cast=safe_cast)
    return plan.project(exprs, names, ctx=ctx)


def _metadata_spec_from_schema(schema: SchemaLike) -> SchemaMetadataSpec:
    schema_meta = dict(schema.metadata or {})
    field_meta = {
        field.name: dict(field.metadata or {}) for field in schema if field.metadata is not None
    }
    return SchemaMetadataSpec(schema_metadata=schema_meta, field_metadata=field_meta)


def unify_schemas(
    schemas: Sequence[SchemaLike],
    *,
    promote_options: str = "permissive",
) -> SchemaLike:
    """Unify schemas while preserving metadata from the first schema.

    Returns
    -------
    SchemaLike
        Unified schema with metadata preserved.
    """
    if not schemas:
        return pa.schema([])
    evolution = SchemaEvolutionSpec(promote_options=promote_options)
    unified = evolution.unify_schema_from_schemas(schemas)
    return _metadata_spec_from_schema(schemas[0]).apply(unified)


def unify_tables(
    tables: Sequence[TableLike],
    *,
    promote_options: str = "permissive",
) -> TableLike:
    """Unify and concatenate tables with metadata-aware schema alignment.

    Returns
    -------
    TableLike
        Concatenated table aligned to the unified schema.
    """
    if not tables:
        return empty_table(pa.schema([]))
    schema = unify_schemas([table.schema for table in tables], promote_options=promote_options)
    aligned = [table.cast(schema) for table in tables]
    return pa.concat_tables(aligned)


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


def _ordering_metadata_spec(
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
    meta = {b"ordering_level": level.value.encode("utf-8")}
    if keys:
        key_text = ",".join(f"{col}:{order}" for col, order in keys)
        meta[b"ordering_keys"] = key_text.encode("utf-8")
    return SchemaMetadataSpec(schema_metadata=meta)


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


def materialize_plan(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    metadata_spec: SchemaMetadataSpec | None = None,
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
    result = finalize_plan(
        plan,
        ctx=ctx,
        prefer_reader=False,
        metadata_spec=metadata_spec,
    )
    if isinstance(result, pa.RecordBatchReader):
        msg = "Expected table result from finalize_plan."
        raise TypeError(msg)
    return cast("TableLike", result)


def stream_plan(plan: Plan, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
    """Return a streaming reader for a plan.

    Returns
    -------
    RecordBatchReaderLike
        Streaming reader for the plan.
    """
    return PlanSpec.from_plan(plan).to_reader(ctx=ctx)


def finalize_plan(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
    metadata_spec: SchemaMetadataSpec | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Return a reader when possible, otherwise materialize the plan.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when allowed, otherwise a table.
    """
    spec = PlanSpec.from_plan(plan)
    if (
        prefer_reader
        and not spec.pipeline_breakers
        and ctx.determinism != DeterminismTier.CANONICAL
    ):
        reader = spec.to_reader(ctx=ctx)
        schema_for_ordering = (
            metadata_spec.apply(reader.schema) if metadata_spec is not None else reader.schema
        )
        ordering_spec = _ordering_metadata_spec(
            spec.plan.ordering,
            schema=schema_for_ordering,
        )
        combined = merge_metadata_specs(metadata_spec, ordering_spec)
        return _apply_metadata_spec(reader, metadata_spec=combined)
    table = spec.to_table(ctx=ctx)
    table, canonical_keys = _apply_canonical_sort(table, ctx=ctx)
    schema_for_ordering = (
        metadata_spec.apply(table.schema) if metadata_spec is not None else table.schema
    )
    ordering_spec = _ordering_metadata_spec(
        spec.plan.ordering,
        schema=schema_for_ordering,
        canonical_keys=canonical_keys,
    )
    combined = merge_metadata_specs(
        metadata_spec,
        ordering_spec,
        _pipeline_breaker_spec(spec.pipeline_breakers),
    )
    return cast("TableLike", _apply_metadata_spec(table, metadata_spec=combined))


def finalize_plan_bundle(
    plans: Mapping[str, Plan],
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
    metadata_specs: Mapping[str, SchemaMetadataSpec] | None = None,
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
        outputs[name] = finalize_plan(
            plan,
            ctx=ctx,
            prefer_reader=prefer_reader,
            metadata_spec=spec,
        )
    return outputs


__all__ = [
    "align_plan",
    "align_table",
    "finalize_plan",
    "finalize_plan_bundle",
    "flatten_struct_field",
    "iter_record_batches",
    "materialize_plan",
    "plan_from_rows",
    "project_columns",
    "query_for_schema",
    "reader_from_rows",
    "rows_to_table",
    "stream_plan",
    "unify_schemas",
    "unify_tables",
]
