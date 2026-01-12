"""Shared table construction helpers for extractors."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import (
    ComputeExpression,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    pc,
)
from arrowdsl.plan.plan import Plan, PlanSpec
from arrowdsl.plan.query import QuerySpec
from arrowdsl.schema.schema import (
    SchemaEvolutionSpec,
    SchemaMetadataSpec,
    SchemaTransform,
    empty_table,
    projection_for_schema,
)


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


def append_projection(
    plan: Plan,
    *,
    base: Sequence[str],
    extras: Sequence[tuple[ComputeExpression, str]],
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Append extra expressions to a projection of base columns.

    Returns
    -------
    Plan
        Plan with appended projection expressions.
    """
    expressions: list[ComputeExpression] = [pc.field(name) for name in base]
    names = list(base)
    for expr, name in extras:
        expressions.append(expr)
        names.append(name)
    return plan.project(expressions, names, ctx=ctx)


def rename_plan_columns(
    plan: Plan,
    *,
    columns: Sequence[str],
    rename: Mapping[str, str],
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Rename columns via a plan projection.

    Returns
    -------
    Plan
        Plan with renamed columns.
    """
    names = [rename.get(name, name) for name in columns]
    expressions = [pc.field(name) for name in columns]
    return plan.project(expressions, names, ctx=ctx)


def query_for_schema(schema: SchemaLike) -> QuerySpec:
    """Return a QuerySpec projecting the schema columns.

    Returns
    -------
    QuerySpec
        QuerySpec with base columns set to the schema names.
    """
    return QuerySpec.simple(*schema.names)


def apply_query_spec(
    plan: Plan,
    *,
    spec: QuerySpec,
    ctx: ExecutionContext,
    provenance: bool = False,
) -> Plan:
    """Apply QuerySpec filters and projections to a plan.

    Returns
    -------
    Plan
        Updated plan with filters/projections applied.
    """
    predicate = spec.predicate_expression()
    if predicate is not None:
        plan = plan.filter(predicate, ctx=ctx)
    cols = spec.scan_columns(provenance=provenance)
    if isinstance(cols, Mapping):
        names = list(cols.keys())
        expressions = list(cols.values())
    else:
        names = list(cols)
        expressions = [pc.field(name) for name in cols]
    return plan.project(expressions, names, ctx=ctx)


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


def materialize_plan(plan: Plan, *, ctx: ExecutionContext) -> TableLike:
    """Materialize a plan as a table.

    Returns
    -------
    TableLike
        Materialized table.
    """
    return PlanSpec.from_plan(plan).to_table(ctx=ctx)


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
) -> TableLike | RecordBatchReaderLike:
    """Return a reader when possible, otherwise materialize the plan.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when allowed, otherwise a table.
    """
    spec = PlanSpec.from_plan(plan)
    if prefer_reader and not spec.pipeline_breakers:
        return spec.to_reader(ctx=ctx)
    return spec.to_table(ctx=ctx)


def finalize_plan_bundle(
    plans: Mapping[str, Plan],
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
) -> dict[str, TableLike | RecordBatchReaderLike]:
    """Finalize a bundle of plans into tables or readers.

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Finalized plan outputs keyed by name.
    """
    return {
        name: finalize_plan(plan, ctx=ctx, prefer_reader=prefer_reader)
        for name, plan in plans.items()
    }


def flatten_struct_field(field: pa.Field) -> list[pa.Field]:
    """Flatten a struct field into child fields with parent-name prefixes.

    Returns
    -------
    list[pyarrow.Field]
        Flattened fields with parent-name prefixes.
    """
    return list(field.flatten())


__all__ = [
    "align_plan",
    "align_table",
    "append_projection",
    "apply_query_spec",
    "finalize_plan",
    "finalize_plan_bundle",
    "flatten_struct_field",
    "iter_record_batches",
    "materialize_plan",
    "plan_from_rows",
    "query_for_schema",
    "reader_from_rows",
    "rename_plan_columns",
    "rows_to_table",
    "stream_plan",
    "unify_schemas",
    "unify_tables",
]
