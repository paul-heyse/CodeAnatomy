"""Plan helpers for CPG plan-lane builders."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa
import pyarrow.dataset as ds

from arrowdsl.core.context import DeterminismTier, ExecutionContext, Ordering, OrderingLevel
from arrowdsl.core.interop import (
    ComputeExpression,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    pc,
)
from arrowdsl.finalize.finalize import Contract
from arrowdsl.plan.plan import Plan, PlanSpec
from arrowdsl.plan_helpers import encoding_columns_from_metadata
from arrowdsl.schema.schema import (
    EncodingSpec,
    empty_table,
    encode_expression,
    projection_for_schema,
)
from cpg.sources import DatasetSource
from schema_spec.system import DatasetSpec


def ensure_plan(
    source: Plan | TableLike | DatasetSource,
    *,
    label: str = "",
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Return a plan backed by an Acero table source.

    Returns
    -------
    Plan
        Plan for the source value.

    Raises
    ------
    ValueError
        Raised when a DatasetSource is provided without a context.
    """
    if isinstance(source, Plan):
        return source
    if isinstance(source, DatasetSource):
        if ctx is None:
            msg = "ensure_plan requires ctx when source is DatasetSource."
            raise ValueError(msg)
        return plan_from_dataset(source.dataset, spec=source.spec, ctx=ctx)
    return Plan.table_source(source, label=label)


def empty_plan(schema: SchemaLike, *, label: str = "") -> Plan:
    """Return a plan backed by an empty table with the provided schema.

    Returns
    -------
    Plan
        Plan with an empty table source.
    """
    return Plan.table_source(empty_table(schema), label=label)


def align_plan(plan: Plan, *, schema: SchemaLike, ctx: ExecutionContext) -> Plan:
    """Align a plan to a target schema via projection.

    Returns
    -------
    Plan
        Plan projecting/casting to the schema.
    """
    available = plan.schema(ctx=ctx).names
    exprs, names = projection_for_schema(schema, available=available, safe_cast=ctx.safe_cast)
    return plan.project(exprs, names, ctx=ctx)


def encode_plan(
    plan: Plan,
    *,
    specs: Sequence[EncodingSpec],
    ctx: ExecutionContext,
) -> Plan:
    """Return a plan with dictionary encoding applied.

    Returns
    -------
    Plan
        Plan with dictionary-encoded columns.
    """
    encode_cols = {spec.column for spec in specs}
    names = plan.schema(ctx=ctx).names
    exprs: list[ComputeExpression] = []
    for name in names:
        if name in encode_cols:
            exprs.append(encode_expression(name))
        else:
            exprs.append(pc.field(name))
    return plan.project(exprs, names, ctx=ctx)


def set_or_append_column(
    plan: Plan,
    *,
    name: str,
    expr: ComputeExpression,
    ctx: ExecutionContext,
) -> Plan:
    """Replace or append a column with the provided expression.

    Returns
    -------
    Plan
        Updated plan with the column set.
    """
    names = list(plan.schema(ctx=ctx).names)
    exprs = [pc.field(col) for col in names]
    if name in names:
        idx = names.index(name)
        exprs[idx] = expr
    else:
        names.append(name)
        exprs.append(expr)
    return plan.project(exprs, names, ctx=ctx)


def finalize_plan(plan: Plan, *, ctx: ExecutionContext) -> TableLike:
    """Materialize a plan as a table.

    Returns
    -------
    TableLike
        Materialized plan output.
    """
    table = PlanSpec.from_plan(plan).to_table(ctx=ctx)
    return finalize_table(table)


def plan_reader(plan: Plan, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
    """Return a reader for streaming plans without pipeline breakers.

    Returns
    -------
    RecordBatchReaderLike
        Streaming reader for the plan.
    """
    return PlanSpec.from_plan(plan).to_reader(ctx=ctx)


def plan_from_dataset(
    dataset: ds.Dataset,
    *,
    spec: DatasetSpec,
    ctx: ExecutionContext,
) -> Plan:
    """Compile a dataset-backed scan plan with ordering metadata.

    Returns
    -------
    Plan
        Plan representing the dataset scan and projection.
    """
    scan_ctx = spec.scan_context(dataset, ctx)
    ordering = (
        Ordering.implicit()
        if ctx.runtime.scan.implicit_ordering or ctx.runtime.scan.require_sequenced_output
        else Ordering.unordered()
    )
    return Plan(
        decl=scan_ctx.acero_decl(),
        label=spec.name,
        ordering=ordering,
        pipeline_breakers=(),
    )


def finalize_table(table: TableLike, *, unify_dicts: bool = True) -> TableLike:
    """Finalize a materialized table for downstream contracts.

    Returns
    -------
    TableLike
        Table with dictionary pools unified when requested.
    """
    if unify_dicts:
        return table.unify_dictionaries()
    return table


def unify_schema_with_metadata(
    schemas: Sequence[SchemaLike],
    *,
    promote_options: str = "permissive",
) -> SchemaLike:
    """Return a unified schema, preserving metadata from the first schema.

    Returns
    -------
    SchemaLike
        Unified schema with preserved metadata.
    """
    if not schemas:
        return pa.schema([])
    try:
        unified = pa.unify_schemas(list(schemas), promote_options=promote_options)
    except TypeError:
        unified = pa.unify_schemas(list(schemas))

    base = schemas[0]
    field_meta = {field.name: field.metadata for field in base if field.metadata}
    fields = []
    for field in unified:
        meta = field_meta.get(field.name)
        if meta:
            fields.append(field.with_metadata(meta))
        else:
            fields.append(field)
    unified = pa.schema(fields)
    if base.metadata:
        return unified.with_metadata(dict(base.metadata))
    return unified


def align_table_to_schema(table: TableLike, *, schema: SchemaLike) -> TableLike:
    """Cast a table to a target schema, preserving schema metadata.

    Returns
    -------
    TableLike
        Table cast to the provided schema.
    """
    return table.cast(schema)


def finalize_context_for_plan(
    plan: Plan,
    *,
    contract: Contract,
    ctx: ExecutionContext,
) -> ExecutionContext:
    """Return the finalize context with canonical ordering disabled when redundant.

    Returns
    -------
    ExecutionContext
        Execution context configured for finalize ordering.
    """
    if ctx.determinism != DeterminismTier.CANONICAL:
        return ctx
    explicit_keys = tuple((sk.column, sk.order) for sk in contract.canonical_sort)
    if plan.ordering.level == OrderingLevel.EXPLICIT and plan.ordering.keys == explicit_keys:
        return ctx.with_determinism(DeterminismTier.STABLE_SET)
    return ctx


def assert_schema_metadata(table: TableLike, *, schema: SchemaLike) -> None:
    """Raise when schema metadata does not match the target schema.

    Raises
    ------
    ValueError
        Raised when schema metadata does not match.
    """
    table_schema = pa.schema(table.schema)
    expected_schema = pa.schema(schema)
    if not table_schema.equals(expected_schema, check_metadata=True):
        msg = "Schema metadata mismatch after finalize."
        raise ValueError(msg)


__all__ = [
    "align_plan",
    "align_table_to_schema",
    "assert_schema_metadata",
    "empty_plan",
    "encode_plan",
    "encoding_columns_from_metadata",
    "ensure_plan",
    "finalize_context_for_plan",
    "finalize_plan",
    "finalize_table",
    "plan_from_dataset",
    "plan_reader",
    "set_or_append_column",
    "unify_schema_with_metadata",
]
