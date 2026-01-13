"""Schema alignment, inference, and constraint helpers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import pyarrow.types as patypes

import arrowdsl.core.interop as pa
from arrowdsl.compute.kernels import ChunkPolicy
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import (
    ArrayLike,
    ComputeExpression,
    DataTypeLike,
    FieldLike,
    ScalarLike,
    SchemaLike,
    TableLike,
    ensure_expression,
    pc,
)
from arrowdsl.plan.plan import Plan
from arrowdsl.schema.metadata import metadata_spec_from_schema
from arrowdsl.schema.schema import (
    EncodingPolicy,
    EncodingSpec,
    SchemaEvolutionSpec,
    align_to_schema,
    encode_expression,
    projection_for_schema,
)
from schema_spec.specs import TableSchemaSpec


def encoding_projection(
    columns: Sequence[str],
    *,
    available: Sequence[str],
) -> tuple[list[ComputeExpression], list[str]]:
    """Return projection expressions to apply dictionary encoding.

    Returns
    -------
    tuple[list[ComputeExpression], list[str]]
        Expressions and column names for encoding projection.
    """
    encode_set = set(columns)
    expressions: list[ComputeExpression] = []
    names: list[str] = []
    for name in available:
        expr = encode_expression(name) if name in encode_set else pc.field(name)
        expressions.append(expr)
        names.append(name)
    return expressions, names


def encoding_columns_from_metadata(schema: SchemaLike) -> list[str]:
    """Return columns marked for dictionary encoding via field metadata.

    Returns
    -------
    list[str]
        Column names marked for dictionary encoding.
    """
    encoding_columns: list[str] = []
    for field in schema:
        meta = field.metadata or {}
        if meta.get(b"encoding") == b"dictionary":
            encoding_columns.append(field.name)
    return encoding_columns


def align_table(
    table: TableLike,
    *,
    schema: SchemaLike,
    ctx: ExecutionContext,
    keep_extra_columns: bool = False,
) -> TableLike:
    """Align a table to a schema using context casting policy.

    Returns
    -------
    TableLike
        Aligned table.
    """
    aligned, _ = align_to_schema(
        table,
        schema=schema,
        safe_cast=ctx.safe_cast,
        keep_extra_columns=keep_extra_columns,
    )
    return aligned


def align_plan(
    plan: Plan,
    *,
    schema: SchemaLike,
    ctx: ExecutionContext,
    keep_extra_columns: bool = False,
    available: Sequence[str] | None = None,
) -> Plan:
    """Align a plan to a target schema via projection.

    Returns
    -------
    Plan
        Plan projecting/casting to the schema.
    """
    available = plan.schema(ctx=ctx).names if available is None else available
    exprs, names = projection_for_schema(schema, available=available, safe_cast=ctx.safe_cast)
    if keep_extra_columns:
        extras = [name for name in available if name not in names]
        for name in extras:
            names.append(name)
            exprs.append(pc.field(name))
    return plan.project(exprs, names, ctx=ctx)


def encode_plan(plan: Plan, *, columns: Sequence[str], ctx: ExecutionContext) -> Plan:
    """Return a plan with dictionary encoding applied.

    Returns
    -------
    Plan
        Plan with dictionary-encoded columns.
    """
    exprs, names = encoding_projection(columns, available=plan.schema(ctx=ctx).names)
    return plan.project(exprs, names, ctx=ctx)


def encode_table(table: TableLike, *, columns: Sequence[str]) -> TableLike:
    """Dictionary-encode specified columns on a table.

    Returns
    -------
    TableLike
        Table with encoded columns.
    """
    if not columns:
        return table
    specs = tuple(EncodingSpec(column=col) for col in columns)
    return EncodingPolicy(specs=specs).apply(table)


def encode_any(
    value: TableLike | Plan,
    *,
    policy: EncodingPolicy,
    ctx: ExecutionContext,
) -> TableLike | Plan:
    """Apply encoding policy to a plan or table.

    Returns
    -------
    TableLike | Plan
        Encoded plan or table.
    """
    if isinstance(value, Plan):
        columns = tuple(spec.column for spec in policy.specs)
        return encode_plan(value, columns=columns, ctx=ctx)
    return policy.apply(value)


def _is_advanced_type(dtype: object) -> bool:
    return any(
        (
            patypes.is_struct(dtype),
            patypes.is_list(dtype),
            patypes.is_large_list(dtype),
            patypes.is_list_view(dtype),
            patypes.is_large_list_view(dtype),
            patypes.is_map(dtype),
            patypes.is_dictionary(dtype),
        )
    )


def _prefer_base_nested(base: SchemaLike, merged: SchemaLike) -> SchemaLike:
    fields: list[FieldLike] = []
    base_map = {field.name: field for field in base}
    for field in merged:
        base_field = base_map.get(field.name)
        if base_field is not None and _is_advanced_type(base_field.type):
            fields.append(base_field)
        else:
            fields.append(field)
    return pa.schema(fields)


def unify_schemas(
    schemas: Sequence[SchemaLike],
    *,
    promote_options: str = "permissive",
    prefer_nested: bool = True,
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
    if prefer_nested:
        unified = _prefer_base_nested(schemas[0], unified)
    return metadata_spec_from_schema(schemas[0]).apply(unified)


def unify_schema_with_metadata(
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
    return unify_schemas(schemas, promote_options=promote_options)


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
        return pa.Table.from_arrays([], names=[])
    schema = unify_schemas([table.schema for table in tables], promote_options=promote_options)
    aligned: list[TableLike] = []
    for table in tables:
        aligned_table, _ = align_to_schema(table, schema=schema, safe_cast=True)
        aligned.append(aligned_table)
    combined = pa.concat_tables(aligned, promote=True)
    return ChunkPolicy().apply(combined)


def best_fit_type(array: ArrayLike, candidates: Sequence[DataTypeLike]) -> DataTypeLike:
    """Return the most specific candidate type that preserves validity.

    Returns
    -------
    pyarrow.DataType
        Best-fit type for the array.
    """
    total_rows = len(array)
    for dtype in candidates:
        casted = pc.cast(array, dtype, safe=False)
        valid = pc.is_valid(casted)
        total = pc.call_function("sum", [pc.cast(valid, pa.int64())])
        value = cast("int | float | bool | None", cast("ScalarLike", total).as_py())
        if value is None:
            continue
        count = int(value)
        if count == total_rows:
            return dtype
    return array.type


def infer_schema_from_tables(
    tables: Sequence[TableLike],
    *,
    promote_options: str = "permissive",
    prefer_nested: bool = True,
) -> SchemaLike:
    """Infer a unified schema from tables using Arrow evolution rules.

    Returns
    -------
    SchemaLike
        Unified schema for the input tables.
    """
    schemas = [table.schema for table in tables if table is not None]
    return unify_schemas(
        schemas,
        promote_options=promote_options,
        prefer_nested=prefer_nested,
    )


def required_field_names(spec: TableSchemaSpec) -> tuple[str, ...]:
    """Return required field names (explicit or non-nullable).

    Returns
    -------
    tuple[str, ...]
        Required field names.
    """
    required = set(spec.required_non_null)
    return tuple(
        field.name for field in spec.fields if field.name in required or not field.nullable
    )


def required_non_null_mask(
    required: Sequence[str],
    *,
    available: set[str],
) -> ComputeExpression:
    """Return a plan-lane mask for required non-null violations.

    Returns
    -------
    ComputeExpression
        Boolean expression for invalid rows.
    """
    exprs = [
        ensure_expression(pc.invert(pc.is_valid(pc.field(name))))
        for name in required
        if name in available
    ]
    if not exprs:
        return ensure_expression(pc.scalar(pa.scalar(value=False)))
    return ensure_expression(pc.or_(*exprs))


def missing_key_fields(keys: Sequence[str], *, missing_cols: Sequence[str]) -> tuple[str, ...]:
    """Return key fields missing from the available columns.

    Returns
    -------
    tuple[str, ...]
        Missing key field names.
    """
    missing = set(missing_cols)
    return tuple(key for key in keys if key in missing)


__all__ = [
    "align_plan",
    "align_table",
    "best_fit_type",
    "encode_any",
    "encode_plan",
    "encode_table",
    "encoding_columns_from_metadata",
    "encoding_projection",
    "infer_schema_from_tables",
    "missing_key_fields",
    "required_field_names",
    "required_non_null_mask",
    "unify_schema_with_metadata",
    "unify_schemas",
    "unify_tables",
]
