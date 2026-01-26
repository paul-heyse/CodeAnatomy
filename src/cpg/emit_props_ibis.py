"""Ibis-based property emission helpers."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import ibis
import pyarrow as pa
from ibis.expr.types import Table, Value

from arrowdsl.core.ordering import Ordering
from arrowdsl.schema.build import empty_table
from cpg.kind_catalog import EntityKind
from cpg.schemas import CPG_PROPS_SCHEMA
from cpg.specs import (
    PropFieldSpec,
    PropOptions,
    PropTableSpec,
    TaskIdentity,
    filter_fields,
    resolve_prop_transform,
)
from ibis_engine.hash_exprs import HashExprSpec, masked_stable_id_expr_from_spec
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import (
    bind_expr_schema,
    ensure_columns,
    ibis_dtype_from_arrow,
    ibis_null_literal,
    ibis_schema_from_arrow,
    validate_expr_schema,
)

SQLGLOT_UNION_THRESHOLD = 96


@dataclass(frozen=True)
class CpgPropOptions:
    """Default property include options for CPG builders."""

    include_heavy_json_props: bool = False


def emit_props_ibis(
    rel: IbisPlan | Table,
    *,
    spec: PropTableSpec,
    options: PropOptions | None = None,
    task_identity: TaskIdentity | None = None,
    union_builder: Callable[[Sequence[Table]], Table] | None = None,
) -> IbisPlan:
    """Emit CPG properties from a relation table using Ibis expressions.

    Returns
    -------
    IbisPlan
        Ibis plan emitting property rows.
    """
    expr = rel.expr if isinstance(rel, IbisPlan) else rel
    resolved_options = options or CpgPropOptions()
    fields = filter_fields(spec.fields, options=resolved_options)
    if not fields:
        return _empty_props_plan(expr)
    task_name = task_identity.name if task_identity is not None else None
    task_priority = task_identity.priority if task_identity is not None else None
    rows: list[Table] = []
    for field in fields:
        row_expr = _prop_row_expr(
            expr,
            spec=spec,
            field=field,
            task_name=task_name,
            task_priority=task_priority,
        )
        if row_expr is None:
            continue
        rows.append(row_expr)
    if not rows:
        return _empty_props_plan(expr)
    if union_builder is not None and len(rows) >= SQLGLOT_UNION_THRESHOLD:
        combined = union_builder(rows)
    else:
        combined = _union_rows(rows)
    combined = ensure_columns(combined, schema=CPG_PROPS_SCHEMA, only_missing=True)
    combined = combined.select(*CPG_PROPS_SCHEMA.names)
    validate_expr_schema(combined, expected=CPG_PROPS_SCHEMA, allow_extra_columns=False)
    combined = bind_expr_schema(
        combined,
        schema=CPG_PROPS_SCHEMA,
        allow_extra_columns=False,
    )
    return IbisPlan(expr=combined, ordering=Ordering.unordered())


def _prop_row_expr(
    expr: Table,
    *,
    spec: PropTableSpec,
    field: PropFieldSpec,
    task_name: str | None,
    task_priority: int | None,
) -> Table | None:
    value_expr = _field_value_expr(expr, field)
    if value_expr is None:
        return None
    if field.skip_if_none:
        expr = expr.filter(value_expr.notnull())
    schema = CPG_PROPS_SCHEMA
    schema_names = set(schema.names)
    expr, entity_id = _entity_id_expr(expr, spec)
    columns: dict[str, Value] = {}
    if "entity_kind" in schema_names:
        columns["entity_kind"] = ibis.literal(spec.entity_kind.value)
    if "entity_id" in schema_names:
        columns["entity_id"] = entity_id
    if "node_kind" in schema_names:
        node_kind = str(spec.node_kind) if spec.node_kind is not None else None
        columns["node_kind"] = _literal_or_null(node_kind, pa.string())
    if "prop_key" in schema_names:
        columns["prop_key"] = ibis.literal(field.prop_key)
    value_type = field.value_type or "string"
    if "value_type" in schema_names:
        columns["value_type"] = ibis.literal(value_type)
    if "task_name" in schema_names:
        columns["task_name"] = _literal_or_null(task_name, pa.string())
    if "task_priority" in schema_names:
        columns["task_priority"] = _literal_or_null(task_priority, pa.int32())
    columns.update(_value_columns(schema, value_expr, value_type=value_type))
    row = expr.select(**columns)
    row = ensure_columns(row, schema=CPG_PROPS_SCHEMA, only_missing=True)
    return row.select(*CPG_PROPS_SCHEMA.names)


def _field_value_expr(expr: Table, field: PropFieldSpec) -> Value | None:
    if field.literal is not None:
        value = ibis.literal(field.literal)
    elif field.source_col is not None:
        value = (
            expr[field.source_col]
            if field.source_col in expr.columns
            else ibis_null_literal(pa.string())
        )
    else:
        return None
    transform = resolve_prop_transform(field.transform_id)
    if transform is None:
        return value
    return transform.expr_fn(value)


def _entity_id_expr(expr: Table, spec: PropTableSpec) -> tuple[Table, Value]:
    id_cols = spec.id_cols
    if spec.entity_kind == EntityKind.EDGE and len(id_cols) == 1 and id_cols[0] in expr.columns:
        return expr, expr[id_cols[0]]
    expr, id_cols, required = _prepare_id_columns(expr, id_cols)
    prefix = "node" if spec.entity_kind == EntityKind.NODE else "edge"
    if spec.entity_kind == EntityKind.NODE and spec.node_kind is not None:
        extra_literals = (str(spec.node_kind),)
    else:
        extra_literals = ()
    entity_id = masked_stable_id_expr_from_spec(
        expr,
        spec=HashExprSpec(
            prefix=prefix,
            cols=id_cols,
            extra_literals=extra_literals,
            null_sentinel="None",
        ),
        required=required,
    )
    return expr, entity_id


def _prepare_id_columns(
    expr: Table,
    columns: Sequence[str],
) -> tuple[Table, tuple[str, ...], tuple[str, ...]]:
    required = tuple(column for column in columns if column in expr.columns)
    missing = [column for column in columns if column not in expr.columns]
    updates: dict[str, Value] = {}
    for column in missing:
        updates[column] = ibis_null_literal(pa.string())
    if not columns:
        updates["__id_null"] = ibis_null_literal(pa.string())
        return expr.mutate(**updates), ("__id_null",), ()
    if updates:
        expr = expr.mutate(**updates)
    return expr, tuple(columns), required


def _value_columns(schema: pa.Schema, value_expr: Value, *, value_type: str) -> dict[str, Value]:
    names = set(schema.names)
    value_column = _resolve_value_column(names, value_type)
    columns: dict[str, Value] = {}
    if value_column is None:
        return columns
    value_field = schema.field(value_column)
    columns[value_column] = ibis.cast(value_expr, ibis_dtype_from_arrow(value_field.type))
    for field in schema:
        if field.name == value_column:
            continue
        if field.name.startswith("value_") and field.name in names:
            columns[field.name] = ibis_null_literal(field.type)
    return columns


def _resolve_value_column(names: set[str], value_type: str) -> str | None:
    candidates: dict[str, tuple[str, ...]] = {
        "string": ("value_string", "value_str", "value"),
        "int": ("value_int", "value_i64", "value"),
        "float": ("value_float", "value_f64", "value"),
        "bool": ("value_bool", "value"),
        "json": ("value_json", "value"),
    }
    for candidate in candidates.get(value_type, ("value",)):
        if candidate in names:
            return candidate
    if "prop_value" in names:
        return "prop_value"
    return None


def _literal_or_null(value: object | None, dtype: pa.DataType) -> Value:
    if value is None:
        return ibis_null_literal(dtype)
    return ibis.literal(value)


def _union_rows(rows: Sequence[Table], *, batch_size: int = 32) -> Table:
    if len(rows) <= batch_size:
        return _union_exprs(rows)
    batches = [
        _union_exprs(rows[index : index + batch_size]) for index in range(0, len(rows), batch_size)
    ]
    return _union_exprs(batches)


def _union_exprs(exprs: Sequence[Table]) -> Table:
    iterator = iter(exprs)
    combined = next(iterator)
    for expr in iterator:
        combined = combined.union(expr)
    return combined


def _empty_props_plan(expr: Table) -> IbisPlan:
    backend = getattr(expr, "_find_backend", lambda: None)()
    if backend is None:
        backend = getattr(expr, "backend", None)
    if backend is None:
        msg = "CPG property emission requires an Ibis backend."
        raise ValueError(msg)
    empty = empty_table(CPG_PROPS_SCHEMA)
    table_name = f"__cpg_props_empty_{uuid.uuid4().hex}"
    backend.create_table(
        table_name,
        obj=empty,
        schema=ibis_schema_from_arrow(empty.schema),
        temp=True,
        overwrite=True,
    )
    return IbisPlan(expr=backend.table(table_name), ordering=Ordering.unordered())


__all__ = ["CpgPropOptions", "emit_props_ibis"]
