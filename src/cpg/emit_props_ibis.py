"""Ibis-based property emission helpers."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

import ibis
import pyarrow as pa
from ibis.expr.types import Table, Value

from arrowdsl.core.ordering import Ordering
from cpg.kind_catalog import EntityKind
from cpg.specs import (
    PropFieldSpec,
    PropOptions,
    PropTableSpec,
    TaskIdentity,
    filter_fields,
    resolve_prop_transform,
)
from ibis_engine.expr_compiler import expr_ir_to_ibis
from ibis_engine.hashing import HashExprSpec, masked_stable_id_expr_ir
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import ibis_dtype_from_arrow, ibis_null_literal
from sqlglot_tools.expr_spec import SqlExprSpec


def _expr_from_spec(table: Table, spec: SqlExprSpec) -> Value:
    expr_ir = spec.expr_ir
    if expr_ir is None:
        msg = "SqlExprSpec missing expr_ir; ExprIR-backed specs are required."
        raise ValueError(msg)
    return expr_ir_to_ibis(expr_ir, table)


def masked_stable_id_expr_from_spec(
    table: Table,
    *,
    spec: HashExprSpec,
    required: Sequence[str],
    use_128: bool | None = None,
) -> Value:
    return _expr_from_spec(
        table,
        masked_stable_id_expr_ir(spec=spec, required=tuple(required), use_128=use_128),
    )

SQLGLOT_UNION_THRESHOLD = 96
_PROP_OUTPUT_COLUMNS: tuple[str, ...] = (
    "entity_kind",
    "entity_id",
    "node_kind",
    "prop_key",
    "value_type",
    "value_string",
    "value_int",
    "value_float",
    "value_bool",
    "value_json",
    "task_name",
    "task_priority",
)


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
    combined = combined.select(*_PROP_OUTPUT_COLUMNS)
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
    expr, entity_id = _entity_id_expr(expr, spec)
    columns: dict[str, Value] = {}
    columns["entity_kind"] = ibis.literal(spec.entity_kind.value)
    columns["entity_id"] = entity_id
    node_kind = str(spec.node_kind) if spec.node_kind is not None else None
    columns["node_kind"] = _literal_or_null(node_kind, pa.string())
    columns["prop_key"] = ibis.literal(field.prop_key)
    value_type = field.value_type or "string"
    columns["value_type"] = ibis.literal(value_type)
    columns["task_name"] = _literal_or_null(task_name, pa.string())
    columns["task_priority"] = _literal_or_null(task_priority, pa.int32())
    columns.update(_value_columns(value_expr, value_type=value_type))
    row = expr.select(**columns)
    return row.select(*_PROP_OUTPUT_COLUMNS)


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


def _value_columns(value_expr: Value, *, value_type: str) -> dict[str, Value]:
    columns: dict[str, Value] = {
        "value_string": ibis_null_literal(pa.string()),
        "value_int": ibis_null_literal(pa.int64()),
        "value_float": ibis_null_literal(pa.float64()),
        "value_bool": ibis_null_literal(pa.bool_()),
        "value_json": ibis_null_literal(pa.string()),
    }
    dtype_map: dict[str, pa.DataType] = {
        "string": pa.string(),
        "int": pa.int64(),
        "float": pa.float64(),
        "bool": pa.bool_(),
        "json": pa.string(),
    }
    target = {
        "string": "value_string",
        "int": "value_int",
        "float": "value_float",
        "bool": "value_bool",
        "json": "value_json",
    }.get(value_type, "value_string")
    target_dtype = dtype_map.get(value_type, pa.string())
    columns[target] = ibis.cast(value_expr, ibis_dtype_from_arrow(target_dtype))
    return columns


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
    row = expr.select(
        entity_kind=ibis_null_literal(pa.string()),
        entity_id=ibis_null_literal(pa.string()),
        node_kind=ibis_null_literal(pa.string()),
        prop_key=ibis_null_literal(pa.string()),
        value_type=ibis_null_literal(pa.string()),
        value_string=ibis_null_literal(pa.string()),
        value_int=ibis_null_literal(pa.int64()),
        value_float=ibis_null_literal(pa.float64()),
        value_bool=ibis_null_literal(pa.bool_()),
        value_json=ibis_null_literal(pa.string()),
        task_name=ibis_null_literal(pa.string()),
        task_priority=ibis_null_literal(pa.int32()),
    )
    empty = row.filter(ibis.literal(value=False) == ibis.literal(value=True))
    return IbisPlan(expr=empty, ordering=Ordering.unordered())


__all__ = ["CpgPropOptions", "emit_props_ibis"]
