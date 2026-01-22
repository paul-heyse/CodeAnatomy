"""Ibis-based property emission helpers for CPG builders."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

import ibis
import pyarrow as pa
from ibis.expr.types import StringValue, Table, Value

from arrowdsl.core.ordering import Ordering
from cpg.prop_transforms import expr_context_expr, flag_to_bool_expr
from cpg.schemas import CPG_PROPS_SCHEMA
from cpg.specs import (
    TRANSFORM_EXPR_CONTEXT,
    TRANSFORM_FLAG_TO_BOOL,
    PropFieldSpec,
    PropTableSpec,
    PropValueType,
)
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import ibis_null_literal, validate_expr_schema


def emit_props_fast(
    rel: IbisPlan | Table,
    *,
    spec: PropTableSpec,
    fields: Sequence[PropFieldSpec],
    schema_version: int | None,
) -> list[IbisPlan]:
    """Emit property rows for fast-lane (non-JSON) fields.

    Returns
    -------
    list[IbisPlan]
        Ibis plans emitting property rows.
    """
    context = PropEmitContext(
        spec=spec,
        schema_version=schema_version,
        include_node_kind=True,
        json_mode=False,
        schema=CPG_PROPS_SCHEMA,
    )
    return _emit_props_ibis(rel, fields=fields, context=context)


def emit_props_json(
    rel: IbisPlan | Table,
    *,
    spec: PropTableSpec,
    fields: Sequence[PropFieldSpec],
    schema_version: int | None,
    schema: pa.Schema,
) -> list[IbisPlan]:
    """Emit property rows for JSON-heavy fields.

    Returns
    -------
    list[IbisPlan]
        Ibis plans emitting JSON property rows.
    """
    context = PropEmitContext(
        spec=spec,
        schema_version=schema_version,
        include_node_kind=False,
        json_mode=True,
        schema=schema,
    )
    return _emit_props_ibis(rel, fields=fields, context=context)


@dataclass(frozen=True)
class PropEmitContext:
    """Shared context for property emission."""

    spec: PropTableSpec
    schema_version: int | None
    include_node_kind: bool
    json_mode: bool
    schema: pa.Schema


@dataclass(frozen=True)
class PropFieldPlanContext:
    """Shared context for per-field plan emission."""

    spec: PropTableSpec
    entity_id: Value
    schema_version: int | None
    json_mode: bool
    schema: pa.Schema


def _emit_props_ibis(
    rel: IbisPlan | Table,
    *,
    fields: Sequence[PropFieldSpec],
    context: PropEmitContext,
) -> list[IbisPlan]:
    expr = rel.expr if isinstance(rel, IbisPlan) else rel
    entity_id = _entity_id_expr(expr, context.spec.id_cols)
    field_context = PropFieldPlanContext(
        spec=context.spec,
        entity_id=entity_id,
        schema_version=context.schema_version,
        json_mode=context.json_mode,
        schema=context.schema,
    )
    plans: list[IbisPlan] = []
    if context.include_node_kind and context.spec.node_kind is not None:
        node_field = PropFieldSpec(
            prop_key="node_kind",
            literal=str(context.spec.node_kind),
            value_type="string",
        )
        plans.append(
            _prop_field_plan(
                expr,
                context=field_context,
                field=node_field,
                json_mode=False,
            )
        )
    plans.extend(
        _prop_field_plan(
            expr,
            context=field_context,
            field=field,
            json_mode=context.json_mode,
        )
        for field in fields
    )
    return plans


def _prop_field_plan(
    expr: Table,
    *,
    context: PropFieldPlanContext,
    field: PropFieldSpec,
    json_mode: bool,
) -> IbisPlan:
    value_expr, raw_expr, value_type = _value_expr(expr, field=field, json_mode=json_mode)
    output = expr
    if field.skip_if_none:
        output = output.filter(raw_expr.notnull())
    values = _null_value_exprs()
    target_col = _value_column(value_type, json_mode=json_mode)
    values[target_col] = value_expr
    output = output.select(
        entity_kind=ibis.literal(context.spec.entity_kind.value),
        entity_id=context.entity_id,
        prop_key=ibis.literal(field.prop_key),
        **values,
    )
    if context.schema_version is not None:
        output = output.mutate(schema_version=ibis.literal(context.schema_version))
    validate_expr_schema(output, expected=context.schema)
    return IbisPlan(expr=output, ordering=Ordering.unordered())


def _entity_id_expr(table: Table, cols: Sequence[str]) -> Value:
    values: list[Value] = []
    for col in cols:
        if col not in table.columns:
            continue
        values.append(_null_if_empty_or_zero(table[col].cast("string")))
    if not values:
        return ibis_null_literal(pa.string())
    if len(values) == 1:
        return values[0]
    return ibis.coalesce(*values)


def _null_if_empty_or_zero(value: Value) -> Value:
    text: StringValue = value.cast("string").strip()
    empty = text.length() == 0
    is_zero = text == ibis.literal("0")
    return ibis.ifelse(
        empty,
        ibis_null_literal(pa.string()),
        ibis.ifelse(is_zero, ibis_null_literal(pa.string()), text),
    )


def _value_expr(
    table: Table,
    *,
    field: PropFieldSpec,
    json_mode: bool,
) -> tuple[Value, Value, PropValueType]:
    value_type = field.value_type
    if value_type is None:
        msg = f"Missing value_type for prop field {field.prop_key!r}."
        raise ValueError(msg)
    raw = _source_expr(table, field=field, value_type=value_type)
    raw = _apply_transform(raw, transform_id=field.transform_id)
    if json_mode:
        return _json_value_expr(raw), raw, value_type
    if value_type == "json":
        return ibis_null_literal(pa.string()), raw, value_type
    return _cast_value(raw, value_type=value_type), raw, value_type


def _source_expr(table: Table, *, field: PropFieldSpec, value_type: PropValueType) -> Value:
    if field.literal is not None:
        return ibis.literal(field.literal)
    if field.source_col is not None and field.source_col in table.columns:
        return table[field.source_col]
    return ibis_null_literal(_value_dtype(value_type))


def _apply_transform(expr: Value, *, transform_id: str | None) -> Value:
    if transform_id is None:
        return expr
    if transform_id == TRANSFORM_EXPR_CONTEXT:
        return expr_context_expr(expr)
    if transform_id == TRANSFORM_FLAG_TO_BOOL:
        return flag_to_bool_expr(expr)
    msg = f"Unknown prop transform id: {transform_id!r}"
    raise ValueError(msg)


def _json_value_expr(expr: Value) -> Value:
    return expr.cast("string")


def _cast_value(expr: Value, *, value_type: PropValueType) -> Value:
    return expr.cast(_value_cast_type(value_type))


def _value_cast_type(value_type: PropValueType) -> Literal["string", "int64", "float64", "bool"]:
    if value_type == "string":
        return "string"
    if value_type == "int":
        return "int64"
    if value_type == "float":
        return "float64"
    if value_type == "bool":
        return "bool"
    return "string"


def _value_column(value_type: PropValueType, *, json_mode: bool) -> str:
    if json_mode:
        return "value_json"
    return {
        "string": "value_str",
        "int": "value_int",
        "float": "value_float",
        "bool": "value_bool",
        "json": "value_json",
    }[value_type]


def _null_value_exprs() -> dict[str, Value]:
    return {
        "value_str": ibis_null_literal(pa.string()),
        "value_int": ibis_null_literal(pa.int64()),
        "value_float": ibis_null_literal(pa.float64()),
        "value_bool": ibis_null_literal(pa.bool_()),
        "value_json": ibis_null_literal(pa.string()),
    }


def _value_dtype(value_type: PropValueType) -> pa.DataType:
    if value_type == "string":
        return pa.string()
    if value_type == "int":
        return pa.int64()
    if value_type == "float":
        return pa.float64()
    if value_type == "bool":
        return pa.bool_()
    return pa.string()


def filter_prop_fields(
    fields: Iterable[PropFieldSpec],
    *,
    json_mode: bool,
) -> list[PropFieldSpec]:
    """Return fields filtered to fast or JSON lanes.

    Returns
    -------
    list[PropFieldSpec]
        Fields that match the requested lane.
    """
    selected: list[PropFieldSpec] = []
    for field in fields:
        if json_mode and field.value_type == "json":
            selected.append(field)
        if not json_mode and field.value_type != "json":
            selected.append(field)
    return selected


__all__ = [
    "emit_props_fast",
    "emit_props_json",
    "filter_prop_fields",
]
