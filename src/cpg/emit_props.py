"""Plan-lane property emission helpers."""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.compute.udfs import ensure_json_udf
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, DataTypeLike, ensure_expression, pc
from arrowdsl.plan.plan import Plan
from cpg.plan_exprs import null_if_empty_or_zero
from cpg.prop_transforms import (
    expr_context_expr,
    expr_context_value,
    flag_to_bool,
    flag_to_bool_expr,
)
from cpg.specs import PropFieldSpec, PropOptions, PropTableSpec, PropValueType


@dataclass(frozen=True)
class PropFieldPlanContext:
    """Context for emitting a property field plan."""

    entity_kind: str
    entity_id: ComputeExpression
    schema_version: int | None
    ctx: ExecutionContext


@dataclass(frozen=True)
class PropValueExpr:
    """Resolved value expression metadata for property emission."""

    expr: ComputeExpression
    value_type: PropValueType
    defer_json: bool = False
    json_dtype: DataTypeLike | None = None


def _entity_id_expr(
    cols: Sequence[str],
    *,
    available: set[str],
) -> ComputeExpression:
    exprs: list[ComputeExpression] = []
    for col in cols:
        if col not in available:
            continue
        expr = ensure_expression(pc.cast(pc.field(col), pa.string(), safe=False))
        expr = null_if_empty_or_zero(expr)
        exprs.append(expr)
    if not exprs:
        return ensure_expression(pc.cast(pc.scalar(None), pa.string(), safe=False))
    if len(exprs) == 1:
        return exprs[0]
    return ensure_expression(pc.coalesce(*exprs))


def _value_dtype(value_type: PropValueType) -> DataTypeLike:
    if value_type == "string":
        return pa.string()
    if value_type == "int":
        return pa.int64()
    if value_type == "float":
        return pa.float64()
    if value_type == "bool":
        return pa.bool_()
    return pa.string()


def _value_columns() -> tuple[str, ...]:
    return ("value_str", "value_int", "value_float", "value_bool", "value_json")


VALUE_STRUCT_FIELD = "value_struct"


def _null_value_exprs() -> dict[str, ComputeExpression]:
    return {
        "value_str": ensure_expression(pc.cast(pc.scalar(None), pa.string(), safe=False)),
        "value_int": ensure_expression(pc.cast(pc.scalar(None), pa.int64(), safe=False)),
        "value_float": ensure_expression(pc.cast(pc.scalar(None), pa.float64(), safe=False)),
        "value_bool": ensure_expression(pc.cast(pc.scalar(None), pa.bool_(), safe=False)),
        "value_json": ensure_expression(pc.cast(pc.scalar(None), pa.string(), safe=False)),
    }


def _json_literal_expr(value: object) -> ComputeExpression:
    try:
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except (TypeError, ValueError):
        encoded = json.dumps(str(value), ensure_ascii=False)
    return ensure_expression(pc.cast(pc.scalar(encoded), pa.string(), safe=False))


def _serialize_json_expr(
    expr: ComputeExpression,
    *,
    dtype: DataTypeLike | None,
    ctx: ExecutionContext,
) -> ComputeExpression:
    _ = ctx
    if dtype is None or patypes.is_string(dtype) or patypes.is_large_string(dtype):
        return ensure_expression(pc.cast(expr, pa.string(), safe=False))
    func_name = ensure_json_udf(dtype)
    return ensure_expression(pc.call_function(func_name, [expr]))


def _json_value_expr(
    expr: ComputeExpression,
    *,
    dtype: DataTypeLike | None,
    ctx: ExecutionContext,
) -> PropValueExpr:
    if dtype is None or patypes.is_string(dtype) or patypes.is_large_string(dtype):
        return PropValueExpr(
            expr=ensure_expression(pc.cast(expr, pa.string(), safe=False)),
            value_type="json",
        )
    if patypes.is_list(dtype):
        list_type = cast("pa.ListType", dtype)
        inner = list_type.value_type
        target_dtype = dtype
        if not patypes.is_large_list(dtype):
            target_dtype = pa.large_list(inner)
        coerced = ensure_expression(pc.cast(expr, target_dtype, safe=False))
        return PropValueExpr(
            expr=coerced,
            value_type="json",
            defer_json=True,
            json_dtype=target_dtype,
        )
    if patypes.is_struct(dtype):
        return PropValueExpr(
            expr=ensure_expression(expr),
            value_type="json",
            defer_json=True,
            json_dtype=dtype,
        )
    serialized = _serialize_json_expr(expr, dtype=dtype, ctx=ctx)
    return PropValueExpr(expr=serialized, value_type="json")


def _apply_transform(expr: ComputeExpression, field: PropFieldSpec) -> ComputeExpression:
    if field.transform is None:
        return expr
    if field.transform is expr_context_value:
        return expr_context_expr(expr)
    if field.transform is flag_to_bool:
        return flag_to_bool_expr(expr)
    msg = f"Unsupported prop transform: {field.transform!r}"
    raise ValueError(msg)


def _value_expr(
    field: PropFieldSpec,
    *,
    available: set[str],
    schema_types: dict[str, DataTypeLike],
    ctx: ExecutionContext,
) -> PropValueExpr:
    value_type = field.value_type
    if value_type is None:
        msg = f"Missing value_type for prop field {field.prop_key!r}."
        raise ValueError(msg)

    if field.literal is not None:
        if value_type == "json":
            expr = _json_literal_expr(field.literal)
        else:
            expr = ensure_expression(
                pc.cast(pc.scalar(field.literal), _value_dtype(value_type), safe=False)
            )
        return PropValueExpr(expr=expr, value_type=value_type)

    source_col = field.source_col
    if source_col is None or source_col not in available:
        expr = ensure_expression(pc.cast(pc.scalar(None), _value_dtype(value_type), safe=False))
    else:
        expr = pc.field(source_col)

    expr = _apply_transform(expr, field)
    if value_type == "json":
        dtype = schema_types.get(source_col) if source_col is not None else None
        return _json_value_expr(expr, dtype=dtype, ctx=ctx)

    return PropValueExpr(
        expr=ensure_expression(pc.cast(expr, _value_dtype(value_type), safe=False)),
        value_type=value_type,
    )


def _value_struct_expr(value_exprs: dict[str, ComputeExpression]) -> ComputeExpression:
    names = list(value_exprs.keys())
    exprs = [value_exprs[name] for name in names]
    return ensure_expression(pc.make_struct(*exprs, field_names=names))


def _expand_value_struct(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    defer_json: bool,
    json_dtype: DataTypeLike | None,
) -> Plan:
    value_struct = pc.field(VALUE_STRUCT_FIELD)
    value_str = ensure_expression(pc.struct_field(value_struct, "value_str"))
    value_int = ensure_expression(pc.struct_field(value_struct, "value_int"))
    value_float = ensure_expression(pc.struct_field(value_struct, "value_float"))
    value_bool = ensure_expression(pc.struct_field(value_struct, "value_bool"))
    value_json = ensure_expression(pc.struct_field(value_struct, "value_json"))
    if defer_json:
        value_json = _serialize_json_expr(value_json, dtype=json_dtype, ctx=ctx)
    else:
        value_json = ensure_expression(pc.cast(value_json, pa.string(), safe=False))

    exprs: list[ComputeExpression] = [
        pc.field("entity_kind"),
        pc.field("entity_id"),
        pc.field("prop_key"),
        value_str,
        value_int,
        value_float,
        value_bool,
        value_json,
    ]
    names = ["entity_kind", "entity_id", "prop_key", *_value_columns()]
    if "schema_version" in plan.schema(ctx=ctx).names:
        exprs.append(pc.field("schema_version"))
        names.append("schema_version")
    return plan.project(exprs, names, ctx=ctx)


def prop_field_plan(
    plan: Plan,
    *,
    field: PropFieldSpec,
    context: PropFieldPlanContext,
) -> Plan:
    """Return a plan that emits rows for a single property field.

    Returns
    -------
    Plan
        Plan emitting rows for the property field.
    """
    schema = plan.schema(ctx=context.ctx)
    available = set(schema.names)
    schema_types = {schema_field.name: schema_field.type for schema_field in schema}
    value = _value_expr(
        field,
        available=available,
        schema_types=schema_types,
        ctx=context.ctx,
    )
    value_exprs = _null_value_exprs()
    target_col = {
        "string": "value_str",
        "int": "value_int",
        "float": "value_float",
        "bool": "value_bool",
        "json": "value_json",
    }[value.value_type]
    value_exprs[target_col] = value.expr
    value_struct = _value_struct_expr(value_exprs)

    exprs: list[ComputeExpression] = [
        ensure_expression(pc.cast(pc.scalar(context.entity_kind), pa.string(), safe=False)),
        context.entity_id,
        ensure_expression(pc.cast(pc.scalar(field.prop_key), pa.string(), safe=False)),
        value_struct,
    ]
    names = ["entity_kind", "entity_id", "prop_key", VALUE_STRUCT_FIELD]
    if context.schema_version is not None:
        exprs.append(
            ensure_expression(pc.cast(pc.scalar(context.schema_version), pa.int32(), safe=False))
        )
        names.append("schema_version")
    out = plan.project(exprs, names, ctx=context.ctx)
    if field.skip_if_none:
        value_expr = ensure_expression(
            pc.struct_field(pc.field(VALUE_STRUCT_FIELD), target_col)
        )
        out = out.filter(ensure_expression(pc.is_valid(value_expr)), ctx=context.ctx)
    return _expand_value_struct(
        out,
        ctx=context.ctx,
        defer_json=value.defer_json,
        json_dtype=value.json_dtype,
    )


def emit_props_plans(
    plan: Plan,
    *,
    spec: PropTableSpec,
    schema_version: int | None,
    ctx: ExecutionContext,
) -> list[Plan]:
    """Return a list of property field plans for the spec.

    Returns
    -------
    list[Plan]
        Plans emitting property rows.
    """
    available = set(plan.schema(ctx=ctx).names)
    entity_id = _entity_id_expr(spec.id_cols, available=available)
    context = PropFieldPlanContext(
        entity_kind=spec.entity_kind.value,
        entity_id=entity_id,
        schema_version=schema_version,
        ctx=ctx,
    )
    plans: list[Plan] = []
    if spec.node_kind is not None:
        node_field = PropFieldSpec(
            prop_key="node_kind",
            literal=spec.node_kind.value,
            value_type="string",
        )
        plans.append(prop_field_plan(plan, field=node_field, context=context))

    plans.extend([prop_field_plan(plan, field=field, context=context) for field in spec.fields])
    return plans


def filter_fields(
    fields: Iterable[PropFieldSpec],
    *,
    options: PropOptions,
) -> list[PropFieldSpec]:
    """Return the PropFieldSpec list filtered by include_if.

    Returns
    -------
    list[PropFieldSpec]
        Filtered field specs.
    """
    selected: list[PropFieldSpec] = []
    for field in fields:
        if field.include_if is not None and not field.include_if(options):
            continue
        selected.append(field)
    return selected


__all__ = ["emit_props_plans", "filter_fields", "prop_field_plan"]
