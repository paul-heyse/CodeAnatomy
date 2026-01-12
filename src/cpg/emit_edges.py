"""Plan-lane edge emission helpers."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import HashSpec, hash_expression
from arrowdsl.core.interop import ComputeExpression, DataTypeLike, ensure_expression, pc
from arrowdsl.plan.plan import Plan
from cpg.specs import EdgeEmitSpec


def _field_expr(
    name: str,
    *,
    available: set[str],
    dtype: DataTypeLike,
) -> ComputeExpression:
    if name in available:
        return ensure_expression(pc.cast(pc.field(name), dtype, safe=False))
    return ensure_expression(pc.cast(pc.scalar(None), dtype, safe=False))


def _coalesce_expr(
    cols: Sequence[str],
    *,
    available: set[str],
    dtype: DataTypeLike,
) -> ComputeExpression:
    exprs = [
        ensure_expression(pc.cast(pc.field(col), dtype, safe=False))
        for col in cols
        if col in available
    ]
    if not exprs:
        return ensure_expression(pc.cast(pc.scalar(None), dtype, safe=False))
    if len(exprs) == 1:
        return exprs[0]
    return ensure_expression(pc.coalesce(*exprs))


EDGE_OUTPUT_NAMES: tuple[str, ...] = (
    "edge_id",
    "edge_kind",
    "src_node_id",
    "dst_node_id",
    "path",
    "bstart",
    "bend",
    "origin",
    "resolution_method",
    "confidence",
    "score",
    "symbol_roles",
    "qname_source",
    "ambiguity_group_id",
    "rule_name",
    "rule_priority",
)


def _normalized_relation_plan(
    rel: Plan,
    *,
    spec: EdgeEmitSpec,
    ctx: ExecutionContext,
) -> Plan:
    available = set(rel.schema(ctx=ctx).names)
    exprs = [
        _coalesce_expr(spec.src_cols, available=available, dtype=pa.string()),
        _coalesce_expr(spec.dst_cols, available=available, dtype=pa.string()),
        _coalesce_expr(spec.path_cols, available=available, dtype=pa.string()),
        _coalesce_expr(spec.bstart_cols, available=available, dtype=pa.int64()),
        _coalesce_expr(spec.bend_cols, available=available, dtype=pa.int64()),
        _field_expr("origin", available=available, dtype=pa.string()),
        _field_expr("resolution_method", available=available, dtype=pa.string()),
        _field_expr("confidence", available=available, dtype=pa.float32()),
        _field_expr("score", available=available, dtype=pa.float32()),
        _field_expr("symbol_roles", available=available, dtype=pa.int32()),
        _field_expr("qname_source", available=available, dtype=pa.string()),
        _field_expr("ambiguity_group_id", available=available, dtype=pa.string()),
        _field_expr("rule_name", available=available, dtype=pa.string()),
        _field_expr("rule_priority", available=available, dtype=pa.int32()),
    ]
    names = [
        "src",
        "dst",
        "path",
        "bstart",
        "bend",
        "origin",
        "resolution_method",
        "confidence",
        "score",
        "symbol_roles",
        "qname_source",
        "ambiguity_group_id",
        "rule_name",
        "rule_priority",
    ]
    return rel.project(exprs, names, ctx=ctx)


def emit_edges_plan(
    rel: Plan,
    *,
    spec: EdgeEmitSpec,
    ctx: ExecutionContext,
) -> Plan:
    """Project an edge emission plan from a relation plan.

    Returns
    -------
    Plan
        Plan emitting edge columns.
    """
    rel_norm = _normalized_relation_plan(rel, spec=spec, ctx=ctx)
    available = rel_norm.schema(ctx=ctx).names

    base_spec = HashSpec(
        prefix="edge",
        cols=("src", "dst"),
        extra_literals=(spec.edge_kind.value,),
        as_string=True,
    )
    full_spec = HashSpec(
        prefix="edge",
        cols=("src", "dst", "path", "bstart", "bend"),
        extra_literals=(spec.edge_kind.value,),
        as_string=True,
    )
    base_id = hash_expression(base_spec, available=available)
    full_id = hash_expression(full_spec, available=available)

    has_span = ensure_expression(
        pc.and_(
            pc.is_valid(pc.field("path")),
            pc.and_(pc.is_valid(pc.field("bstart")), pc.is_valid(pc.field("bend"))),
        )
    )
    edge_id = ensure_expression(pc.if_else(has_span, full_id, base_id))
    valid = ensure_expression(
        pc.and_(pc.is_valid(pc.field("src")), pc.is_valid(pc.field("dst")))
    )
    edge_id = ensure_expression(
        pc.if_else(valid, edge_id, pc.cast(pc.scalar(None), pa.string(), safe=False))
    )

    default_score = 1.0 if spec.origin == "scip" else 0.5
    origin = ensure_expression(
        pc.cast(
            pc.fill_null(pc.field("origin"), pc.scalar(spec.origin)),
            pa.string(),
            safe=False,
        )
    )
    resolution_method = ensure_expression(
        pc.cast(
            pc.fill_null(
                pc.field("resolution_method"),
                pc.scalar(spec.default_resolution_method),
            ),
            pa.string(),
            safe=False,
        )
    )
    confidence = ensure_expression(
        pc.cast(
            pc.fill_null(pc.field("confidence"), pc.scalar(default_score)),
            pa.float32(),
            safe=False,
        )
    )
    score = ensure_expression(
        pc.cast(
            pc.fill_null(pc.field("score"), pc.scalar(default_score)),
            pa.float32(),
            safe=False,
        )
    )

    exprs = [
        edge_id,
        ensure_expression(pc.cast(pc.scalar(spec.edge_kind.value), pa.string(), safe=False)),
        pc.field("src"),
        pc.field("dst"),
        pc.field("path"),
        pc.field("bstart"),
        pc.field("bend"),
        origin,
        resolution_method,
        confidence,
        score,
        pc.field("symbol_roles"),
        pc.field("qname_source"),
        pc.field("ambiguity_group_id"),
        pc.field("rule_name"),
        pc.field("rule_priority"),
    ]
    return rel_norm.project(exprs, list(EDGE_OUTPUT_NAMES), ctx=ctx)


__all__ = ["emit_edges_plan"]
