"""Plan-lane edge emission helpers."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.compute.filters import valid_mask_expr
from arrowdsl.compute.macros import null_expr, scalar_expr
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import hash_expression
from arrowdsl.core.interop import ensure_expression, pc
from arrowdsl.plan.plan import Plan
from arrowdsl.plan_helpers import coalesce_expr, column_or_null_expr
from cpg.constants import edge_hash_specs
from cpg.specs import EdgeEmitSpec

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
        coalesce_expr(
            spec.src_cols,
            available=available,
            dtype=pa.string(),
            cast=True,
            safe=False,
        ),
        coalesce_expr(
            spec.dst_cols,
            available=available,
            dtype=pa.string(),
            cast=True,
            safe=False,
        ),
        coalesce_expr(
            spec.path_cols,
            available=available,
            dtype=pa.string(),
            cast=True,
            safe=False,
        ),
        coalesce_expr(
            spec.bstart_cols,
            available=available,
            dtype=pa.int64(),
            cast=True,
            safe=False,
        ),
        coalesce_expr(
            spec.bend_cols,
            available=available,
            dtype=pa.int64(),
            cast=True,
            safe=False,
        ),
        column_or_null_expr(
            "origin",
            available=available,
            dtype=pa.string(),
            cast=True,
            safe=False,
        ),
        column_or_null_expr(
            "resolution_method",
            available=available,
            dtype=pa.string(),
            cast=True,
            safe=False,
        ),
        column_or_null_expr(
            "confidence",
            available=available,
            dtype=pa.float32(),
            cast=True,
            safe=False,
        ),
        column_or_null_expr(
            "score",
            available=available,
            dtype=pa.float32(),
            cast=True,
            safe=False,
        ),
        column_or_null_expr(
            "symbol_roles",
            available=available,
            dtype=pa.int32(),
            cast=True,
            safe=False,
        ),
        column_or_null_expr(
            "qname_source",
            available=available,
            dtype=pa.string(),
            cast=True,
            safe=False,
        ),
        column_or_null_expr(
            "ambiguity_group_id",
            available=available,
            dtype=pa.string(),
            cast=True,
            safe=False,
        ),
        column_or_null_expr(
            "rule_name",
            available=available,
            dtype=pa.string(),
            cast=True,
            safe=False,
        ),
        column_or_null_expr(
            "rule_priority",
            available=available,
            dtype=pa.int32(),
            cast=True,
            safe=False,
        ),
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

    base_spec, full_spec = edge_hash_specs(spec.edge_kind.value)
    base_id = hash_expression(base_spec, available=available)
    full_id = hash_expression(full_spec, available=available)

    has_span = valid_mask_expr(["path", "bstart", "bend"], available=available)
    edge_id = ensure_expression(pc.if_else(has_span, full_id, base_id))
    valid = valid_mask_expr(["src", "dst"], available=available)
    edge_id = ensure_expression(pc.if_else(valid, edge_id, null_expr(pa.string())))

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
        scalar_expr(spec.edge_kind.value, dtype=pa.string()),
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
