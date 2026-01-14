"""Ibis-based edge emission helpers."""

from __future__ import annotations

import ibis
import pyarrow as pa
from ibis.expr.types import Table, Value

from arrowdsl.core.context import Ordering
from cpg.schemas import CPG_EDGES_SCHEMA
from cpg.specs import EdgeEmitSpec
from ibis_engine.ids import stable_id_expr
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import align_table_to_schema, coalesce_columns, ibis_null_literal


def emit_edges_ibis(rel: IbisPlan | Table, *, spec: EdgeEmitSpec) -> IbisPlan:
    """Emit CPG edges from a relation table using Ibis expressions.

    Returns
    -------
    IbisPlan
        Ibis plan emitting edge rows.
    """
    expr = rel.expr if isinstance(rel, IbisPlan) else rel
    normalized = _normalized_relation_expr(expr, spec=spec)
    edge_id = _edge_id_expr(normalized, spec=spec)
    edge_kind = ibis.literal(spec.edge_kind.value)
    output = normalized.mutate(edge_id=edge_id, edge_kind=edge_kind)
    output = output.select(
        edge_id=output.edge_id,
        edge_kind=output.edge_kind,
        src_node_id=output.src,
        dst_node_id=output.dst,
        path=output.path,
        bstart=output.bstart,
        bend=output.bend,
        origin=output.origin,
        resolution_method=output.resolution_method,
        confidence=output.confidence,
        score=output.score,
        symbol_roles=output.symbol_roles,
        qname_source=output.qname_source,
        ambiguity_group_id=output.ambiguity_group_id,
        rule_name=output.rule_name,
        rule_priority=output.rule_priority,
    )
    aligned = align_table_to_schema(output, schema=CPG_EDGES_SCHEMA)
    return IbisPlan(expr=aligned, ordering=Ordering.unordered())


def _edge_id_expr(rel: Table, *, spec: EdgeEmitSpec) -> Value:
    edge_kind = spec.edge_kind.value
    base_id = stable_id_expr("edge", edge_kind, rel.src, rel.dst)
    span_id = stable_id_expr(
        "edge",
        edge_kind,
        rel.src,
        rel.dst,
        rel.path,
        rel.bstart,
        rel.bend,
    )
    has_span = rel.path.notnull() & rel.bstart.notnull() & rel.bend.notnull()
    valid_nodes = rel.src.notnull() & rel.dst.notnull()
    return ibis.ifelse(valid_nodes, ibis.ifelse(has_span, span_id, base_id), ibis.null())


def _normalized_relation_expr(rel: Table, *, spec: EdgeEmitSpec) -> Table:
    src = coalesce_columns(rel, spec.src_cols, default=ibis_null_literal(pa.string()))
    dst = coalesce_columns(rel, spec.dst_cols, default=ibis_null_literal(pa.string()))
    path = coalesce_columns(rel, spec.path_cols, default=ibis_null_literal(pa.string()))
    bstart = coalesce_columns(rel, spec.bstart_cols, default=ibis_null_literal(pa.int64()))
    bend = coalesce_columns(rel, spec.bend_cols, default=ibis_null_literal(pa.int64()))
    origin = ibis.coalesce(
        _optional_column(rel, "origin", pa.string()),
        ibis.literal(spec.origin),
    )
    resolution_method = ibis.coalesce(
        _optional_column(rel, "resolution_method", pa.string()),
        ibis.literal(spec.default_resolution_method),
    )
    default_score = 1.0 if spec.origin == "scip" else 0.5
    confidence = ibis.coalesce(
        _optional_column(rel, "confidence", pa.float32()),
        ibis.literal(default_score),
    )
    score = ibis.coalesce(
        _optional_column(rel, "score", pa.float32()),
        ibis.literal(default_score),
    )
    symbol_roles = _optional_column(rel, "symbol_roles", pa.int32())
    qname_source = _optional_column(rel, "qname_source", pa.string())
    ambiguity_group_id = _optional_column(rel, "ambiguity_group_id", pa.string())
    rule_name = _optional_column(rel, "rule_name", pa.string())
    rule_priority = _optional_column(rel, "rule_priority", pa.int32())
    return rel.mutate(
        src=src,
        dst=dst,
        path=path,
        bstart=bstart,
        bend=bend,
        origin=origin,
        resolution_method=resolution_method,
        confidence=confidence,
        score=score,
        symbol_roles=symbol_roles,
        qname_source=qname_source,
        ambiguity_group_id=ambiguity_group_id,
        rule_name=rule_name,
        rule_priority=rule_priority,
    )


def _optional_column(rel: Table, name: str, dtype: pa.DataType) -> Value:
    if name in rel.columns:
        return rel[name]
    return ibis_null_literal(dtype)


__all__ = ["emit_edges_ibis"]
