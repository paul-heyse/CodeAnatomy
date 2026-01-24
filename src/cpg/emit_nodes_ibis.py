"""Ibis-based node emission helpers."""

from __future__ import annotations

from collections.abc import Sequence

import ibis
import pyarrow as pa
from ibis.expr.types import Table, Value

from arrowdsl.core.ordering import Ordering
from cpg.schemas import CPG_NODES_SCHEMA
from cpg.specs import NodeEmitSpec
from ibis_engine.ids import masked_stable_id_expr
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import coalesce_columns, ensure_columns, ibis_null_literal


def emit_nodes_ibis(rel: IbisPlan | Table, *, spec: NodeEmitSpec) -> IbisPlan:
    """Emit CPG nodes from a relation table using Ibis expressions.

    Returns
    -------
    IbisPlan
        Ibis plan emitting node rows.
    """
    expr = rel.expr if isinstance(rel, IbisPlan) else rel
    id_values, required = _id_values(expr, spec.id_cols)
    node_id = masked_stable_id_expr(
        "node",
        parts=(str(spec.node_kind), *id_values),
        required=required,
    )
    node_kind = ibis.literal(str(spec.node_kind))
    path = _coalesced(expr, spec.path_cols, pa.string())
    bstart = _coalesced(expr, spec.bstart_cols, pa.int64())
    bend = _coalesced(expr, spec.bend_cols, pa.int64())
    file_id = _coalesced(expr, spec.file_id_cols, pa.string())
    output = expr.mutate(
        node_id=node_id,
        node_kind=node_kind,
        path=path,
        bstart=bstart,
        bend=bend,
        file_id=file_id,
    )
    output = ensure_columns(output, schema=CPG_NODES_SCHEMA, only_missing=True)
    output = output.select(*CPG_NODES_SCHEMA.names)
    return IbisPlan(expr=output, ordering=Ordering.unordered())


def _id_values(expr: Table, columns: Sequence[str]) -> tuple[tuple[Value, ...], tuple[Value, ...]]:
    values: list[Value] = []
    required: list[Value] = []
    for column in columns:
        if column in expr.columns:
            value = expr[column]
            values.append(value)
            required.append(value)
        else:
            values.append(ibis_null_literal(pa.string()))
    return tuple(values), tuple(required)


def _coalesced(expr: Table, columns: Sequence[str], dtype: pa.DataType) -> Value:
    return coalesce_columns(expr, columns, default=ibis_null_literal(dtype))


__all__ = ["emit_nodes_ibis"]
