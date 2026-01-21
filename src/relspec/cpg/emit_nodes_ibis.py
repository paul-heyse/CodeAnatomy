"""Ibis-based node emission helpers for CPG builders."""

from __future__ import annotations

from typing import Literal

import ibis
import pyarrow as pa
import pyarrow.types as patypes
from ibis.expr.types import Table, Value

from arrowdsl.core.ordering import Ordering
from cpg.schemas import CPG_NODES_SCHEMA
from cpg.specs import NodeEmitSpec
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import coalesce_columns, ibis_null_literal, validate_expr_schema


def emit_nodes_ibis(
    rel: IbisPlan | Table,
    *,
    spec: NodeEmitSpec,
) -> IbisPlan:
    """Emit CPG nodes from a relation table using Ibis expressions.

    Returns
    -------
    IbisPlan
        Ibis plan emitting node rows.
    """
    expr = rel.expr if isinstance(rel, IbisPlan) else rel
    node_id = _coalesce_cast(expr, spec.id_cols, dtype=pa.string())
    path = _coalesce_cast(expr, spec.path_cols, dtype=pa.string())
    bstart = _coalesce_cast(expr, spec.bstart_cols, dtype=pa.int64())
    bend = _coalesce_cast(expr, spec.bend_cols, dtype=pa.int64())
    file_id = _coalesce_cast(expr, spec.file_id_cols, dtype=pa.string())
    node_kind = ibis.literal(spec.node_kind.value)
    output = expr.select(
        node_id=node_id,
        node_kind=node_kind,
        path=path,
        bstart=bstart,
        bend=bend,
        file_id=file_id,
    )
    validate_expr_schema(output, expected=CPG_NODES_SCHEMA)
    return IbisPlan(expr=output, ordering=Ordering.unordered())


def _coalesce_cast(table: Table, cols: tuple[str, ...], *, dtype: pa.DataType) -> Value:
    value = coalesce_columns(table, cols, default=ibis_null_literal(dtype))
    return value.cast(_ibis_cast_type(dtype))


def _ibis_cast_type(dtype: pa.DataType) -> Literal["string", "int64"]:
    if patypes.is_string(dtype):
        return "string"
    if patypes.is_integer(dtype):
        return "int64"
    msg = f"Unsupported cast dtype for node emission: {dtype}"
    raise ValueError(msg)


__all__ = ["emit_nodes_ibis"]
