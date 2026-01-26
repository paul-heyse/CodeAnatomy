"""Ibis-based node emission helpers."""

from __future__ import annotations

from collections.abc import Sequence

import ibis
import pyarrow as pa
from ibis.expr.types import Table, Value

from arrowdsl.core.ordering import Ordering
from cpg.schemas import CPG_NODES_SCHEMA
from cpg.specs import NodeEmitSpec
from ibis_engine.expr_compiler import expr_ir_to_ibis
from ibis_engine.hashing import HashExprSpec, masked_stable_id_expr_ir
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import (
    bind_expr_schema,
    coalesce_columns,
    ensure_columns,
    ibis_null_literal,
    validate_expr_schema,
)
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


def emit_nodes_ibis(
    rel: IbisPlan | Table,
    *,
    spec: NodeEmitSpec,
    task_name: str | None = None,
    task_priority: int | None = None,
) -> IbisPlan:
    """Emit CPG nodes from a relation table using Ibis expressions.

    Returns
    -------
    IbisPlan
        Ibis plan emitting node rows.
    """
    expr = rel.expr if isinstance(rel, IbisPlan) else rel
    expr, id_cols, required = _prepare_id_columns(expr, spec.id_cols)
    node_id = masked_stable_id_expr_from_spec(
        expr,
        spec=HashExprSpec(
            prefix="node",
            cols=id_cols,
            extra_literals=(str(spec.node_kind),),
            null_sentinel="None",
        ),
        required=required,
    )
    node_kind = ibis.literal(str(spec.node_kind))
    path = _coalesced(expr, spec.path_cols, pa.string())
    bstart = _coalesced(expr, spec.bstart_cols, pa.int64())
    bend = _coalesced(expr, spec.bend_cols, pa.int64())
    file_id = _coalesced(expr, spec.file_id_cols, pa.string())
    schema_names = set(CPG_NODES_SCHEMA.names)
    columns: dict[str, Value] = {
        "node_id": node_id,
        "node_kind": node_kind,
        "path": path,
        "bstart": bstart,
        "bend": bend,
        "file_id": file_id,
    }
    if "task_name" in schema_names:
        columns["task_name"] = _literal_or_null(task_name, pa.string())
    if "task_priority" in schema_names:
        columns["task_priority"] = _literal_or_null(task_priority, pa.int32())
    output = expr.mutate(**columns)
    output = ensure_columns(output, schema=CPG_NODES_SCHEMA, only_missing=True)
    output = output.select(*CPG_NODES_SCHEMA.names)
    validate_expr_schema(output, expected=CPG_NODES_SCHEMA, allow_extra_columns=False)
    output = bind_expr_schema(
        output,
        schema=CPG_NODES_SCHEMA,
        allow_extra_columns=False,
    )
    return IbisPlan(expr=output, ordering=Ordering.unordered())


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


def _coalesced(expr: Table, columns: Sequence[str], dtype: pa.DataType) -> Value:
    return coalesce_columns(expr, columns, default=ibis_null_literal(dtype))


def _literal_or_null(value: object | None, dtype: pa.DataType) -> Value:
    if value is None:
        return ibis_null_literal(dtype)
    return ibis.literal(value)


__all__ = ["emit_nodes_ibis"]
