"""Plan builders for CPG node/edge/property tasks."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import TYPE_CHECKING

import pyarrow as pa
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.ordering import Ordering
from arrowdsl.schema.build import empty_table
from cpg.emit_edges_ibis import emit_edges_from_relation_output
from cpg.emit_nodes_ibis import emit_nodes_ibis
from cpg.emit_props_ibis import CpgPropOptions, emit_props_ibis
from cpg.schemas import CPG_EDGES_SCHEMA, CPG_NODES_SCHEMA, CPG_PROPS_SCHEMA
from cpg.spec_registry import (
    edge_prop_spec,
    node_plan_specs,
    prop_table_specs,
    scip_role_flag_prop_spec,
)
from ibis_engine.catalog import IbisPlanCatalog
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import ensure_columns
from ibis_engine.sources import SourceToIbisOptions, register_ibis_table
from relspec.relationship_plans import RELATION_OUTPUT_NAME

if TYPE_CHECKING:
    from ibis.expr.types import Table

    from cpg.specs import PropOptions


def build_cpg_nodes_plan(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    backend: BaseBackend,
    task_name: str | None = None,
    task_priority: int | None = None,
) -> IbisPlan:
    """Return the CPG nodes plan compiled from node specs.

    Returns
    -------
    IbisPlan
        Plan for CPG nodes.
    """
    plans: list[IbisPlan] = []
    for spec in node_plan_specs():
        table = _resolve_table(catalog, ctx=ctx, name=spec.table_ref)
        plan = emit_nodes_ibis(
            table,
            spec=spec.emit,
            task_name=task_name,
            task_priority=task_priority,
        )
        plans.append(plan)
    return _union_plans(plans, schema=CPG_NODES_SCHEMA, backend=backend)


def build_cpg_edges_plan(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    backend: BaseBackend,
) -> IbisPlan:
    """Return the CPG edges plan compiled from relation outputs.

    Returns
    -------
    IbisPlan
        Plan for CPG edges.
    """
    _ = backend
    relation_expr = _resolve_table(catalog, ctx=ctx, name=RELATION_OUTPUT_NAME)
    plan = emit_edges_from_relation_output(relation_expr)
    return _ensure_plan_schema(plan, schema=CPG_EDGES_SCHEMA)


def build_cpg_props_plan(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    backend: BaseBackend,
    options: PropOptions | None = None,
    task_name: str | None = None,
    task_priority: int | None = None,
) -> IbisPlan:
    """Return the CPG props plan compiled from prop specs.

    Returns
    -------
    IbisPlan
        Plan for CPG properties.
    """
    resolved_options = options or CpgPropOptions()
    source_columns_lookup = _source_columns_lookup(catalog, ctx=ctx)
    prop_specs = list(prop_table_specs(source_columns_lookup=source_columns_lookup))
    prop_specs.append(scip_role_flag_prop_spec())
    prop_specs.append(edge_prop_spec())
    plans: list[IbisPlan] = []
    for spec in prop_specs:
        table = _resolve_table(catalog, ctx=ctx, name=spec.table_ref)
        plan = emit_props_ibis(
            table,
            spec=spec,
            options=resolved_options,
            task_name=task_name,
            task_priority=task_priority,
        )
        plans.append(plan)
    return _union_plans(plans, schema=CPG_PROPS_SCHEMA, backend=backend)


def _source_columns_lookup(
    catalog: IbisPlanCatalog,
    *,
    ctx: ExecutionContext,
) -> Callable[[str], Sequence[str] | None]:
    def _lookup(table_name: str) -> Sequence[str] | None:
        try:
            table = _resolve_table(catalog, ctx=ctx, name=table_name)
        except KeyError:
            return None
        return tuple(table.columns)

    return _lookup


def _resolve_table(catalog: IbisPlanCatalog, *, ctx: ExecutionContext, name: str) -> Table:
    plan = catalog.resolve_plan(name, ctx=ctx, label=name)
    if plan is not None:
        return plan.expr
    schema = _schema_for(name)
    if schema is None:
        msg = f"Missing schema for CPG input {name!r}."
        raise KeyError(msg)
    empty = empty_table(schema)
    plan = register_ibis_table(
        empty,
        options=SourceToIbisOptions(
            backend=catalog.backend,
            name=name,
            ordering=Ordering.unordered(),
        ),
    )
    catalog.add(name, plan)
    return plan.expr


def _schema_for(name: str) -> pa.Schema | None:
    try:
        from datafusion_engine.schema_registry import schema_for
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None
    try:
        return schema_for(name)
    except KeyError:
        return None


def _union_plans(plans: Iterable[IbisPlan], *, schema: pa.Schema, backend: BaseBackend) -> IbisPlan:
    exprs = [plan.expr for plan in plans]
    if not exprs:
        return _empty_plan(schema, backend=backend)
    combined = _union_exprs(exprs)
    combined = ensure_columns(combined, schema=schema, only_missing=True)
    combined = combined.select(*schema.names)
    return IbisPlan(expr=combined, ordering=Ordering.unordered())


def _union_exprs(exprs: Sequence[Table]) -> Table:
    iterator = iter(exprs)
    combined = next(iterator)
    for expr in iterator:
        combined = combined.union(expr)
    return combined


def _empty_plan(schema: pa.Schema, *, backend: BaseBackend) -> IbisPlan:
    empty = empty_table(schema)
    return register_ibis_table(
        empty,
        options=SourceToIbisOptions(
            backend=backend,
            name=None,
            ordering=Ordering.unordered(),
        ),
    )


def _ensure_plan_schema(plan: IbisPlan, *, schema: pa.Schema) -> IbisPlan:
    expr = ensure_columns(plan.expr, schema=schema, only_missing=True)
    expr = expr.select(*schema.names)
    return IbisPlan(expr=expr, ordering=Ordering.unordered())


__all__ = [
    "build_cpg_edges_plan",
    "build_cpg_nodes_plan",
    "build_cpg_props_plan",
]
