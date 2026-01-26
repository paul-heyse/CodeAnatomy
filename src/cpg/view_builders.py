"""View builders for CPG node/edge/property outputs."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.ordering import Ordering
from cpg.emit_edges_ibis import emit_edges_from_relation_output
from cpg.emit_nodes_ibis import emit_nodes_ibis
from cpg.emit_props_ibis import CpgPropOptions, emit_props_ibis
from cpg.spec_registry import (
    edge_prop_spec,
    node_plan_specs,
    prop_table_specs,
    scip_role_flag_prop_spec,
)
from cpg.specs import TaskIdentity
from ibis_engine.catalog import IbisPlanCatalog
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import ensure_columns
from relspec.view_defs import RELATION_OUTPUT_NAME

if TYPE_CHECKING:
    from ibis.expr.types import Table

    from cpg.specs import PropOptions


def build_cpg_nodes_expr(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
    *,
    task_identity: TaskIdentity | None = None,
) -> IbisPlan:
    """Return an Ibis plan for CPG nodes derived from view specs.

    Returns
    -------
    IbisPlan
        Plan for CPG nodes.
    """
    specs = node_plan_specs()
    task_name = task_identity.name if task_identity is not None else None
    task_priority = task_identity.priority if task_identity is not None else None
    plans: list[IbisPlan] = []
    for spec in specs:
        table = _resolve_table_expr(catalog, ctx=ctx, name=spec.table_ref)
        plan = emit_nodes_ibis(
            table,
            spec=spec.emit,
            task_name=task_name,
            task_priority=task_priority,
        )
        plans.append(plan)
    return _union_plans(plans)


def build_cpg_edges_expr(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
) -> IbisPlan:
    """Return an Ibis plan for CPG edges from relation outputs.

    Returns
    -------
    IbisPlan
        Plan for CPG edges.
    """
    relation_expr = _resolve_table_expr(catalog, ctx=ctx, name=RELATION_OUTPUT_NAME)
    return emit_edges_from_relation_output(relation_expr)


def build_cpg_props_expr(
    catalog: IbisPlanCatalog,
    ctx: ExecutionContext,
    _backend: BaseBackend,
    *,
    options: PropOptions | None = None,
    task_identity: TaskIdentity | None = None,
) -> IbisPlan:
    """Return an Ibis plan for CPG properties from prop specs.

    Returns
    -------
    IbisPlan
        Plan for CPG properties.
    """
    resolved_options = options or CpgPropOptions()
    source_columns_lookup = _source_columns_lookup(catalog)
    prop_specs = list(prop_table_specs(source_columns_lookup=source_columns_lookup))
    prop_specs.append(scip_role_flag_prop_spec())
    prop_specs.append(edge_prop_spec())
    plans: list[IbisPlan] = []
    for spec in prop_specs:
        table = _resolve_table_expr(catalog, ctx=ctx, name=spec.table_ref)
        plan = emit_props_ibis(
            table,
            spec=spec,
            options=resolved_options,
            task_identity=task_identity,
            union_builder=None,
        )
        plans.append(plan)
    return _union_plans(plans)


def _resolve_table_expr(
    catalog: IbisPlanCatalog,
    *,
    ctx: ExecutionContext,
    name: str,
) -> Table:
    try:
        return catalog.resolve_expr(name, ctx=ctx)
    except KeyError as exc:
        msg = f"Missing required source table {name!r} for CPG view."
        raise ValueError(msg) from exc


def _source_columns_lookup(
    catalog: IbisPlanCatalog,
) -> Callable[[str], Sequence[str] | None]:
    columns_by_table: dict[str, tuple[str, ...]] = {}
    for name, source in catalog.tables.items():
        cols = _columns_from_source(source)
        if cols:
            columns_by_table[name] = tuple(sorted(cols))

    def _lookup(table_name: str) -> Sequence[str] | None:
        return columns_by_table.get(table_name)

    return _lookup


def _columns_from_source(source: object) -> Sequence[str]:
    if isinstance(source, IbisPlan):
        schema = source.expr.schema()
        return cast("Sequence[str]", schema.names)
    schema = getattr(source, "schema", None)
    if schema is not None:
        names = getattr(schema, "names", None)
        if names is not None:
            return cast("Sequence[str]", names)
        fields = getattr(schema, "fields", None)
        if fields is not None:
            return tuple(field.name for field in fields if hasattr(field, "name"))
    return ()


def _union_plans(plans: Iterable[IbisPlan]) -> IbisPlan:
    exprs = [plan.expr for plan in plans]
    if not exprs:
        msg = "CPG view builder did not produce any plans."
        raise ValueError(msg)
    combined = _union_exprs(exprs)
    schema = _merge_schema_from_exprs(exprs)
    if schema is None:
        msg = "Unable to derive CPG schema from plan outputs."
        raise ValueError(msg)
    combined = ensure_columns(combined, schema=schema, only_missing=True)
    combined = combined.select(*schema.names)
    return IbisPlan(expr=combined, ordering=Ordering.unordered())


def _union_exprs(exprs: Sequence[Table]) -> Table:
    iterator = iter(exprs)
    combined = next(iterator)
    for expr in iterator:
        combined = combined.union(expr)
    return combined


def _merge_schema_from_exprs(exprs: Sequence[Table]) -> pa.Schema | None:
    if not exprs:
        return None
    fields: dict[str, pa.Field] = {}
    for expr in exprs:
        schema = expr.schema()
        arrow = schema.to_pyarrow() if hasattr(schema, "to_pyarrow") else None
        if arrow is None:
            continue
        for field in arrow:
            fields.setdefault(field.name, field)
    if not fields:
        return None
    return pa.schema(list(fields.values()))


__all__ = [
    "build_cpg_edges_expr",
    "build_cpg_nodes_expr",
    "build_cpg_props_expr",
]
