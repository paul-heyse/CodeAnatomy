"""Ibis bridge wrappers for normalize plan builders."""

from __future__ import annotations

from collections.abc import Callable, Mapping

from ibis.backends import BaseBackend

from arrowdsl.core.context import ExecutionContext
from arrowdsl.plan.catalog import PlanCatalog, PlanDeriver
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.scan_io import PlanSource, plan_from_source
from ibis_engine.plan import IbisPlan
from ibis_engine.plan_bridge import plan_to_ibis
from normalize.bytecode_cfg_plans import CFG_BLOCKS_NAME, CFG_EDGES_NAME
from normalize.bytecode_dfg_plans import DEF_USE_NAME, REACHES_NAME
from normalize.diagnostics_plans import DIAG_NAME
from normalize.plan_builders import (
    cfg_blocks_builder,
    cfg_edges_builder,
    def_use_events_builder,
    diagnostics_builder,
    reaching_defs_builder,
    span_errors_builder,
    type_exprs_builder,
    type_nodes_builder,
)
from normalize.types_plans import TYPE_EXPRS_NAME, TYPE_NODES_NAME

type IbisPlanDeriver = Callable[[PlanCatalog, ExecutionContext, BaseBackend], IbisPlan | None]


def _ensure_plan(value: Plan | PlanSource, *, ctx: ExecutionContext, label: str) -> Plan:
    if isinstance(value, Plan):
        return value
    return plan_from_source(value, ctx=ctx, label=label)


def _wrap_builder(builder: PlanDeriver, *, view_name: str) -> IbisPlanDeriver:
    def _derive(
        catalog: PlanCatalog,
        ctx: ExecutionContext,
        backend: BaseBackend,
    ) -> IbisPlan | None:
        derived = builder(catalog, ctx)
        if derived is None:
            return None
        plan = _ensure_plan(derived, ctx=ctx, label=view_name)
        return plan_to_ibis(plan, ctx=ctx, backend=backend, name=view_name)

    return _derive


PLAN_BUILDERS_IBIS: Mapping[str, IbisPlanDeriver] = {
    "type_exprs": _wrap_builder(type_exprs_builder, view_name=TYPE_EXPRS_NAME),
    "type_nodes": _wrap_builder(type_nodes_builder, view_name=TYPE_NODES_NAME),
    "cfg_blocks": _wrap_builder(cfg_blocks_builder, view_name=CFG_BLOCKS_NAME),
    "cfg_edges": _wrap_builder(cfg_edges_builder, view_name=CFG_EDGES_NAME),
    "def_use_events": _wrap_builder(def_use_events_builder, view_name=DEF_USE_NAME),
    "reaching_defs": _wrap_builder(reaching_defs_builder, view_name=REACHES_NAME),
    "diagnostics": _wrap_builder(diagnostics_builder, view_name=DIAG_NAME),
    "span_errors": _wrap_builder(span_errors_builder, view_name="span_errors_v1"),
}


def resolve_plan_builder_ibis(name: str) -> IbisPlanDeriver:
    """Return an Ibis plan builder by name.

    Returns
    -------
    IbisPlanDeriver
        Builder that returns an IbisPlan or ``None`` when inputs are missing.

    Raises
    ------
    KeyError
        Raised when the builder name is unknown.
    """
    builder = PLAN_BUILDERS_IBIS.get(name)
    if builder is None:
        msg = f"Unknown normalize Ibis plan builder: {name!r}."
        raise KeyError(msg)
    return builder


def plan_builders_ibis() -> Mapping[str, IbisPlanDeriver]:
    """Return the registered normalize Ibis plan builders.

    Returns
    -------
    Mapping[str, IbisPlanDeriver]
        Mapping of builder names to Ibis plan-deriver callables.
    """
    return PLAN_BUILDERS_IBIS


__all__ = [
    "PLAN_BUILDERS_IBIS",
    "IbisPlanDeriver",
    "plan_builders_ibis",
    "resolve_plan_builder_ibis",
]
