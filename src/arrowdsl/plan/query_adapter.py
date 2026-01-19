"""Adapters between IbisQuerySpec and ArrowDSL plan QuerySpec."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Protocol, cast

from arrowdsl.spec.expr_ir import ExprIR
from ibis_engine.query_compiler import IbisQuerySpec

if TYPE_CHECKING:
    from arrowdsl.compute.expr_core import ExprSpec
    from arrowdsl.plan.query import ProjectionSpec, QuerySpec


class _PlanQueryModule(Protocol):
    ProjectionSpec: type[ProjectionSpec]
    QuerySpec: type[QuerySpec]


def _plan_query_module() -> _PlanQueryModule:
    module = importlib.import_module("arrowdsl.plan.query")
    return cast("_PlanQueryModule", module)


def _require_expr_ir(expr: object) -> ExprIR:
    if isinstance(expr, ExprIR):
        return expr
    msg = "Plan-lane adapter requires ExprIR expressions."
    raise TypeError(msg)


def _to_expr_spec(expr: object) -> ExprSpec:
    expr_ir = _require_expr_ir(expr)
    return expr_ir.to_expr_spec()


def ibis_query_to_plan_query(spec: IbisQuerySpec) -> QuerySpec:
    """Return a plan-lane QuerySpec for an IbisQuerySpec.

    Returns
    -------
    QuerySpec
        Plan-lane query spec derived from the Ibis query spec.
    """
    module = _plan_query_module()
    derived = {name: _to_expr_spec(expr) for name, expr in spec.projection.derived.items()}
    predicate = _to_expr_spec(spec.predicate) if spec.predicate is not None else None
    pushdown = (
        _to_expr_spec(spec.pushdown_predicate) if spec.pushdown_predicate is not None else None
    )
    return module.QuerySpec(
        projection=module.ProjectionSpec(base=spec.projection.base, derived=derived),
        predicate=predicate,
        pushdown_predicate=pushdown,
    )


__all__ = ["ibis_query_to_plan_query"]
