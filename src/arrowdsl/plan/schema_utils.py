"""Schema helpers for plan-like objects."""

from __future__ import annotations

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from arrowdsl.plan.plan import Plan
from ibis_engine.plan import IbisPlan


def plan_schema(plan: Plan | IbisPlan, *, ctx: ExecutionContext) -> SchemaLike:
    """Return the schema for a Plan or IbisPlan without materialization.

    Returns
    -------
    SchemaLike
        Arrow schema representing the plan output.
    """
    if isinstance(plan, IbisPlan):
        return plan.expr.schema().to_pyarrow()
    return plan.schema(ctx=ctx)


__all__ = ["plan_schema"]
