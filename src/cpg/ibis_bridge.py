"""Ibis bridge helpers for CPG plan outputs."""

from __future__ import annotations

from collections.abc import Mapping

from ibis.backends import BaseBackend

from arrowdsl.core.context import ExecutionContext
from arrowdsl.plan.plan import Plan
from ibis_engine.plan import IbisPlan
from ibis_engine.plan_bridge import plan_to_ibis


def plan_bundle_to_ibis(
    plans: Mapping[str, Plan],
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    name_prefix: str = "cpg_rel",
) -> dict[str, IbisPlan]:
    """Convert a CPG plan bundle into Ibis plans.

    Returns
    -------
    dict[str, IbisPlan]
        Mapping of output names to Ibis plans.
    """
    return {
        name: plan_to_ibis(plan, ctx=ctx, backend=backend, name=f"{name_prefix}_{name}")
        for name, plan in plans.items()
    }


__all__ = ["plan_bundle_to_ibis"]
