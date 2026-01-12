"""Shared table catalog and references for CPG builders."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.compute.predicates import bitmask_is_set_expr
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, ensure_expression, pc
from arrowdsl.plan import catalog as plan_catalog
from arrowdsl.plan.plan import Plan
from arrowdsl.plan_helpers import coalesce_expr
from cpg.plan_helpers import set_or_append_column
from cpg.role_flags import ROLE_FLAG_SPECS

PlanCatalog = plan_catalog.PlanCatalog
PlanDeriver = plan_catalog.PlanDeriver
PlanGetter = plan_catalog.PlanGetter
PlanRef = plan_catalog.PlanRef
PlanSource = plan_catalog.PlanSource
TableCatalog = plan_catalog.TableCatalog
TableDeriver = plan_catalog.TableDeriver
TableGetter = plan_catalog.TableGetter
TableRef = plan_catalog.TableRef


def derive_cst_defs_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Derive a normalized CST definitions plan when available.

    Returns
    -------
    Plan | None
        Derived plan or ``None`` when unavailable.
    """
    defs = catalog.resolve(PlanRef("cst_defs"), ctx=ctx)
    if defs is None:
        return None
    available = set(defs.schema(ctx=ctx).names)
    expr = coalesce_expr(
        ("def_kind", "kind"),
        dtype=pa.string(),
        available=available,
        cast=True,
        safe=False,
    )
    return set_or_append_column(defs, name="def_kind_norm", expr=expr, ctx=ctx)


def derive_scip_role_flags(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Derive SCIP role flag aggregates when available.

    Returns
    -------
    Plan | None
        Derived plan or ``None`` when unavailable.
    """
    occurrences = catalog.resolve(PlanRef("scip_occurrences"), ctx=ctx)
    if occurrences is None:
        return None
    schema = occurrences.schema(ctx=ctx)
    available = set(schema.names)
    if "symbol" not in available or "symbol_roles" not in available:
        return None

    flag_exprs: list[ComputeExpression] = []
    flag_names: list[str] = []
    for name, mask, _ in ROLE_FLAG_SPECS:
        hit = bitmask_is_set_expr(pc.field("symbol_roles"), mask=mask)
        flag_exprs.append(ensure_expression(pc.cast(hit, pa.int32(), safe=False)))
        flag_names.append(name)

    project_exprs = [
        ensure_expression(pc.cast(pc.field("symbol"), pa.string(), safe=False)),
        *flag_exprs,
    ]
    project_names = ["symbol", *flag_names]
    projected = occurrences.project(project_exprs, project_names, ctx=ctx)
    aggregated = projected.aggregate(
        group_keys=("symbol",),
        aggs=[(name, "max") for name in flag_names],
        ctx=ctx,
    )
    rename_exprs = [pc.field("symbol")] + [pc.field(f"{name}_max") for name in flag_names]
    rename_names = ["symbol", *flag_names]
    return aggregated.project(rename_exprs, rename_names, ctx=ctx)


__all__ = [
    "PlanCatalog",
    "PlanDeriver",
    "PlanGetter",
    "PlanRef",
    "PlanSource",
    "TableCatalog",
    "TableDeriver",
    "TableGetter",
    "TableRef",
    "derive_cst_defs_norm",
    "derive_scip_role_flags",
]
