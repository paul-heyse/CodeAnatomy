"""Plan registry for normalize catalog derivations."""

from __future__ import annotations

from dataclasses import dataclass

from arrowdsl.core.context import ExecutionContext
from arrowdsl.plan.catalog import PlanCatalog, PlanDeriver, PlanRef
from normalize.rule_factories import build_rule_definitions_from_specs
from normalize.rule_registry_specs import rule_family_specs
from normalize.runner import compile_normalize_plans
from normalize.utils import PlanSource


@dataclass(frozen=True)
class PlanRow:
    """Row specification for a normalize plan reference."""

    name: str
    inputs: tuple[str, ...] = ()
    derive: PlanDeriver | None = None


def _derive_output(output: str) -> PlanDeriver:
    def _derive(catalog: PlanCatalog, ctx: ExecutionContext) -> PlanSource | None:
        plans = compile_normalize_plans(catalog, ctx=ctx, required_outputs=(output,))
        return plans.get(output)

    return _derive


def derive_type_exprs_norm(
    catalog: PlanCatalog,
    ctx: ExecutionContext,
) -> PlanSource | None:
    """Derive a normalized type expression plan.

    Returns
    -------
    PlanSource | None
        Derived plan source or ``None`` when inputs are missing.
    """
    return _derive_output("type_exprs_norm_v1")(catalog, ctx)


def derive_types_norm(
    catalog: PlanCatalog,
    ctx: ExecutionContext,
) -> PlanSource | None:
    """Derive a normalized type nodes plan.

    Returns
    -------
    PlanSource | None
        Derived plan source or ``None`` when inputs are missing.
    """
    return _derive_output("type_nodes_v1")(catalog, ctx)


def derive_cfg_blocks_norm(
    catalog: PlanCatalog,
    ctx: ExecutionContext,
) -> PlanSource | None:
    """Derive normalized CFG blocks.

    Returns
    -------
    PlanSource | None
        Derived plan source or ``None`` when inputs are missing.
    """
    return _derive_output("py_bc_blocks_norm_v1")(catalog, ctx)


def derive_cfg_edges_norm(
    catalog: PlanCatalog,
    ctx: ExecutionContext,
) -> PlanSource | None:
    """Derive normalized CFG edges.

    Returns
    -------
    PlanSource | None
        Derived plan source or ``None`` when inputs are missing.
    """
    return _derive_output("py_bc_cfg_edges_norm_v1")(catalog, ctx)


def derive_def_use_events(
    catalog: PlanCatalog,
    ctx: ExecutionContext,
) -> PlanSource | None:
    """Derive def/use events from bytecode instructions.

    Returns
    -------
    PlanSource | None
        Derived plan source or ``None`` when inputs are missing.
    """
    return _derive_output("py_bc_def_use_events_v1")(catalog, ctx)


def derive_reaches(
    catalog: PlanCatalog,
    ctx: ExecutionContext,
) -> PlanSource | None:
    """Derive reaching-def edges from def/use events.

    Returns
    -------
    PlanSource | None
        Derived plan source or ``None`` when inputs are missing.
    """
    return _derive_output("py_bc_reaches_v1")(catalog, ctx)


def derive_diagnostics_norm(
    catalog: PlanCatalog,
    ctx: ExecutionContext,
) -> PlanSource | None:
    """Derive normalized diagnostics from extraction sources.

    Returns
    -------
    PlanSource | None
        Derived plan source or ``None`` when inputs are missing.
    """
    return _derive_output("diagnostics_norm_v1")(catalog, ctx)


def derive_span_errors(
    catalog: PlanCatalog,
    ctx: ExecutionContext,
) -> PlanSource | None:
    """Derive span error rows from provided span error sources.

    Returns
    -------
    PlanSource | None
        Derived plan source or ``None`` when inputs are missing.
    """
    return _derive_output("span_errors_v1")(catalog, ctx)


PLAN_ROWS: tuple[PlanRow, ...] = tuple(
    PlanRow(name=rule.output, inputs=rule.inputs, derive=_derive_output(rule.output))
    for rule in build_rule_definitions_from_specs(rule_family_specs())
)

_PLAN_REFS: dict[str, PlanRef] = {
    row.name: PlanRef(row.name, derive=row.derive) for row in PLAN_ROWS
}


def plan_ref(name: str) -> PlanRef:
    """Return a plan reference by name.

    Returns
    -------
    PlanRef
        Plan reference for the name.
    """
    return _PLAN_REFS[name]


def plan_rows() -> tuple[PlanRow, ...]:
    """Return the plan rows in registry order.

    Returns
    -------
    tuple[PlanRow, ...]
        Plan rows.
    """
    return PLAN_ROWS


def plan_names() -> tuple[str, ...]:
    """Return the plan names in registry order.

    Returns
    -------
    tuple[str, ...]
        Plan names.
    """
    return tuple(row.name for row in PLAN_ROWS)


__all__ = [
    "PLAN_ROWS",
    "PlanRow",
    "derive_cfg_blocks_norm",
    "derive_cfg_edges_norm",
    "derive_def_use_events",
    "derive_diagnostics_norm",
    "derive_reaches",
    "derive_span_errors",
    "derive_type_exprs_norm",
    "derive_types_norm",
    "plan_names",
    "plan_ref",
    "plan_rows",
]
