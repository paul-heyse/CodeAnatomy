"""Plan registry for normalize catalog derivations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.catalog import PlanCatalog, PlanDeriver, PlanRef
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import run_plan
from arrowdsl.plan.scan_io import PlanSource, plan_from_source
from normalize.bytecode_cfg import cfg_blocks_plan, cfg_edges_plan
from normalize.bytecode_dfg import def_use_events_plan, reaching_defs_plan
from normalize.diagnostics import DiagnosticsSources, diagnostics_plan
from normalize.text_index import RepoTextIndex
from normalize.types import type_exprs_plan, type_nodes_plan


@dataclass(frozen=True)
class PlanRow:
    """Row specification for a normalize plan reference."""

    name: str
    inputs: tuple[str, ...] = ()
    derive: PlanDeriver | None = None


def _table_from_source(source: PlanSource | None, *, ctx: ExecutionContext) -> TableLike | None:
    if source is None:
        return None
    if isinstance(source, TableLike):
        return source
    if isinstance(source, Plan):
        result = run_plan(
            source,
            ctx=ctx,
            prefer_reader=False,
            attach_ordering_metadata=True,
        )
        return cast("TableLike", result.value)
    plan = plan_from_source(source, ctx=ctx, label="normalize_catalog")
    result = run_plan(
        plan,
        ctx=ctx,
        prefer_reader=False,
        attach_ordering_metadata=True,
    )
    return cast("TableLike", result.value)


def derive_type_exprs_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Derive a normalized type expression plan.

    Returns
    -------
    Plan | None
        Derived plan or ``None`` when inputs are missing.
    """
    source = catalog.tables.get("cst_type_exprs")
    if source is None:
        return None
    return type_exprs_plan(source, ctx=ctx)


def derive_types_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Derive a normalized type nodes plan.

    Returns
    -------
    Plan | None
        Derived plan or ``None`` when inputs are missing.
    """
    type_exprs_norm = catalog.resolve(plan_ref("type_exprs_norm"), ctx=ctx)
    if type_exprs_norm is None:
        return None
    scip_info = catalog.tables.get("scip_symbol_information")
    return type_nodes_plan(type_exprs_norm, scip_info, ctx=ctx)


def derive_cfg_blocks_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Derive normalized CFG blocks.

    Returns
    -------
    Plan | None
        Derived plan or ``None`` when inputs are missing.
    """
    blocks = catalog.tables.get("py_bc_blocks")
    code_units = catalog.tables.get("py_bc_code_units")
    if blocks is None or code_units is None:
        return None
    return cfg_blocks_plan(blocks, code_units, ctx=ctx)


def derive_cfg_edges_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Derive normalized CFG edges.

    Returns
    -------
    Plan | None
        Derived plan or ``None`` when inputs are missing.
    """
    edges = catalog.tables.get("py_bc_cfg_edges")
    code_units = catalog.tables.get("py_bc_code_units")
    if edges is None or code_units is None:
        return None
    return cfg_edges_plan(code_units, edges, ctx=ctx)


def derive_def_use_events(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Derive def/use events from bytecode instructions.

    Returns
    -------
    Plan | None
        Derived plan or ``None`` when inputs are missing.
    """
    instructions = catalog.tables.get("py_bc_instructions")
    if instructions is None:
        return None
    return def_use_events_plan(instructions, ctx=ctx)


def derive_reaches(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Derive reaching-def edges from def/use events.

    Returns
    -------
    Plan | None
        Derived plan or ``None`` when inputs are missing.
    """
    def_use = catalog.resolve(plan_ref("def_use_events"), ctx=ctx)
    if def_use is None:
        return None
    return reaching_defs_plan(def_use, ctx=ctx)


def derive_diagnostics_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Derive normalized diagnostics from extraction sources.

    Returns
    -------
    Plan | None
        Derived plan or ``None`` when inputs are missing.
    """
    repo_text_index = getattr(catalog, "repo_text_index", None)
    if not isinstance(repo_text_index, RepoTextIndex):
        return None
    sources = DiagnosticsSources(
        cst_parse_errors=_table_from_source(
            catalog.tables.get("cst_parse_errors"),
            ctx=ctx,
        ),
        ts_errors=_table_from_source(catalog.tables.get("ts_errors"), ctx=ctx),
        ts_missing=_table_from_source(catalog.tables.get("ts_missing"), ctx=ctx),
        scip_diagnostics=_table_from_source(
            catalog.tables.get("scip_diagnostics"),
            ctx=ctx,
        ),
        scip_documents=_table_from_source(
            catalog.tables.get("scip_documents"),
            ctx=ctx,
        ),
    )
    return diagnostics_plan(repo_text_index, sources=sources, ctx=ctx)


PLAN_ROWS: tuple[PlanRow, ...] = (
    PlanRow(
        name="type_exprs_norm",
        inputs=("cst_type_exprs",),
        derive=derive_type_exprs_norm,
    ),
    PlanRow(
        name="types_norm",
        inputs=("type_exprs_norm", "scip_symbol_information"),
        derive=derive_types_norm,
    ),
    PlanRow(
        name="cfg_blocks_norm",
        inputs=("py_bc_blocks", "py_bc_code_units"),
        derive=derive_cfg_blocks_norm,
    ),
    PlanRow(
        name="cfg_edges_norm",
        inputs=("py_bc_cfg_edges", "py_bc_code_units"),
        derive=derive_cfg_edges_norm,
    ),
    PlanRow(
        name="def_use_events",
        inputs=("py_bc_instructions",),
        derive=derive_def_use_events,
    ),
    PlanRow(
        name="reaches",
        inputs=("def_use_events",),
        derive=derive_reaches,
    ),
    PlanRow(
        name="diagnostics_norm",
        inputs=(
            "cst_parse_errors",
            "ts_errors",
            "ts_missing",
            "scip_diagnostics",
            "scip_documents",
        ),
        derive=derive_diagnostics_norm,
    ),
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
    "derive_type_exprs_norm",
    "derive_types_norm",
    "plan_names",
    "plan_ref",
    "plan_rows",
]
