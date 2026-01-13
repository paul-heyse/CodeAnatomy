"""Plan builder registry for normalize rules."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.catalog import PlanCatalog, PlanDeriver
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import run_plan
from arrowdsl.plan.scan_io import PlanSource, plan_from_source
from normalize.bytecode_cfg_plans import cfg_blocks_plan, cfg_edges_plan
from normalize.bytecode_dfg_plans import def_use_events_plan, reaching_defs_plan
from normalize.diagnostics_plans import DiagnosticsSources, diagnostics_plan
from normalize.text_index import RepoTextIndex
from normalize.types_plans import type_exprs_plan, type_nodes_plan


def type_exprs_builder(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Build a plan for normalized type expressions.

    Returns
    -------
    Plan | None
        Plan for type expressions, or None when inputs are missing.
    """
    source = catalog.tables.get("cst_type_exprs")
    if source is None:
        return None
    return type_exprs_plan(source, ctx=ctx)


def type_nodes_builder(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Build a plan for normalized type nodes.

    Returns
    -------
    Plan | None
        Plan for type nodes, or None when inputs are missing.
    """
    type_exprs_norm = catalog.tables.get("type_exprs_norm_v1")
    if type_exprs_norm is None:
        return None
    scip_info = catalog.tables.get("scip_symbol_information")
    return type_nodes_plan(type_exprs_norm, scip_info, ctx=ctx)


def cfg_blocks_builder(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Build a plan for normalized bytecode CFG blocks.

    Returns
    -------
    Plan | None
        Plan for CFG blocks, or None when inputs are missing.
    """
    blocks = catalog.tables.get("py_bc_blocks")
    code_units = catalog.tables.get("py_bc_code_units")
    if blocks is None or code_units is None:
        return None
    return cfg_blocks_plan(blocks, code_units, ctx=ctx)


def cfg_edges_builder(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Build a plan for normalized bytecode CFG edges.

    Returns
    -------
    Plan | None
        Plan for CFG edges, or None when inputs are missing.
    """
    edges = catalog.tables.get("py_bc_cfg_edges")
    code_units = catalog.tables.get("py_bc_code_units")
    if edges is None or code_units is None:
        return None
    return cfg_edges_plan(code_units, edges, ctx=ctx)


def def_use_events_builder(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Build a plan for bytecode def/use events.

    Returns
    -------
    Plan | None
        Plan for def/use events, or None when inputs are missing.
    """
    instructions = catalog.tables.get("py_bc_instructions")
    if instructions is None:
        return None
    return def_use_events_plan(instructions, ctx=ctx)


def reaching_defs_builder(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Build a plan for reaching-def edges.

    Returns
    -------
    Plan | None
        Plan for reaching-def edges, or None when inputs are missing.
    """
    def_use = catalog.tables.get("py_bc_def_use_events_v1")
    if def_use is None:
        return None
    return reaching_defs_plan(def_use, ctx=ctx)


def diagnostics_builder(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Build a plan for normalized diagnostics.

    Returns
    -------
    Plan | None
        Plan for diagnostics, or None when inputs are missing.
    """
    repo_text_index = getattr(catalog, "repo_text_index", None)
    if not isinstance(repo_text_index, RepoTextIndex):
        return None
    sources = DiagnosticsSources(
        cst_parse_errors=_table_from_source(catalog.tables.get("cst_parse_errors"), ctx=ctx),
        ts_errors=_table_from_source(catalog.tables.get("ts_errors"), ctx=ctx),
        ts_missing=_table_from_source(catalog.tables.get("ts_missing"), ctx=ctx),
        scip_diagnostics=_table_from_source(catalog.tables.get("scip_diagnostics"), ctx=ctx),
        scip_documents=_table_from_source(catalog.tables.get("scip_documents"), ctx=ctx),
    )
    return diagnostics_plan(repo_text_index, sources=sources, ctx=ctx)


def span_errors_builder(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Build a plan for span error rows.

    Returns
    -------
    Plan | None
        Plan for span errors, or None when inputs are missing.
    """
    source = catalog.tables.get("span_errors_v1")
    if source is None:
        return None
    return plan_from_source(source, ctx=ctx, label="span_errors")


def resolve_plan_builder(name: str) -> PlanDeriver:
    """Return a plan builder by name.

    Returns
    -------
    PlanDeriver
        Plan builder function.

    Raises
    ------
    KeyError
        Raised when the plan builder name is unknown.
    """
    builder = PLAN_BUILDERS.get(name)
    if builder is None:
        msg = f"Unknown normalize plan builder: {name!r}."
        raise KeyError(msg)
    return builder


def plan_builders() -> Mapping[str, PlanDeriver]:
    """Return the registered plan builders.

    Returns
    -------
    Mapping[str, PlanDeriver]
        Mapping of builder names to plan-deriver callables.
    """
    return PLAN_BUILDERS


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
    plan = plan_from_source(source, ctx=ctx, label="normalize_rule_source")
    result = run_plan(
        plan,
        ctx=ctx,
        prefer_reader=False,
        attach_ordering_metadata=True,
    )
    return cast("TableLike", result.value)


PLAN_BUILDERS: dict[str, PlanDeriver] = {
    "type_exprs": type_exprs_builder,
    "type_nodes": type_nodes_builder,
    "cfg_blocks": cfg_blocks_builder,
    "cfg_edges": cfg_edges_builder,
    "def_use_events": def_use_events_builder,
    "reaching_defs": reaching_defs_builder,
    "diagnostics": diagnostics_builder,
    "span_errors": span_errors_builder,
}


__all__ = [
    "PLAN_BUILDERS",
    "cfg_blocks_builder",
    "cfg_edges_builder",
    "def_use_events_builder",
    "diagnostics_builder",
    "plan_builders",
    "reaching_defs_builder",
    "resolve_plan_builder",
    "span_errors_builder",
    "type_exprs_builder",
    "type_nodes_builder",
]
