"""Plan catalog helpers for normalize pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.plan import catalog as plan_catalog
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.source import PlanSource, plan_from_source
from normalize.bytecode_cfg import cfg_blocks_plan, cfg_edges_plan
from normalize.bytecode_dfg import def_use_events_plan, reaching_defs_plan
from normalize.diagnostics import DiagnosticsSources, diagnostics_plan
from normalize.text_index import RepoTextIndex
from normalize.types import type_exprs_plan, type_nodes_plan

PlanCatalog = plan_catalog.PlanCatalog
PlanDeriver = plan_catalog.PlanDeriver
PlanGetter = plan_catalog.PlanGetter
PlanRef = plan_catalog.PlanRef
TableCatalog = plan_catalog.TableCatalog
TableDeriver = plan_catalog.TableDeriver
TableGetter = plan_catalog.TableGetter
TableRef = plan_catalog.TableRef


@dataclass(frozen=True)
class NormalizeCatalogInputs:
    """Source tables for normalize-derived plan catalogs."""

    cst_type_exprs: PlanSource | None = None
    scip_symbol_information: PlanSource | None = None
    cst_parse_errors: PlanSource | None = None
    ts_errors: PlanSource | None = None
    ts_missing: PlanSource | None = None
    scip_diagnostics: PlanSource | None = None
    scip_documents: PlanSource | None = None
    py_bc_blocks: PlanSource | None = None
    py_bc_cfg_edges: PlanSource | None = None
    py_bc_code_units: PlanSource | None = None
    py_bc_instructions: PlanSource | None = None
    repo_text_index: RepoTextIndex | None = None


@dataclass
class NormalizePlanCatalog(plan_catalog.PlanCatalog):
    """Plan catalog that avoids caching derived entries."""

    repo_text_index: RepoTextIndex | None = None

    def resolve(self, ref: PlanRef, *, ctx: ExecutionContext) -> Plan | None:
        """Resolve a plan without caching derived results.

        Returns
        -------
        Plan | None
            Resolved plan or ``None`` when unavailable.
        """
        value = self.tables.get(ref.name)
        if value is not None:
            return plan_from_source(value, ctx=ctx, label=ref.name)
        if ref.derive is None:
            return None
        derived = ref.derive(self, ctx)
        if derived is None:
            return None
        return plan_from_source(derived, ctx=ctx, label=ref.name)


def _table_from_source(source: PlanSource | None, *, ctx: ExecutionContext) -> TableLike | None:
    if source is None:
        return None
    if isinstance(source, TableLike):
        return source
    if isinstance(source, Plan):
        return source.to_table(ctx=ctx)
    return plan_from_source(source, ctx=ctx, label="normalize_catalog").to_table(ctx=ctx)


def derive_type_exprs_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    source = catalog.tables.get("cst_type_exprs")
    if source is None:
        return None
    return type_exprs_plan(source, ctx=ctx)


def derive_types_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    type_exprs_norm = catalog.resolve(TYPE_EXPRS_NORM_REF, ctx=ctx)
    if type_exprs_norm is None:
        return None
    scip_info = catalog.tables.get("scip_symbol_information")
    return type_nodes_plan(type_exprs_norm, scip_info, ctx=ctx)


def derive_cfg_blocks_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    blocks = catalog.tables.get("py_bc_blocks")
    code_units = catalog.tables.get("py_bc_code_units")
    if blocks is None or code_units is None:
        return None
    return cfg_blocks_plan(blocks, code_units, ctx=ctx)


def derive_cfg_edges_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    edges = catalog.tables.get("py_bc_cfg_edges")
    code_units = catalog.tables.get("py_bc_code_units")
    if edges is None or code_units is None:
        return None
    return cfg_edges_plan(code_units, edges, ctx=ctx)


def derive_def_use_events(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    instructions = catalog.tables.get("py_bc_instructions")
    if instructions is None:
        return None
    return def_use_events_plan(instructions, ctx=ctx)


def derive_reaches(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    def_use = catalog.resolve(DEF_USE_EVENTS_REF, ctx=ctx)
    if def_use is None:
        return None
    return reaching_defs_plan(def_use, ctx=ctx)


def derive_diagnostics_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    normalize_catalog = cast("NormalizePlanCatalog", catalog)
    repo_text_index = normalize_catalog.repo_text_index
    if repo_text_index is None:
        return None
    sources = DiagnosticsSources(
        cst_parse_errors=_table_from_source(
            normalize_catalog.tables.get("cst_parse_errors"),
            ctx=ctx,
        ),
        ts_errors=_table_from_source(normalize_catalog.tables.get("ts_errors"), ctx=ctx),
        ts_missing=_table_from_source(normalize_catalog.tables.get("ts_missing"), ctx=ctx),
        scip_diagnostics=_table_from_source(
            normalize_catalog.tables.get("scip_diagnostics"),
            ctx=ctx,
        ),
        scip_documents=_table_from_source(
            normalize_catalog.tables.get("scip_documents"),
            ctx=ctx,
        ),
    )
    return diagnostics_plan(repo_text_index, sources=sources, ctx=ctx)


TYPE_EXPRS_NORM_REF = PlanRef("type_exprs_norm", derive=derive_type_exprs_norm)
TYPES_NORM_REF = PlanRef("types_norm", derive=derive_types_norm)
CFG_BLOCKS_NORM_REF = PlanRef("cfg_blocks_norm", derive=derive_cfg_blocks_norm)
CFG_EDGES_NORM_REF = PlanRef("cfg_edges_norm", derive=derive_cfg_edges_norm)
DEF_USE_EVENTS_REF = PlanRef("def_use_events", derive=derive_def_use_events)
REACHES_REF = PlanRef("reaches", derive=derive_reaches)
DIAGNOSTICS_NORM_REF = PlanRef("diagnostics_norm", derive=derive_diagnostics_norm)


def normalize_plan_catalog(inputs: NormalizeCatalogInputs) -> NormalizePlanCatalog:
    """Build a normalize plan catalog from source inputs.

    Returns
    -------
    NormalizePlanCatalog
        Catalog with base sources wired for derived plans.
    """
    tables = {name: value for name, value in _input_entries(inputs).items() if value is not None}
    return NormalizePlanCatalog(tables=tables, repo_text_index=inputs.repo_text_index)


def _input_entries(inputs: NormalizeCatalogInputs) -> Mapping[str, PlanSource | None]:
    return {
        "cst_type_exprs": inputs.cst_type_exprs,
        "scip_symbol_information": inputs.scip_symbol_information,
        "cst_parse_errors": inputs.cst_parse_errors,
        "ts_errors": inputs.ts_errors,
        "ts_missing": inputs.ts_missing,
        "scip_diagnostics": inputs.scip_diagnostics,
        "scip_documents": inputs.scip_documents,
        "py_bc_blocks": inputs.py_bc_blocks,
        "py_bc_cfg_edges": inputs.py_bc_cfg_edges,
        "py_bc_code_units": inputs.py_bc_code_units,
        "py_bc_instructions": inputs.py_bc_instructions,
    }


__all__ = [
    "CFG_BLOCKS_NORM_REF",
    "CFG_EDGES_NORM_REF",
    "DEF_USE_EVENTS_REF",
    "DIAGNOSTICS_NORM_REF",
    "REACHES_REF",
    "TYPES_NORM_REF",
    "TYPE_EXPRS_NORM_REF",
    "NormalizeCatalogInputs",
    "NormalizePlanCatalog",
    "PlanCatalog",
    "PlanDeriver",
    "PlanGetter",
    "PlanRef",
    "TableCatalog",
    "TableDeriver",
    "TableGetter",
    "TableRef",
    "normalize_plan_catalog",
]
