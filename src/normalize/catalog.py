"""Plan catalog helpers for normalize pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from arrowdsl.core.context import ExecutionContext
from arrowdsl.plan import catalog as plan_catalog
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.scan_io import PlanSource, plan_from_source
from normalize.registry_plans import plan_ref
from normalize.text_index import RepoTextIndex

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


TYPE_EXPRS_NORM_REF = plan_ref("type_exprs_norm")
TYPES_NORM_REF = plan_ref("types_norm")
CFG_BLOCKS_NORM_REF = plan_ref("cfg_blocks_norm")
CFG_EDGES_NORM_REF = plan_ref("cfg_edges_norm")
DEF_USE_EVENTS_REF = plan_ref("def_use_events")
REACHES_REF = plan_ref("reaches")
DIAGNOSTICS_NORM_REF = plan_ref("diagnostics_norm")


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
