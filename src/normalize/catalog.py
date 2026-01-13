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
    span_errors: PlanSource | None = None
    repo_text_index: RepoTextIndex | None = None

    def as_tables(self) -> Mapping[str, PlanSource | None]:
        """Return catalog tables keyed by dataset name.

        Returns
        -------
        Mapping[str, PlanSource | None]
            Catalog tables keyed by dataset name.
        """
        return {
            "cst_type_exprs": self.cst_type_exprs,
            "scip_symbol_information": self.scip_symbol_information,
            "cst_parse_errors": self.cst_parse_errors,
            "ts_errors": self.ts_errors,
            "ts_missing": self.ts_missing,
            "scip_diagnostics": self.scip_diagnostics,
            "scip_documents": self.scip_documents,
            "py_bc_blocks": self.py_bc_blocks,
            "py_bc_cfg_edges": self.py_bc_cfg_edges,
            "py_bc_code_units": self.py_bc_code_units,
            "py_bc_instructions": self.py_bc_instructions,
            "span_errors_v1": self.span_errors,
        }


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


TYPE_EXPRS_NORM_REF = plan_ref("type_exprs_norm_v1")
TYPES_NORM_REF = plan_ref("type_nodes_v1")
CFG_BLOCKS_NORM_REF = plan_ref("py_bc_blocks_norm_v1")
CFG_EDGES_NORM_REF = plan_ref("py_bc_cfg_edges_norm_v1")
DEF_USE_EVENTS_REF = plan_ref("py_bc_def_use_events_v1")
REACHES_REF = plan_ref("py_bc_reaches_v1")
DIAGNOSTICS_NORM_REF = plan_ref("diagnostics_norm_v1")
SPAN_ERRORS_REF = plan_ref("span_errors_v1")


def normalize_plan_catalog(inputs: NormalizeCatalogInputs) -> NormalizePlanCatalog:
    """Build a normalize plan catalog from source inputs.

    Returns
    -------
    NormalizePlanCatalog
        Catalog with base sources wired for derived plans.
    """
    tables = {name: value for name, value in inputs.as_tables().items() if value is not None}
    return NormalizePlanCatalog(tables=tables, repo_text_index=inputs.repo_text_index)


__all__ = [
    "CFG_BLOCKS_NORM_REF",
    "CFG_EDGES_NORM_REF",
    "DEF_USE_EVENTS_REF",
    "DIAGNOSTICS_NORM_REF",
    "REACHES_REF",
    "SPAN_ERRORS_REF",
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
