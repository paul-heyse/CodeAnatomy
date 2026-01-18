"""Catalog helpers for Ibis-backed normalize pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from ibis.backends import BaseBackend

from normalize.ibis_plan_builders import IbisPlanCatalog, IbisPlanSource
from normalize.text_index import RepoTextIndex


@dataclass(frozen=True)
class NormalizeCatalogInputs:
    """Source tables for normalize-derived Ibis catalogs."""

    cst_type_exprs: IbisPlanSource | None = None
    scip_symbol_information: IbisPlanSource | None = None
    file_line_index: IbisPlanSource | None = None
    cst_parse_errors: IbisPlanSource | None = None
    ts_errors: IbisPlanSource | None = None
    ts_missing: IbisPlanSource | None = None
    scip_diagnostics: IbisPlanSource | None = None
    scip_documents: IbisPlanSource | None = None
    py_bc_blocks: IbisPlanSource | None = None
    py_bc_cfg_edges: IbisPlanSource | None = None
    py_bc_code_units: IbisPlanSource | None = None
    py_bc_instructions: IbisPlanSource | None = None
    span_errors: IbisPlanSource | None = None
    repo_text_index: RepoTextIndex | None = None

    def as_tables(self) -> Mapping[str, IbisPlanSource | None]:
        """Return catalog tables keyed by dataset name.

        Returns
        -------
        Mapping[str, IbisPlanSource | None]
            Catalog tables keyed by dataset name.
        """
        return {
            "cst_type_exprs": self.cst_type_exprs,
            "scip_symbol_information": self.scip_symbol_information,
            "file_line_index": self.file_line_index,
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


def normalize_plan_catalog(
    inputs: NormalizeCatalogInputs,
    *,
    backend: BaseBackend,
) -> IbisPlanCatalog:
    """Build a normalize Ibis catalog from source inputs.

    Returns
    -------
    IbisPlanCatalog
        Catalog with base sources wired for derived plans.
    """
    tables = {name: value for name, value in inputs.as_tables().items() if value is not None}
    return IbisPlanCatalog(
        backend=backend,
        tables=tables,
        repo_text_index=inputs.repo_text_index,
    )


__all__ = [
    "IbisPlanCatalog",
    "IbisPlanSource",
    "NormalizeCatalogInputs",
    "normalize_plan_catalog",
]
