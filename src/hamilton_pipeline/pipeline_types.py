"""Shared configuration and bundle types for the Hamilton pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pyarrow as pa

from relspec.compiler import CompiledOutput
from relspec.registry import ContractCatalog, DatasetLocation, RelationshipRegistry


@dataclass(frozen=True)
class RepoScanConfig:
    """Configuration for repository scanning."""

    repo_root: str
    include_globs: tuple[str, ...]
    exclude_globs: tuple[str, ...]
    max_files: int


@dataclass(frozen=True)
class RelspecConfig:
    """Configuration for relationship-spec execution."""

    relspec_mode: Literal["memory", "filesystem"]
    scip_index_path: str | None


@dataclass(frozen=True)
class OutputConfig:
    """Configuration for output and intermediate materialization."""

    work_dir: str | None
    output_dir: str | None
    overwrite_intermediate_datasets: bool


@dataclass(frozen=True)
class CstRelspecInputs:
    """CST inputs required for relationship-spec datasets."""

    cst_name_refs: pa.Table
    cst_imports_norm: pa.Table
    cst_callsites: pa.Table


@dataclass(frozen=True)
class QnameInputs:
    """Qualified-name inputs used by relationship rules."""

    callsite_qname_candidates: pa.Table
    dim_qualified_names: pa.Table


@dataclass(frozen=True)
class ScipOccurrenceInputs:
    """SCIP occurrence inputs for relationship rules."""

    scip_occurrences_norm: pa.Table


@dataclass(frozen=True)
class CstBuildInputs:
    """CST inputs required for CPG node/property building."""

    cst_name_refs: pa.Table
    cst_imports_norm: pa.Table
    cst_callsites: pa.Table
    cst_defs_norm: pa.Table


@dataclass(frozen=True)
class ScipBuildInputs:
    """SCIP inputs required for CPG node/property building."""

    scip_symbol_information: pa.Table
    scip_occurrences_norm: pa.Table


@dataclass(frozen=True)
class CpgBaseInputs:
    """Shared inputs for CPG nodes and properties."""

    repo_files: pa.Table
    dim_qualified_names: pa.Table
    cst_build_inputs: CstBuildInputs
    scip_build_inputs: ScipBuildInputs


@dataclass(frozen=True)
class RelationshipOutputTables:
    """Relationship output tables used across pipeline stages."""

    rel_name_symbol: pa.Table
    rel_import_symbol: pa.Table
    rel_callsite_symbol: pa.Table
    rel_callsite_qname: pa.Table

    def as_dict(self) -> dict[str, pa.Table]:
        """Return the relationship outputs as a name->table mapping.

        Returns
        -------
        dict[str, pa.Table]
            Mapping of relationship output names to tables.
        """
        return {
            "rel_name_symbol": self.rel_name_symbol,
            "rel_import_symbol": self.rel_import_symbol,
            "rel_callsite_symbol": self.rel_callsite_symbol,
            "rel_callsite_qname": self.rel_callsite_qname,
        }


@dataclass(frozen=True)
class CpgOutputTables:
    """CPG output tables used across pipeline stages."""

    cpg_nodes: pa.Table
    cpg_edges: pa.Table
    cpg_props: pa.Table

    def as_dict(self) -> dict[str, pa.Table]:
        """Return the CPG outputs as a name->table mapping.

        Returns
        -------
        dict[str, pa.Table]
            Mapping of CPG output names to tables.
        """
        return {
            "cpg_nodes": self.cpg_nodes,
            "cpg_edges": self.cpg_edges,
            "cpg_props": self.cpg_props,
        }


@dataclass(frozen=True)
class RelspecInputsBundle:
    """Bundle of relationship input tables and optional locations."""

    tables: dict[str, pa.Table]
    locations: dict[str, DatasetLocation]


@dataclass(frozen=True)
class RelspecSnapshots:
    """Snapshots required to reproduce relationship outputs."""

    registry: RelationshipRegistry
    contracts: ContractCatalog
    compiled_outputs: dict[str, CompiledOutput]
