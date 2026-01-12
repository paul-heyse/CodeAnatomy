"""Shared configuration and bundle types for the Hamilton pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from arrowdsl.core.interop import TableLike
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
class TreeSitterConfig:
    """Configuration for tree-sitter extraction."""

    enable_tree_sitter: bool


@dataclass(frozen=True)
class ScipIndexConfig:
    """Configuration for scip-python indexing inside the pipeline."""

    enabled: bool = True
    index_path_override: str | None = None
    output_dir: str = "build/scip"
    env_json_path: str | None = None
    scip_python_bin: str = "scip-python"
    target_only: str | None = None
    node_max_old_space_mb: int | None = 8192
    timeout_s: int | None = None


@dataclass(frozen=True)
class RuntimeInspectConfig:
    """Configuration for runtime inspection extraction."""

    enable_runtime_inspect: bool
    module_allowlist: tuple[str, ...]
    timeout_s: int


@dataclass(frozen=True)
class CstRelspecInputs:
    """CST inputs required for relationship-spec datasets."""

    cst_name_refs: TableLike
    cst_imports_norm: TableLike
    cst_callsites: TableLike


@dataclass(frozen=True)
class QnameInputs:
    """Qualified-name inputs used by relationship rules."""

    callsite_qname_candidates: TableLike
    dim_qualified_names: TableLike


@dataclass(frozen=True)
class ScipOccurrenceInputs:
    """SCIP occurrence inputs for relationship rules."""

    scip_occurrences_norm: TableLike


@dataclass(frozen=True)
class ScipIdentityOverrides:
    """Optional overrides for SCIP project identity."""

    project_name_override: str | None
    project_version_override: str | None
    project_namespace_override: str | None


@dataclass(frozen=True)
class CstBuildInputs:
    """CST inputs required for CPG node/property building."""

    cst_name_refs: TableLike
    cst_imports_norm: TableLike
    cst_callsites: TableLike
    cst_defs_norm: TableLike


@dataclass(frozen=True)
class ScipBuildInputs:
    """SCIP inputs required for CPG node/property building."""

    scip_symbol_information: TableLike
    scip_occurrences_norm: TableLike
    scip_symbol_relationships: TableLike
    scip_external_symbol_information: TableLike


@dataclass(frozen=True)
class CpgBaseInputs:
    """Shared inputs for CPG nodes and properties."""

    repo_files: TableLike
    dim_qualified_names: TableLike
    cst_build_inputs: CstBuildInputs
    scip_build_inputs: ScipBuildInputs


@dataclass(frozen=True)
class TreeSitterInputs:
    """Tree-sitter tables used in CPG build steps."""

    ts_nodes: TableLike
    ts_errors: TableLike
    ts_missing: TableLike


@dataclass(frozen=True)
class TypeInputs:
    """Type tables used in CPG build steps."""

    type_exprs_norm: TableLike
    types_norm: TableLike


@dataclass(frozen=True)
class DiagnosticsInputs:
    """Diagnostics tables used in CPG build steps."""

    diagnostics_norm: TableLike


@dataclass(frozen=True)
class RuntimeInputs:
    """Runtime inspection tables used in CPG build steps."""

    rt_objects: TableLike
    rt_signatures: TableLike
    rt_signature_params: TableLike
    rt_members: TableLike


@dataclass(frozen=True)
class CpgExtraInputs:
    """Optional inputs for CPG nodes/props/edges."""

    ts_nodes: TableLike
    ts_errors: TableLike
    ts_missing: TableLike
    type_exprs_norm: TableLike
    types_norm: TableLike
    diagnostics_norm: TableLike
    rt_objects: TableLike
    rt_signatures: TableLike
    rt_signature_params: TableLike
    rt_members: TableLike


@dataclass(frozen=True)
class RelationshipOutputTables:
    """Relationship output tables used across pipeline stages."""

    rel_name_symbol: TableLike
    rel_import_symbol: TableLike
    rel_callsite_symbol: TableLike
    rel_callsite_qname: TableLike

    def as_dict(self) -> dict[str, TableLike]:
        """Return the relationship outputs as a name->table mapping.

        Returns
        -------
        dict[str, TableLike]
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

    cpg_nodes: TableLike
    cpg_edges: TableLike
    cpg_props: TableLike

    def as_dict(self) -> dict[str, TableLike]:
        """Return the CPG outputs as a name->table mapping.

        Returns
        -------
        dict[str, TableLike]
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

    tables: dict[str, TableLike]
    locations: dict[str, DatasetLocation]


@dataclass(frozen=True)
class RelspecSnapshots:
    """Snapshots required to reproduce relationship outputs."""

    registry: RelationshipRegistry
    contracts: ContractCatalog
    compiled_outputs: dict[str, CompiledOutput]
