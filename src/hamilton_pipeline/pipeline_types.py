"""Shared configuration and bundle types for the Hamilton pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from arrowdsl.core.interop import TableLike
    from datafusion_engine.nested_tables import ViewReference
    from engine.plan_policy import WriterStrategy
    from ibis_engine.sources import DatasetSource
    from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy
    from storage.ipc import IpcWriteConfig


@dataclass(frozen=True)
class ScipIndexSettings:
    """Settings for scip-python indexing."""

    enabled: bool = True
    index_path_override: str | None = None
    output_dir: str = "build/scip"
    env_json_path: str | None = None
    generate_env_json: bool = False
    scip_python_bin: str = "scip-python"
    scip_cli_bin: str = "scip"
    target_only: str | None = None
    node_max_old_space_mb: int | None = 8192
    timeout_s: int | None = None
    extra_args: tuple[str, ...] = ()
    use_incremental_shards: bool = False
    shards_dir: str | None = None
    shards_manifest_path: str | None = None
    run_scip_print: bool = False
    scip_print_path: str | None = None
    run_scip_snapshot: bool = False
    scip_snapshot_dir: str | None = None
    scip_snapshot_comment_syntax: str = "#"
    run_scip_test: bool = False
    scip_test_args: tuple[str, ...] = ("--check-documents",)


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


OutputStorageFormat = Literal["delta"]


@dataclass(frozen=True)
class OutputStoragePolicy:
    """Configuration for output storage format enforcement."""

    format: OutputStorageFormat = "delta"
    allow_parquet_exports: bool = False


@dataclass(frozen=True)
class OutputConfig:
    """Configuration for output and intermediate materialization."""

    work_dir: str | None
    output_dir: str | None
    overwrite_intermediate_datasets: bool
    materialize_param_tables: bool = False
    writer_strategy: WriterStrategy = "arrow"
    ipc_dump_enabled: bool = False
    ipc_write_config: IpcWriteConfig | None = None
    output_storage_policy: OutputStoragePolicy = field(default_factory=OutputStoragePolicy)
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    delta_storage_options: Mapping[str, str] | None = None


@dataclass(frozen=True)
class IncrementalRunConfig:
    """Incremental run configuration snapshot for manifests."""

    enabled: bool
    state_dir: str | None
    repo_id: str | None


@dataclass(frozen=True)
class IncrementalDatasetUpdates:
    """Bundle incremental dataset update paths."""

    extract_updates: Mapping[str, str] | None
    normalize_updates: Mapping[str, str] | None
    module_index_updates: Mapping[str, str] | None
    imports_resolved_updates: Mapping[str, str] | None
    exported_defs_updates: Mapping[str, str] | None


@dataclass(frozen=True)
class IncrementalImpactUpdates:
    """Bundle incremental impact update paths."""

    impacted_callers_updates: Mapping[str, str] | None
    impacted_importers_updates: Mapping[str, str] | None
    impacted_files_updates: Mapping[str, str] | None


@dataclass(frozen=True)
class TaskDependencyReport:
    """Task-level dependency report for parameter tables."""

    task_name: str
    param_tables: tuple[str, ...]
    dataset_tables: tuple[str, ...] = ()


@dataclass(frozen=True)
class ActiveParamSet:
    """Active param-table logical names inferred for a run."""

    active: frozenset[str]


@dataclass(frozen=True)
class TreeSitterConfig:
    """Configuration for tree-sitter extraction."""

    enable_tree_sitter: bool


ScipIndexConfig = ScipIndexSettings


@dataclass(frozen=True)
class ScipIndexInputs:
    """Bundle inputs for SCIP indexing and identity resolution."""

    repo_root: str
    scip_identity_overrides: ScipIdentityOverrides
    scip_index_config: ScipIndexConfig


@dataclass(frozen=True)
class RuntimeInspectConfig:
    """Configuration for runtime inspection extraction."""

    enable_runtime_inspect: bool
    module_allowlist: tuple[str, ...]
    timeout_s: int


@dataclass(frozen=True)
class CstRelspecInputs:
    """CST inputs required for relationship-spec datasets."""

    cst_refs: TableLike | ViewReference
    cst_imports_norm: TableLike | ViewReference
    cst_callsites: TableLike | ViewReference
    cst_defs_norm: TableLike | ViewReference


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

    cst_refs: TableLike | DatasetSource | ViewReference
    cst_imports_norm: TableLike | DatasetSource | ViewReference
    cst_callsites: TableLike | DatasetSource | ViewReference
    cst_defs_norm: TableLike | DatasetSource | ViewReference


@dataclass(frozen=True)
class ScipBuildInputs:
    """SCIP inputs required for CPG node/property building."""

    scip_symbol_information: TableLike | DatasetSource | ViewReference
    scip_occurrences_norm: TableLike | DatasetSource | ViewReference
    scip_symbol_relationships: TableLike | DatasetSource | ViewReference
    scip_external_symbol_information: TableLike | DatasetSource | ViewReference


@dataclass(frozen=True)
class SymtableBuildInputs:
    """Symtable inputs required for CPG node/property building."""

    symtable_scopes: TableLike | DatasetSource | ViewReference
    symtable_symbols: TableLike | DatasetSource | ViewReference
    symtable_scope_edges: TableLike | DatasetSource | ViewReference
    symtable_bindings: TableLike | DatasetSource | ViewReference
    symtable_def_sites: TableLike | DatasetSource | ViewReference
    symtable_use_sites: TableLike | DatasetSource | ViewReference
    symtable_type_params: TableLike | DatasetSource | ViewReference
    symtable_type_param_edges: TableLike | DatasetSource | ViewReference


@dataclass(frozen=True)
class CpgBaseInputs:
    """Shared inputs for CPG nodes and properties."""

    repo_files: TableLike | DatasetSource | ViewReference
    dim_qualified_names: TableLike | DatasetSource | ViewReference
    cst_build_inputs: CstBuildInputs
    scip_build_inputs: ScipBuildInputs
    symtable_build_inputs: SymtableBuildInputs


@dataclass(frozen=True)
class TreeSitterInputs:
    """Tree-sitter tables used in CPG build steps."""

    ts_nodes: TableLike | DatasetSource | ViewReference
    ts_errors: TableLike | DatasetSource | ViewReference
    ts_missing: TableLike | DatasetSource | ViewReference


@dataclass(frozen=True)
class TypeInputs:
    """Type tables used in CPG build steps."""

    type_exprs_norm: TableLike | DatasetSource | ViewReference
    types_norm: TableLike | DatasetSource | ViewReference


@dataclass(frozen=True)
class DiagnosticsInputs:
    """Diagnostics tables used in CPG build steps."""

    diagnostics_norm: TableLike | DatasetSource | ViewReference


@dataclass(frozen=True)
class RuntimeInputs:
    """Runtime inspection tables used in CPG build steps."""

    rt_objects: TableLike | DatasetSource | ViewReference
    rt_signatures: TableLike | DatasetSource | ViewReference
    rt_signature_params: TableLike | DatasetSource | ViewReference
    rt_members: TableLike | DatasetSource | ViewReference


@dataclass(frozen=True)
class CpgExtraInputs:
    """Optional inputs for CPG nodes/props/edges."""

    ts_nodes: TableLike | DatasetSource | ViewReference
    ts_errors: TableLike | DatasetSource | ViewReference
    ts_missing: TableLike | DatasetSource | ViewReference
    type_exprs_norm: TableLike | DatasetSource | ViewReference
    types_norm: TableLike | DatasetSource | ViewReference
    diagnostics_norm: TableLike | DatasetSource | ViewReference
    rt_objects: TableLike | DatasetSource | ViewReference
    rt_signatures: TableLike | DatasetSource | ViewReference
    rt_signature_params: TableLike | DatasetSource | ViewReference
    rt_members: TableLike | DatasetSource | ViewReference


@dataclass(frozen=True)
class RelationshipOutputTables:
    """Relationship output tables used across pipeline stages."""

    rel_name_symbol: TableLike
    rel_import_symbol: TableLike
    rel_callsite_symbol: TableLike
    rel_callsite_qname: TableLike
    rel_def_symbol: TableLike

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
            "rel_def_symbol": self.rel_def_symbol,
        }


@dataclass(frozen=True)
class CpgOutputTables:
    """CPG output tables used across pipeline stages."""

    cpg_nodes: TableLike
    cpg_edges: TableLike
    cpg_props: TableLike
    cpg_props_json: TableLike | None = None

    def as_dict(self) -> dict[str, TableLike]:
        """Return the CPG outputs as a name->table mapping.

        Returns
        -------
        dict[str, TableLike]
            Mapping of CPG output names to tables.
        """
        tables = {
            "cpg_nodes": self.cpg_nodes,
            "cpg_edges": self.cpg_edges,
            "cpg_props": self.cpg_props,
        }
        if self.cpg_props_json is not None:
            tables["cpg_props_json"] = self.cpg_props_json
        return tables


@dataclass(frozen=True)
class ParamBundle:
    """Runtime parameter values grouped for scalar and list bindings."""

    scalar: Mapping[str, object] = field(default_factory=dict)
    lists: Mapping[str, tuple[object, ...]] = field(default_factory=dict)

    def list_values(self, name: str) -> tuple[object, ...]:
        """Return list values for a param name.

        Returns
        -------
        tuple[object, ...]
            List parameter values, or an empty tuple when missing.
        """
        values = self.lists.get(name)
        if values is None:
            return ()
        return tuple(values)
