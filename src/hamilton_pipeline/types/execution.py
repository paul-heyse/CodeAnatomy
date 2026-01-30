"""Execution and pipeline bundle types for the Hamilton pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from core_types import JsonValue
    from datafusion_engine.arrow.interop import TableLike

type SimpleViewRef = str


ExecutorKind = Literal["threadpool", "multiprocessing", "dask", "ray"]
GraphAdapterKind = Literal["threadpool", "dask", "ray"]


class ExecutionMode(StrEnum):
    """Execution mode for Hamilton orchestration."""

    DETERMINISTIC_SERIAL = "deterministic_serial"
    PLAN_PARALLEL = "plan_parallel"
    PLAN_PARALLEL_REMOTE = "plan_parallel_remote"


@dataclass(frozen=True)
class ExecutorConfig:
    """Executor configuration for plan-aware scheduling."""

    kind: ExecutorKind = "multiprocessing"
    max_tasks: int = 4
    remote_kind: ExecutorKind | None = None
    remote_max_tasks: int | None = None
    cost_threshold: float | None = None
    ray_init_config: Mapping[str, JsonValue] | None = None
    dask_scheduler: str | None = None
    dask_client_kwargs: Mapping[str, JsonValue] | None = None


@dataclass(frozen=True)
class GraphAdapterConfig:
    """Graph adapter configuration for non-dynamic execution backends."""

    kind: GraphAdapterKind
    options: Mapping[str, JsonValue] | None = None


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


@dataclass(frozen=True)
class CstRelspecInputs:
    """CST inputs required for relationship-spec datasets."""

    cst_refs: TableLike | SimpleViewRef
    cst_imports_norm: TableLike | SimpleViewRef
    cst_callsites: TableLike | SimpleViewRef
    cst_defs_norm: TableLike | SimpleViewRef


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
class CstBuildInputs:
    """CST inputs required for CPG node/property building."""

    cst_refs: TableLike | SimpleViewRef
    cst_imports_norm: TableLike | SimpleViewRef
    cst_callsites: TableLike | SimpleViewRef
    cst_defs_norm: TableLike | SimpleViewRef


@dataclass(frozen=True)
class ScipBuildInputs:
    """SCIP inputs required for CPG node/property building."""

    scip_symbol_information: TableLike | SimpleViewRef
    scip_occurrences_norm: TableLike | SimpleViewRef
    scip_symbol_relationships: TableLike | SimpleViewRef
    scip_external_symbol_information: TableLike | SimpleViewRef


@dataclass(frozen=True)
class SymtableBuildInputs:
    """Symtable inputs required for CPG node/property building."""

    symtable_scopes: TableLike | SimpleViewRef
    symtable_symbols: TableLike | SimpleViewRef
    symtable_scope_edges: TableLike | SimpleViewRef
    symtable_bindings: TableLike | SimpleViewRef
    symtable_def_sites: TableLike | SimpleViewRef
    symtable_use_sites: TableLike | SimpleViewRef
    symtable_type_params: TableLike | SimpleViewRef
    symtable_type_param_edges: TableLike | SimpleViewRef


@dataclass(frozen=True)
class CpgBaseInputs:
    """Shared inputs for CPG nodes and properties."""

    repo_files: TableLike | SimpleViewRef
    dim_qualified_names: TableLike | SimpleViewRef
    cst_build_inputs: CstBuildInputs
    scip_build_inputs: ScipBuildInputs
    symtable_build_inputs: SymtableBuildInputs


@dataclass(frozen=True)
class TreeSitterInputs:
    """Tree-sitter tables used in CPG build steps."""

    ts_nodes: TableLike | SimpleViewRef
    ts_errors: TableLike | SimpleViewRef
    ts_missing: TableLike | SimpleViewRef


@dataclass(frozen=True)
class TypeInputs:
    """Type tables used in CPG build steps."""

    type_exprs_norm: TableLike | SimpleViewRef
    types_norm: TableLike | SimpleViewRef


@dataclass(frozen=True)
class DiagnosticsInputs:
    """Diagnostics tables used in CPG build steps."""

    diagnostics_norm: TableLike | SimpleViewRef


@dataclass(frozen=True)
class CpgExtraInputs:
    """Optional inputs for CPG nodes/props/edges."""

    ts_nodes: TableLike | SimpleViewRef
    ts_errors: TableLike | SimpleViewRef
    ts_missing: TableLike | SimpleViewRef
    type_exprs_norm: TableLike | SimpleViewRef
    types_norm: TableLike | SimpleViewRef
    diagnostics_norm: TableLike | SimpleViewRef


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


__all__ = [
    "ActiveParamSet",
    "CpgBaseInputs",
    "CpgExtraInputs",
    "CpgOutputTables",
    "CstBuildInputs",
    "CstRelspecInputs",
    "DiagnosticsInputs",
    "ExecutionMode",
    "ExecutorConfig",
    "ExecutorKind",
    "GraphAdapterConfig",
    "GraphAdapterKind",
    "ParamBundle",
    "QnameInputs",
    "RelationshipOutputTables",
    "ScipBuildInputs",
    "ScipOccurrenceInputs",
    "SymtableBuildInputs",
    "TaskDependencyReport",
    "TreeSitterInputs",
    "TypeInputs",
]
