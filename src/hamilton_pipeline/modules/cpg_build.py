"""Hamilton CPG build stage and relationship rule wiring."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
import rustworkx as rx
from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from hamilton.function_modifiers import cache, extract_fields, tag
from ibis.backends import BaseBackend
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import (
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    coerce_table_like,
)
from arrowdsl.core.scan_telemetry import ScanTelemetry
from arrowdsl.schema.metadata import ordering_from_schema
from arrowdsl.schema.schema import align_table, empty_table
from cpg.constants import CpgBuildArtifacts
from datafusion_engine.extract_ids import repo_file_id_spec
from datafusion_engine.nested_tables import (
    ViewReference,
    materialize_view_reference,
    register_nested_table,
)
from datafusion_engine.runtime import (
    AdapterExecutionPolicy,
    DataFusionRuntimeProfile,
    dataset_spec_from_context,
    record_view_definition,
)
from datafusion_engine.sql_options import sql_options_for_profile
from datafusion_engine.symtable_views import (
    symtable_binding_resolutions_df,
    symtable_bindings_df,
    symtable_def_sites_df,
    symtable_type_param_edges_df,
    symtable_type_params_df,
    symtable_use_sites_df,
)
from engine.plan_policy import ExecutionSurfacePolicy
from engine.session import EngineSession
from hamilton_pipeline.pipeline_types import (
    CpgBaseInputs,
    CpgExtraInputs,
    CstBuildInputs,
    CstRelspecInputs,
    DiagnosticsInputs,
    IncrementalDatasetUpdates,
    IncrementalImpactUpdates,
    OutputConfig,
    ParamBundle,
    QnameInputs,
    RelationshipOutputTables,
    RuntimeInputs,
    ScipBuildInputs,
    ScipOccurrenceInputs,
    SymtableBuildInputs,
    TreeSitterInputs,
    TypeInputs,
)
from ibis_engine.hashing import HashExprSpec
from ibis_engine.io_bridge import (
    IbisDeltaWriteOptions,
    IbisNamedDatasetWriteOptions,
    write_ibis_named_datasets_delta,
)
from ibis_engine.param_tables import (
    ParamTablePolicy,
    ParamTableRegistry,
    ParamTableSpec,
    param_table_name,
)
from ibis_engine.params_bridge import IbisParamRegistry, registry_from_specs, specs_from_rel_ops
from ibis_engine.plan import IbisPlan
from ibis_engine.registry import datafusion_context
from ibis_engine.sources import DatasetSource
from incremental.publish import (
    publish_cpg_edges,
    publish_cpg_nodes,
    publish_cpg_props,
)
from incremental.relspec_update import (
    RelspecStateReadOptions,
    impacted_file_ids,
    relspec_inputs_from_state,
    scoped_relspec_resolver,
)
from incremental.runtime import IncrementalRuntime
from incremental.state_store import StateStore
from incremental.types import IncrementalConfig, IncrementalFileChanges, IncrementalImpact
from normalize.utils import encoding_policy_from_schema
from relspec.compiler import (
    CompiledOutput,
    CompiledOutputExecutionOptions,
    FilesystemPlanResolver,
    InMemoryPlanResolver,
    PlanResolver,
    RelationshipRuleCompiler,
    apply_policy_defaults,
    validate_policy_requirements,
)
from relspec.config import RelspecConfig
from relspec.contracts import relation_output_contract, relation_output_schema
from relspec.cpg.build_edges import EdgeBuildConfig, EdgeBuildInputs, build_cpg_edges
from relspec.cpg.build_nodes import NodeBuildConfig, NodeInputTables, build_cpg_nodes
from relspec.cpg.build_props import (
    PropsBuildConfig,
    PropsBuildOptions,
    PropsInputTables,
    build_cpg_props,
)
from relspec.edge_contract_validator import (
    EdgeContractValidationConfig,
    validate_relationship_output_contracts_for_edge_kinds,
)
from relspec.errors import RelspecValidationError
from relspec.model import DatasetRef, RelationshipRule
from relspec.param_deps import RuleDependencyReport
from relspec.pipeline_policy import PipelinePolicy
from relspec.policies import PolicyRegistry, evidence_spec_from_schema
from relspec.registry import ContractCatalog, DatasetLocation
from relspec.registry.rules import RuleRegistry
from relspec.rules.compiler import RuleCompiler
from relspec.rules.evidence import EvidenceCatalog
from relspec.rules.exec_events import RuleExecutionEventCollector
from relspec.rules.handlers import default_rule_handlers
from relspec.rules.validation import SqlGlotDiagnosticsConfig, rule_dependency_reports
from relspec.rustworkx_graph import (
    EvidenceNode,
    RuleGraph,
    RuleNode,
    build_rule_graph_from_relationship_rules,
)
from relspec.rustworkx_schedule import RuleSchedule, schedule_rules
from relspec.runtime import RelspecRuntime, RelspecRuntimeOptions, compose_relspec
from relspec.schema_context import RelspecSchemaContext
from relspec.validate import validate_registry
from schema_spec.relationship_specs import (
    relationship_contract_spec as build_relationship_contract_spec,
)
from schema_spec.system import (
    ContractCatalogSpec,
    DataFusionScanOptions,
    DatasetSpec,
)
from sqlglot_tools.bridge import IbisCompilerBackend
from storage.deltalake import coerce_delta_table

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable

    from ibis_engine.execution import IbisExecutionContext

# -----------------------------
# Relationship contracts
# -----------------------------

_HOT_DATAFUSION_INPUTS = frozenset(
    {
        "callsite_qname_candidates",
        "dim_qualified_names",
        "file_line_index",
        "scip_occurrences",
        "type_exprs_norm_v1",
        "type_nodes_v1",
    }
)


@dataclass(frozen=True)
class RelspecResolverContext:
    """Bundled inputs for relationship resolver selection."""

    ctx: ExecutionContext
    param_table_registry: ParamTableRegistry | None


def relspec_resolver_context(
    ctx: ExecutionContext,
    param_table_registry: ParamTableRegistry | None,
) -> RelspecResolverContext:
    """Build resolver context for relationship rule compilation.

    Returns
    -------
    RelspecResolverContext
        Bundled resolver context values.
    """
    return RelspecResolverContext(ctx=ctx, param_table_registry=param_table_registry)


@dataclass(frozen=True)
class RelationshipExecutionContext:
    """Execution settings for relationship table materialization."""

    ctx: ExecutionContext
    execution_policy: AdapterExecutionPolicy
    ibis_backend: BaseBackend
    relspec_param_bindings: Mapping[IbisValue, object]
    surface_policy: ExecutionSurfacePolicy


@dataclass(frozen=True)
class RelationshipTableInputs:
    """Inputs required to build relationship tables."""

    compiled_relationship_outputs: dict[str, CompiledOutput]
    relspec_rule_graph: RuleGraph
    relspec_rule_schedule: RuleSchedule
    relspec_evidence_catalog: EvidenceCatalog
    relspec_resolver: PlanResolver[IbisPlan]
    relationship_contracts: ContractCatalog
    engine_session: EngineSession
    incremental_config: IncrementalConfig


@dataclass(frozen=True)
class RelationshipExecutionInputs:
    """Execution inputs for building relationship execution contexts."""

    execution_policy: AdapterExecutionPolicy
    ibis_backend: BaseBackend
    relspec_param_bindings: Mapping[IbisValue, object]
    surface_policy: ExecutionSurfacePolicy


@dataclass(frozen=True)
class RelationshipCompilationInputs:
    """Inputs required to compile relationship outputs."""

    rule_registry: RuleRegistry
    relspec_input_datasets: dict[str, TableLike]
    relspec_resolver: PlanResolver[IbisPlan]
    relationship_contracts: ContractCatalog
    pipeline_policy: PipelinePolicy
    ctx: ExecutionContext


@dataclass(frozen=True)
class RelspecInputBundles:
    """Bundle relationship input groups for relspec compilation."""

    cst_inputs: CstRelspecInputs
    scip_inputs: ScipOccurrenceInputs
    qname_inputs: QnameInputs


@dataclass(frozen=True)
class RelspecIncrementalContext:
    """Incremental state required for relationship datasets."""

    state_store: StateStore | None
    file_changes: IncrementalFileChanges
    impact: IncrementalImpact
    config: IncrementalConfig
    ctx: ExecutionContext
    runtime: IncrementalRuntime | None


@dataclass(frozen=True)
class RelspecIncrementalInputs:
    """Inputs required to assemble the relspec incremental context."""

    file_changes: IncrementalFileChanges
    impact: IncrementalImpact
    config: IncrementalConfig
    ctx: ExecutionContext
    runtime: IncrementalRuntime | None


@dataclass(frozen=True)
class RelspecInputDatasetContext:
    """Inputs required to assemble relspec input datasets."""

    engine_session: EngineSession
    incremental_context: RelspecIncrementalContext
    incremental_gate: object | None


@dataclass(frozen=True)
class RelspecResolverSources:
    """Resolver inputs for relationship plans."""

    mode: Literal["memory", "filesystem"]
    input_datasets: dict[str, TableLike]
    persisted_inputs: dict[str, DatasetLocation]


@tag(layer="relspec", artifact="relationship_execution_context", kind="object")
def relationship_execution_context(
    engine_session: EngineSession,
    relationship_execution_inputs: RelationshipExecutionInputs,
) -> RelationshipExecutionContext:
    """Bundle execution settings for relationship table materialization.

    Returns
    -------
    RelationshipExecutionContext
        Execution settings for relationship output execution.
    """
    return RelationshipExecutionContext(
        ctx=engine_session.ctx,
        execution_policy=relationship_execution_inputs.execution_policy,
        ibis_backend=relationship_execution_inputs.ibis_backend,
        relspec_param_bindings=relationship_execution_inputs.relspec_param_bindings,
        surface_policy=relationship_execution_inputs.surface_policy,
    )


@tag(layer="relspec", artifact="relationship_execution_inputs", kind="object")
def relationship_execution_inputs(
    adapter_execution_policy: AdapterExecutionPolicy,
    engine_session: EngineSession,
    relspec_param_bindings: Mapping[IbisValue, object],
) -> RelationshipExecutionInputs:
    """Bundle relationship execution inputs for materialization.

    Returns
    -------
    RelationshipExecutionInputs
        Bundled execution input values.
    """
    return RelationshipExecutionInputs(
        execution_policy=adapter_execution_policy,
        ibis_backend=engine_session.ibis_backend,
        relspec_param_bindings=relspec_param_bindings,
        surface_policy=engine_session.surface_policy,
    )


def _datafusion_scan_options(
    name: str,
    *,
    schema: SchemaLike | None,
    dataset_spec: DatasetSpec | None,
) -> DataFusionScanOptions:
    if dataset_spec is not None and dataset_spec.datafusion_scan is not None:
        return dataset_spec.datafusion_scan
    file_sort_order: tuple[str, ...] = ()
    if dataset_spec is not None:
        ordering = dataset_spec.ordering()
        if ordering.keys:
            file_sort_order = tuple(key[0] for key in ordering.keys)
    elif schema is not None:
        ordering = ordering_from_schema(schema)
        if ordering.keys:
            file_sort_order = tuple(key[0] for key in ordering.keys)
    return DataFusionScanOptions(
        file_extension=".parquet",
        parquet_pruning=True,
        skip_metadata=True,
        cache=name in _HOT_DATAFUSION_INPUTS,
        file_sort_order=file_sort_order,
    )


@tag(layer="relspec", artifact="relationship_contract_spec", kind="spec")
def relationship_contract_spec() -> ContractCatalogSpec:
    """Build the contract spec catalog for relationship datasets.

    Returns
    -------
    ContractCatalogSpec
        Contract catalog specification for relationship outputs.
    """
    return build_relationship_contract_spec()


def _catalog_spec_for(name: str) -> DatasetSpec | None:
    try:
        return dataset_spec_from_context(name)
    except KeyError:
        return None


@tag(layer="relspec", artifact="relationship_contracts", kind="catalog")
def relationship_contracts(
    relationship_contract_spec: ContractCatalogSpec,
) -> ContractCatalog:
    """Build output contracts for relationship datasets.

    Returns
    -------
    ContractCatalog
        Contract catalog for relationship outputs.
    """
    catalog = ContractCatalog.from_spec(relationship_contract_spec)
    catalog.register(relation_output_contract())
    return catalog


@tag(layer="relspec", artifact="schema_context", kind="object")
def relspec_schema_context(engine_session: EngineSession) -> RelspecSchemaContext:
    """Build the DataFusion schema context for relspec validation.

    Returns
    -------
    RelspecSchemaContext
        Schema context bound to the engine session.
    """
    return RelspecSchemaContext.from_engine_session(engine_session)


# -----------------------------
# Relationship rules
# -----------------------------


@tag(layer="relspec", artifact="relspec_runtime", kind="object")
def relspec_runtime(
    param_table_specs: tuple[ParamTableSpec, ...],
    relspec_rule_config: RelspecConfig,
    engine_session: EngineSession,
    relspec_schema_context: RelspecSchemaContext,
    pipeline_policy: PipelinePolicy,
) -> RelspecRuntime:
    """Compose the relspec runtime bundle.

    Returns
    -------
    RelspecRuntime
        Runtime bundle with rule registry and compiler wiring.
    """
    runtime = compose_relspec(
        config=relspec_rule_config,
        options=RelspecRuntimeOptions(
            policies=pipeline_policy.policy_registry,
            param_table_specs=param_table_specs,
            engine_session=engine_session,
            schema_context=relspec_schema_context,
        ),
    )
    validate_registry(runtime.registry).raise_for_errors()
    return runtime


@tag(layer="relspec", artifact="rule_registry", kind="registry")
def rule_registry(
    relspec_runtime: RelspecRuntime,
) -> RuleRegistry:
    """Build the central rule registry.

    Returns
    -------
    RuleRegistry
        Centralized registry for CPG/normalize/extract rules.
    """
    return relspec_runtime.registry


@dataclass(frozen=True)
class RuleRegistryContext:
    """Bundled inputs for relationship rule compilation."""

    runtime: RelspecRuntime
    pipeline_policy: PipelinePolicy

    @property
    def rule_registry(self) -> RuleRegistry:
        """Return the rule registry from the runtime bundle."""
        return self.runtime.registry


@tag(layer="relspec", artifact="rule_registry_context", kind="object")
def rule_registry_context(
    relspec_runtime: RelspecRuntime,
    pipeline_policy: PipelinePolicy,
) -> RuleRegistryContext:
    """Bundle rule registry and pipeline policy for compilation.

    Returns
    -------
    RuleRegistryContext
        Bundled registry and policy inputs.
    """
    return RuleRegistryContext(runtime=relspec_runtime, pipeline_policy=pipeline_policy)


@tag(layer="relspec", artifact="relspec_param_registry", kind="object")
def relspec_param_registry(relspec_runtime: RelspecRuntime) -> IbisParamRegistry:
    """Build the relspec parameter registry from rule definitions.

    Returns
    -------
    IbisParamRegistry
        Registry populated with parameter specs from rule rel_ops.
    """
    specs = []
    for rule in relspec_runtime.registry.rule_definitions():
        specs.extend(specs_from_rel_ops(rule.rel_ops))
    return registry_from_specs(specs)


@tag(layer="relspec", artifact="relspec_param_dependency_reports", kind="object")
def relspec_param_dependency_reports(
    relspec_runtime: RelspecRuntime,
    engine_session: EngineSession,
    param_table_specs: tuple[ParamTableSpec, ...],
    param_table_policy: ParamTablePolicy,
) -> tuple[RuleDependencyReport, ...]:
    """Return inferred param dependency reports for relspec rules.

    Returns
    -------
    tuple[RuleDependencyReport, ...]
        Dependency reports for relspec rules.
    """
    rules = relspec_runtime.registry.rule_definitions()
    return rule_dependency_reports(
        rules,
        config=SqlGlotDiagnosticsConfig(
            backend=cast("IbisCompilerBackend", engine_session.ibis_backend),
            schema_context=RelspecSchemaContext.from_engine_session(engine_session),
            ctx=engine_session.ctx,
            param_table_specs=param_table_specs,
            param_table_policy=param_table_policy,
        ),
    )


@tag(layer="relspec", artifact="relspec_param_bindings", kind="object")
def relspec_param_bindings(
    relspec_param_registry: IbisParamRegistry,
    param_bundle: ParamBundle,
) -> Mapping[IbisValue, object]:
    """Return parameter bindings for relspec execution.

    Returns
    -------
    Mapping[IbisValue, object]
        Parameter bindings for Ibis plan execution.
    """
    return relspec_param_registry.bindings(param_bundle.scalar)


# -----------------------------
# Alternate resolver mode (memory vs filesystem)
# -----------------------------


@tag(layer="relspec", artifact="relspec_work_dir", kind="scalar")
def relspec_work_dir(work_dir: str | None, output_dir: str | None) -> str:
    """Choose a working directory for intermediate dataset materialization.

    Precedence:
      1) explicit work_dir
      2) output_dir
      3) local fallback: ./.codeintel_cpg_work

    Returns
    -------
    str
        Working directory path.
    """
    base = work_dir or output_dir or ".codeintel_cpg_work"
    base_path = Path(base)
    base_path.mkdir(exist_ok=True, parents=True)
    return str(base_path)


@tag(layer="relspec", artifact="relspec_input_dataset_dir", kind="scalar")
def relspec_input_dataset_dir(relspec_work_dir: str) -> str:
    """Return the directory for relationship-input datasets.

    Returns
    -------
    str
        Filesystem path for relationship-input datasets.
    """
    dataset_dir = Path(relspec_work_dir) / "relspec_inputs"
    dataset_dir.mkdir(exist_ok=True, parents=True)
    return str(dataset_dir)


def _register_view_reference(
    backend: BaseBackend | None,
    *,
    name: str,
    builder: Callable[[SessionContext], DataFrame],
    runtime_profile: DataFusionRuntimeProfile | None,
) -> ViewReference:
    """Register a view DataFrame builder and return its reference.

    Returns
    -------
    ViewReference
        Reference to the registered view.

    Raises
    ------
    TypeError
        Raised when the backend is missing or unsupported.
    """
    if backend is None:
        msg = f"View {name!r} requires an Ibis backend."
        raise TypeError(msg)
    ctx = datafusion_context(backend)
    view = builder(ctx).into_view(temporary=False)
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            deregister(name)
    ctx.register_table(name, view)
    if runtime_profile is not None:
        record_view_definition(runtime_profile, name=name, sql=None)
    return ViewReference(name)


# -----------------------------
# Nested table fragments
# -----------------------------


@cache()
@tag(layer="extract", artifact="cst_refs", kind="object")
def cst_refs(
    libcst_files: TableLike | RecordBatchReaderLike,
    engine_session: EngineSession,
) -> ViewReference:
    """Return the CST refs projection from nested LibCST files.

    Returns
    -------
    ViewReference
        View reference for CST name references.
    """
    register_nested_table(
        engine_session.ibis_backend,
        name="libcst_files_v1",
        table=libcst_files,
    )
    return ViewReference("cst_refs")


@cache()
@tag(layer="extract", artifact="cst_callsites", kind="object")
def cst_callsites(
    libcst_files: TableLike | RecordBatchReaderLike,
    engine_session: EngineSession,
) -> ViewReference:
    """Return the CST callsites projection from nested LibCST files.

    Returns
    -------
    ViewReference
        View reference for CST callsites.
    """
    register_nested_table(
        engine_session.ibis_backend,
        name="libcst_files_v1",
        table=libcst_files,
    )
    return ViewReference("cst_callsites")


@cache()
@tag(layer="extract", artifact="ts_nodes", kind="object")
def ts_nodes(
    tree_sitter_files: TableLike | RecordBatchReaderLike,
    engine_session: EngineSession,
) -> ViewReference:
    """Return tree-sitter node projection from nested tree-sitter files.

    Returns
    -------
    ViewReference
        View reference for tree-sitter nodes.
    """
    register_nested_table(
        engine_session.ibis_backend,
        name="tree_sitter_files_v1",
        table=tree_sitter_files,
    )
    return ViewReference("ts_nodes")


@cache()
@tag(layer="extract", artifact="ts_errors", kind="object")
def ts_errors(
    tree_sitter_files: TableLike | RecordBatchReaderLike,
    engine_session: EngineSession,
) -> ViewReference:
    """Return tree-sitter error projection from nested tree-sitter files.

    Returns
    -------
    ViewReference
        View reference for tree-sitter errors.
    """
    register_nested_table(
        engine_session.ibis_backend,
        name="tree_sitter_files_v1",
        table=tree_sitter_files,
    )
    return ViewReference("ts_errors")


@cache()
@tag(layer="extract", artifact="ts_missing", kind="object")
def ts_missing(
    tree_sitter_files: TableLike | RecordBatchReaderLike,
    engine_session: EngineSession,
) -> ViewReference:
    """Return tree-sitter missing projection from nested tree-sitter files.

    Returns
    -------
    ViewReference
        View reference for tree-sitter missing nodes.
    """
    register_nested_table(
        engine_session.ibis_backend,
        name="tree_sitter_files_v1",
        table=tree_sitter_files,
    )
    return ViewReference("ts_missing")


@cache()
@tag(layer="extract", artifact="symtable_scopes", kind="object")
def symtable_scopes(
    symtables: TableLike | RecordBatchReaderLike,
    engine_session: EngineSession,
) -> ViewReference:
    """Return symtable scope rows from nested symtable files.

    Returns
    -------
    ViewReference
        View reference for symtable scopes.
    """
    register_nested_table(
        engine_session.ibis_backend,
        name="symtable_files_v1",
        table=symtables,
    )
    return ViewReference("symtable_scopes")


@cache()
@tag(layer="extract", artifact="symtable_symbols", kind="object")
def symtable_symbols(
    symtables: TableLike | RecordBatchReaderLike,
    engine_session: EngineSession,
) -> ViewReference:
    """Return symtable symbol rows from nested symtable files.

    Returns
    -------
    ViewReference
        View reference for symtable symbols.
    """
    register_nested_table(
        engine_session.ibis_backend,
        name="symtable_files_v1",
        table=symtables,
    )
    return ViewReference("symtable_symbols")


@cache()
@tag(layer="extract", artifact="symtable_scope_edges", kind="object")
def symtable_scope_edges(
    symtables: TableLike | RecordBatchReaderLike,
    engine_session: EngineSession,
) -> ViewReference:
    """Return symtable scope edges from nested symtable files.

    Returns
    -------
    ViewReference
        View reference for symtable scope edges.
    """
    register_nested_table(
        engine_session.ibis_backend,
        name="symtable_files_v1",
        table=symtables,
    )
    return ViewReference("symtable_scope_edges")


@cache()
@tag(layer="extract", artifact="symtable_bindings", kind="object")
def symtable_bindings(
    symtables: TableLike | RecordBatchReaderLike,
    engine_session: EngineSession,
) -> ViewReference:
    """Return symtable binding rows from nested symtable files.

    Returns
    -------
    ViewReference
        View reference for symtable bindings.
    """
    register_nested_table(
        engine_session.ibis_backend,
        name="symtable_files_v1",
        table=symtables,
    )
    return _register_view_reference(
        engine_session.ibis_backend,
        name="symtable_bindings",
        builder=symtable_bindings_df,
        runtime_profile=engine_session.datafusion_profile,
    )


@cache()
@tag(layer="extract", artifact="symtable_def_sites", kind="object")
def symtable_def_sites(
    symtables: TableLike | RecordBatchReaderLike,
    libcst_files: TableLike | RecordBatchReaderLike,
    engine_session: EngineSession,
) -> ViewReference:
    """Return symtable definition sites from nested symtable files.

    Returns
    -------
    ViewReference
        View reference for symtable definition sites.
    """
    register_nested_table(
        engine_session.ibis_backend,
        name="symtable_files_v1",
        table=symtables,
    )
    register_nested_table(
        engine_session.ibis_backend,
        name="libcst_files_v1",
        table=libcst_files,
    )
    return _register_view_reference(
        engine_session.ibis_backend,
        name="symtable_def_sites",
        builder=symtable_def_sites_df,
        runtime_profile=engine_session.datafusion_profile,
    )


@cache()
@tag(layer="extract", artifact="symtable_use_sites", kind="object")
def symtable_use_sites(
    symtables: TableLike | RecordBatchReaderLike,
    libcst_files: TableLike | RecordBatchReaderLike,
    engine_session: EngineSession,
) -> ViewReference:
    """Return symtable use sites from nested symtable files.

    Returns
    -------
    ViewReference
        View reference for symtable use sites.
    """
    register_nested_table(
        engine_session.ibis_backend,
        name="symtable_files_v1",
        table=symtables,
    )
    register_nested_table(
        engine_session.ibis_backend,
        name="libcst_files_v1",
        table=libcst_files,
    )
    return _register_view_reference(
        engine_session.ibis_backend,
        name="symtable_use_sites",
        builder=symtable_use_sites_df,
        runtime_profile=engine_session.datafusion_profile,
    )


@cache()
@tag(layer="extract", artifact="symtable_type_params", kind="object")
def symtable_type_params(
    symtables: TableLike | RecordBatchReaderLike,
    engine_session: EngineSession,
) -> ViewReference:
    """Return symtable type parameters from nested symtable files.

    Returns
    -------
    ViewReference
        View reference for symtable type parameters.
    """
    register_nested_table(
        engine_session.ibis_backend,
        name="symtable_files_v1",
        table=symtables,
    )
    return _register_view_reference(
        engine_session.ibis_backend,
        name="symtable_type_params",
        builder=symtable_type_params_df,
        runtime_profile=engine_session.datafusion_profile,
    )


@cache()
@tag(layer="extract", artifact="symtable_type_param_edges", kind="object")
def symtable_type_param_edges(
    symtables: TableLike | RecordBatchReaderLike,
    engine_session: EngineSession,
) -> ViewReference:
    """Return symtable type parameter edges from nested symtable files.

    Returns
    -------
    ViewReference
        View reference for symtable type-parameter edges.
    """
    register_nested_table(
        engine_session.ibis_backend,
        name="symtable_files_v1",
        table=symtables,
    )
    return _register_view_reference(
        engine_session.ibis_backend,
        name="symtable_type_param_edges",
        builder=symtable_type_param_edges_df,
        runtime_profile=engine_session.datafusion_profile,
    )


@tag(layer="relspec", artifact="relspec_cst_inputs", kind="bundle")
def relspec_cst_inputs(
    cst_refs: TableLike | ViewReference,
    cst_imports_norm: TableLike | ViewReference,
    cst_callsites: TableLike | ViewReference,
    cst_defs_norm: TableLike | ViewReference,
) -> CstRelspecInputs:
    """Bundle CST tables needed for relationship inputs.

    Returns
    -------
    CstRelspecInputs
        CST relationship input bundle.
    """
    return CstRelspecInputs(
        cst_refs=cst_refs,
        cst_imports_norm=cst_imports_norm,
        cst_callsites=cst_callsites,
        cst_defs_norm=cst_defs_norm,
    )


@tag(layer="relspec", artifact="relspec_scip_inputs", kind="bundle")
def relspec_scip_inputs(scip_occurrences_norm: TableLike) -> ScipOccurrenceInputs:
    """Bundle SCIP occurrences for relationship inputs.

    Returns
    -------
    ScipOccurrenceInputs
        SCIP occurrence input bundle.
    """
    return ScipOccurrenceInputs(scip_occurrences_norm=scip_occurrences_norm)


@tag(layer="relspec", artifact="relspec_qname_inputs", kind="bundle")
def relspec_qname_inputs(
    callsite_qname_candidates: TableLike,
    dim_qualified_names: TableLike,
) -> QnameInputs:
    """Bundle qualified-name inputs for relationship rules.

    Returns
    -------
    QnameInputs
        Qualified-name input bundle.
    """
    return QnameInputs(
        callsite_qname_candidates=callsite_qname_candidates,
        dim_qualified_names=dim_qualified_names,
    )


@tag(layer="relspec", artifact="relspec_input_bundles", kind="bundle")
def relspec_input_bundles(
    relspec_cst_inputs: CstRelspecInputs,
    relspec_scip_inputs: ScipOccurrenceInputs,
    relspec_qname_inputs: QnameInputs,
) -> RelspecInputBundles:
    """Bundle relationship input groups for relspec compilation.

    Returns
    -------
    RelspecInputBundles
        Bundled relationship inputs.
    """
    return RelspecInputBundles(
        cst_inputs=relspec_cst_inputs,
        scip_inputs=relspec_scip_inputs,
        qname_inputs=relspec_qname_inputs,
    )


@tag(layer="incremental", artifact="relspec_incremental_inputs", kind="object")
def relspec_incremental_inputs(
    incremental_file_changes: IncrementalFileChanges,
    incremental_impact: IncrementalImpact,
    incremental_config: IncrementalConfig,
    incremental_runtime: IncrementalRuntime | None,
    ctx: ExecutionContext,
) -> RelspecIncrementalInputs:
    """Bundle incremental inputs for relationship datasets.

    Returns
    -------
    RelspecIncrementalInputs
        Incremental inputs for relspec dataset assembly.
    """
    return RelspecIncrementalInputs(
        file_changes=incremental_file_changes,
        impact=incremental_impact,
        config=incremental_config,
        ctx=ctx,
        runtime=incremental_runtime,
    )


@tag(layer="incremental", artifact="relspec_incremental_context", kind="object")
def relspec_incremental_context(
    incremental_state_store: StateStore | None,
    relspec_incremental_inputs: RelspecIncrementalInputs,
) -> RelspecIncrementalContext:
    """Bundle incremental context for relationship datasets.

    Returns
    -------
    RelspecIncrementalContext
        Incremental context for relspec inputs.
    """
    return RelspecIncrementalContext(
        state_store=incremental_state_store,
        file_changes=relspec_incremental_inputs.file_changes,
        impact=relspec_incremental_inputs.impact,
        config=relspec_incremental_inputs.config,
        ctx=relspec_incremental_inputs.ctx,
        runtime=relspec_incremental_inputs.runtime,
    )


@tag(layer="relspec", artifact="relspec_incremental_gate", kind="object")
def relspec_incremental_gate(
    incremental_dataset_updates: IncrementalDatasetUpdates,
    incremental_impact_updates: IncrementalImpactUpdates,
) -> None:
    """Ensure incremental extract/normalize updates complete before relspec inputs."""
    _ = incremental_dataset_updates
    _ = incremental_impact_updates


@tag(layer="relspec", artifact="relspec_input_dataset_context", kind="object")
def relspec_input_dataset_context(
    engine_session: EngineSession,
    relspec_incremental_context: RelspecIncrementalContext,
    relspec_incremental_gate: object | None,
) -> RelspecInputDatasetContext:
    """Bundle inputs for relspec dataset materialization.

    Returns
    -------
    RelspecInputDatasetContext
        Inputs for relspec dataset assembly.
    """
    return RelspecInputDatasetContext(
        engine_session=engine_session,
        incremental_context=relspec_incremental_context,
        incremental_gate=relspec_incremental_gate,
    )


def _require_table(
    name: str,
    value: TableLike | DatasetSource | ViewReference,
    *,
    backend: BaseBackend,
) -> TableLike:
    if isinstance(value, DatasetSource):
        msg = f"Relspec input {name!r} must be a table, not a dataset source."
        raise TypeError(msg)
    if isinstance(value, ViewReference):
        return materialize_view_reference(backend, value)
    return value


@tag(layer="relspec", artifact="relspec_input_datasets", kind="bundle")
def relspec_input_datasets(
    relspec_input_bundles: RelspecInputBundles,
    scip_build_inputs: ScipBuildInputs,
    cpg_extra_inputs: CpgExtraInputs,
    relspec_input_dataset_context: RelspecInputDatasetContext,
) -> dict[str, TableLike]:
    """Build the canonical dataset-name to table mapping.

    Important: dataset keys must match the DatasetRef(...) names in ``rule_registry()``.

    Returns
    -------
    dict[str, TableLike]
        Mapping from dataset names to tables.
    """
    _ = relspec_input_dataset_context.incremental_gate
    _ = relspec_input_dataset_context.incremental_context
    backend = relspec_input_dataset_context.engine_session.ibis_backend
    datasets = {
        "cst_refs": _require_table(
            "cst_refs",
            relspec_input_bundles.cst_inputs.cst_refs,
            backend=backend,
        ),
        "cst_imports": _require_table(
            "cst_imports",
            relspec_input_bundles.cst_inputs.cst_imports_norm,
            backend=backend,
        ),
        "cst_callsites": _require_table(
            "cst_callsites",
            relspec_input_bundles.cst_inputs.cst_callsites,
            backend=backend,
        ),
        "cst_defs": _require_table(
            "cst_defs",
            relspec_input_bundles.cst_inputs.cst_defs_norm,
            backend=backend,
        ),
        "scip_occurrences": _require_table(
            "scip_occurrences",
            relspec_input_bundles.scip_inputs.scip_occurrences_norm,
            backend=backend,
        ),
        "scip_symbol_relationships": _require_table(
            "scip_symbol_relationships",
            scip_build_inputs.scip_symbol_relationships,
            backend=backend,
        ),
        "callsite_qname_candidates": _require_table(
            "callsite_qname_candidates",
            relspec_input_bundles.qname_inputs.callsite_qname_candidates,
            backend=backend,
        ),
        "dim_qualified_names": _require_table(
            "dim_qualified_names",
            relspec_input_bundles.qname_inputs.dim_qualified_names,
            backend=backend,
        ),
        "type_exprs_norm": _require_table(
            "type_exprs_norm",
            cpg_extra_inputs.type_exprs_norm,
            backend=backend,
        ),
        "diagnostics_norm": _require_table(
            "diagnostics_norm",
            cpg_extra_inputs.diagnostics_norm,
            backend=backend,
        ),
        "rt_signatures": _require_table(
            "rt_signatures",
            cpg_extra_inputs.rt_signatures,
            backend=backend,
        ),
        "rt_signature_params": _require_table(
            "rt_signature_params",
            cpg_extra_inputs.rt_signature_params,
            backend=backend,
        ),
        "rt_members": _require_table(
            "rt_members",
            cpg_extra_inputs.rt_members,
            backend=backend,
        ),
    }
    if (
        not relspec_incremental_context.config.enabled
        or relspec_incremental_context.state_store is None
    ):
        return datasets
    file_ids = impacted_file_ids(relspec_incremental_context.impact)
    return relspec_inputs_from_state(
        datasets,
        state_root=relspec_incremental_context.state_store.root,
        ctx=relspec_incremental_context.ctx,
        file_ids=file_ids,
        options=RelspecStateReadOptions(runtime=relspec_incremental_context.runtime),
    )


@dataclass(frozen=True)
class RelspecPersistOptions:
    """Options for persisting relationship input datasets."""

    mode: Literal["memory", "filesystem"]
    input_dataset_dir: str
    overwrite_intermediate_datasets: bool
    output_config: OutputConfig


@tag(layer="relspec", artifact="relspec_persist_options", kind="object")
def relspec_persist_options(
    relspec_mode: Literal["memory", "filesystem"],
    relspec_input_dataset_dir: str,
    *,
    overwrite_intermediate_datasets: bool,
    output_config: OutputConfig,
) -> RelspecPersistOptions:
    """Bundle options for persisting relationship input datasets.

    Returns
    -------
    RelspecPersistOptions
        Persist options for relationship input datasets.
    """
    return RelspecPersistOptions(
        mode=relspec_mode,
        input_dataset_dir=relspec_input_dataset_dir,
        overwrite_intermediate_datasets=overwrite_intermediate_datasets,
        output_config=output_config,
    )


@cache()
@tag(layer="relspec", artifact="persisted_relspec_inputs", kind="object")
def persist_relspec_input_datasets(
    relspec_input_datasets: dict[str, TableLike],
    relspec_persist_options: RelspecPersistOptions,
    *,
    ibis_execution: IbisExecutionContext,
) -> dict[str, DatasetLocation]:
    """Write relationship input datasets to disk in filesystem mode.

    Returns mapping: dataset_name -> DatasetLocation for FilesystemPlanResolver.
    In memory mode, returns {} and performs no I/O.

    Returns
    -------
    dict[str, DatasetLocation]
        Dataset locations for persisted inputs.
    """
    mode = (relspec_persist_options.mode or "memory").lower().strip()
    if mode != "filesystem":
        return {}

    output_config = relspec_persist_options.output_config
    # Write as Delta tables for durable filesystem-backed resolvers.
    schemas = {name: table.schema for name, table in relspec_input_datasets.items()}
    dataset_specs = {name: _catalog_spec_for(name) for name in relspec_input_datasets}
    encoding_policies = {}
    for name, table in relspec_input_datasets.items():
        spec = dataset_specs.get(name)
        policy = spec.encoding_policy() if spec is not None else None
        encoding_policies[name] = policy or encoding_policy_from_schema(table.schema)
    delta_mode = "overwrite" if relspec_persist_options.overwrite_intermediate_datasets else "error"
    delta_options = IbisDeltaWriteOptions(mode=delta_mode, schema_mode="overwrite")
    converted = {
        name: coerce_delta_table(
            table,
            schema=schemas.get(name),
            encoding_policy=encoding_policies.get(name),
        )
        for name, table in relspec_input_datasets.items()
    }
    results = write_ibis_named_datasets_delta(
        converted,
        relspec_persist_options.input_dataset_dir,
        options=IbisNamedDatasetWriteOptions(
            execution=ibis_execution,
            writer_strategy=output_config.writer_strategy,
            delta_options=delta_options,
            delta_write_policy=output_config.delta_write_policy,
            delta_schema_policy=output_config.delta_schema_policy,
            storage_options=output_config.delta_storage_options,
        ),
    )

    out: dict[str, DatasetLocation] = {}
    for name, result in results.items():
        table = relspec_input_datasets.get(name)
        table_schema = table.schema if table is not None else None
        dataset_spec = _catalog_spec_for(name)
        out[name] = DatasetLocation(
            path=result.path,
            format="delta",
            partitioning="hive",
            filesystem=None,
            files=None,
            dataset_spec=dataset_spec,
            datafusion_scan=_datafusion_scan_options(
                name,
                schema=table_schema,
                dataset_spec=dataset_spec,
            ),
            datafusion_provider=None,
            delta_version=result.version,
        )
    return out


@tag(layer="relspec", artifact="relspec_resolver_sources", kind="object")
def relspec_resolver_sources(
    relspec_mode: Literal["memory", "filesystem"],
    relspec_input_datasets: dict[str, TableLike],
    persist_relspec_input_datasets: dict[str, DatasetLocation],
) -> RelspecResolverSources:
    """Bundle resolver inputs for relationship plans.

    Returns
    -------
    RelspecResolverSources
        Bundled resolver inputs.
    """
    return RelspecResolverSources(
        mode=relspec_mode,
        input_datasets=relspec_input_datasets,
        persisted_inputs=persist_relspec_input_datasets,
    )


@tag(layer="relspec", artifact="relspec_resolver", kind="runtime")
def relspec_resolver(
    relspec_resolver_sources: RelspecResolverSources,
    engine_session: EngineSession,
    relspec_resolver_context: RelspecResolverContext,
    relspec_incremental_context: RelspecIncrementalContext,
) -> PlanResolver[IbisPlan]:
    """Select the relationship resolver implementation.

    Resolver selection:
      - memory      => InMemoryPlanResolver (tables already in memory)
      - filesystem  => FilesystemPlanResolver (tables scanned from Parquet datasets)

    Returns
    -------
    PlanResolver[IbisPlan]
        Resolver instance for relationship rule compilation.
    """
    mode = (relspec_resolver_sources.mode or "memory").lower().strip()
    if relspec_resolver_context.param_table_registry is None:
        param_tables: Mapping[str, IbisTable] = {}
        policy = ParamTablePolicy()
    else:
        param_tables = relspec_resolver_context.param_table_registry.ibis_tables(
            engine_session.ibis_backend
        )
        policy = relspec_resolver_context.param_table_registry.policy
    param_aliases = _param_table_aliases(param_tables, policy=policy)
    resolver: PlanResolver[IbisPlan]
    if mode == "filesystem":
        cat = engine_session.datasets.catalog
        for name, loc in relspec_resolver_sources.persisted_inputs.items():
            cat.register(name, loc)
        base = FilesystemPlanResolver(
            cat,
            registry=engine_session.datasets,
        )
        if not param_aliases:
            resolver = base
        else:
            param_resolver = InMemoryPlanResolver(
                param_aliases, backend=engine_session.ibis_backend
            )
            resolver = _CompositePlanResolver(
                primary=param_resolver,
                fallback=base,
                primary_names=frozenset(param_aliases),
            )
    else:
        combined: dict[str, TableLike | IbisPlan | IbisTable] = dict(
            relspec_resolver_sources.input_datasets
        )
        for name, table in param_aliases.items():
            combined.setdefault(name, table)
        resolver = InMemoryPlanResolver(combined, backend=engine_session.ibis_backend)
    if relspec_incremental_context.config.enabled:
        file_ids = impacted_file_ids(relspec_incremental_context.impact)
        return scoped_relspec_resolver(resolver, file_ids=file_ids)
    return resolver


def _relationship_rule_schema(
    rule: RelationshipRule,
    *,
    contracts: ContractCatalog | None,
) -> SchemaLike | None:
    if rule.contract_name is None:
        return None
    if contracts is not None:
        contract = contracts.get(rule.contract_name)
        dataset_spec = _catalog_spec_for(contract.name)
        if dataset_spec is not None:
            return dataset_spec.schema()
        return contract.schema
    dataset_spec = _catalog_spec_for(rule.contract_name)
    if dataset_spec is not None:
        return dataset_spec.schema()
    return None


def _resolve_relationship_rules(
    rules: Sequence[RelationshipRule],
    *,
    contracts: ContractCatalog | None,
    policy_registry: PolicyRegistry,
) -> tuple[RelationshipRule, ...]:
    resolved: list[RelationshipRule] = []
    for rule in rules:
        schema = _relationship_rule_schema(rule, contracts=contracts)
        updated = rule
        if schema is not None:
            updated = apply_policy_defaults(rule, schema, registry=policy_registry)
            if updated.evidence is None:
                inferred = evidence_spec_from_schema(schema)
                if inferred is not None:
                    updated = replace(updated, evidence=inferred)
            validate_policy_requirements(updated, schema)
        resolved.append(updated)
    return tuple(resolved)


def _relationship_evidence_catalog(
    relspec_input_datasets: Mapping[str, TableLike],
) -> EvidenceCatalog:
    evidence = EvidenceCatalog()
    for name, table in relspec_input_datasets.items():
        evidence.register(name, table.schema)
    return evidence


def _relationship_output_schema_map(
    rules: Sequence[RelationshipRule],
    *,
    contracts: ContractCatalog,
) -> dict[str, SchemaLike | None]:
    schemas: dict[str, SchemaLike | None] = {}
    for rule in rules:
        output = rule.output_dataset
        if output in schemas and schemas[output] is not None:
            continue
        schemas[output] = _relationship_rule_schema(rule, contracts=contracts)
    return schemas


def _runtime_telemetry(ctx: ExecutionContext) -> Mapping[str, object]:
    profile = ctx.runtime.datafusion
    if profile is None:
        return {}
    return {"datafusion": profile.telemetry_payload_v1()}


def _param_table_aliases(
    param_tables: Mapping[str, IbisTable],
    *,
    policy: ParamTablePolicy,
) -> dict[str, IbisTable]:
    aliases: dict[str, IbisTable] = {}
    for logical_name, table in param_tables.items():
        table_name = param_table_name(policy, logical_name)
        names = (logical_name, table_name)
        for name in names:
            aliases.setdefault(name, table)
    return aliases


@dataclass(frozen=True)
class _CompositePlanResolver(PlanResolver[IbisPlan]):
    primary: PlanResolver[IbisPlan]
    fallback: PlanResolver[IbisPlan]
    primary_names: frozenset[str]
    backend: BaseBackend | None = None

    def __post_init__(self) -> None:
        if self.backend is not None:
            return
        resolved = self.primary.backend or self.fallback.backend
        object.__setattr__(self, "backend", resolved)

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> IbisPlan:
        if ref.name in self.primary_names:
            return self.primary.resolve(ref, ctx=ctx)
        return self.fallback.resolve(ref, ctx=ctx)

    def telemetry(
        self,
        ref: DatasetRef,
        *,
        ctx: ExecutionContext,
    ) -> ScanTelemetry | None:
        if ref.name in self.primary_names:
            return self.primary.telemetry(ref, ctx=ctx)
        return self.fallback.telemetry(ref, ctx=ctx)


def _compile_relationship_outputs(
    rules: Sequence[RelationshipRule],
    *,
    compiler: RelationshipRuleCompiler,
    ctx: ExecutionContext,
    contracts: ContractCatalog | None,
    edge_validation: EdgeContractValidationConfig,
) -> dict[str, CompiledOutput]:
    if contracts is not None:
        validate_relationship_output_contracts_for_edge_kinds(
            rules=rules,
            contract_catalog=contracts,
            config=edge_validation,
        )
    by_output: dict[str, list[RelationshipRule]] = {}
    for rule in rules:
        by_output.setdefault(rule.output_dataset, []).append(rule)
    compiled: dict[str, CompiledOutput] = {}
    for out_name in sorted(by_output):
        output_rules = sorted(by_output[out_name], key=lambda rule: (rule.priority, rule.name))
        contract_names = {rule.contract_name for rule in output_rules}
        if len(contract_names) > 1:
            msg = (
                f"Output {out_name!r} has inconsistent contract_name across rules: "
                f"{contract_names}."
            )
            raise ValueError(msg)
        contributors = tuple(compiler.compile_rule(rule, ctx=ctx) for rule in output_rules)
        telemetry = compiler.collect_scan_telemetry(output_rules, ctx=ctx)
        runtime_telemetry = _runtime_telemetry(ctx)
        compiled[out_name] = CompiledOutput(
            output_dataset=out_name,
            contract_name=output_rules[0].contract_name,
            contributors=contributors,
            telemetry=telemetry,
            runtime_telemetry=runtime_telemetry,
        )
    return compiled


# -----------------------------
# Relationship compilation + execution
# -----------------------------


@cache()
@tag(layer="relspec", artifact="resolved_relationship_rules", kind="object")
def resolved_relationship_rules(
    rule_registry_context: RuleRegistryContext,
    relationship_contracts: ContractCatalog,
    ctx: ExecutionContext,
) -> tuple[RelationshipRule, ...]:
    """Return resolved relationship rules with policies applied."""
    rule_compiler = RuleCompiler(
        handlers=default_rule_handlers(
            policies=rule_registry_context.pipeline_policy.policy_registry
        )
    )
    compiled = rule_compiler.compile_rules(
        rule_registry_context.rule_registry.rules_for_domain("cpg"),
        ctx=ctx,
    )
    rules = cast("tuple[RelationshipRule, ...]", compiled)
    resolved = _resolve_relationship_rules(
        rules,
        contracts=relationship_contracts,
        policy_registry=rule_registry_context.pipeline_policy.policy_registry,
    )
    contract_names = set(relationship_contracts.names())
    return tuple(
        rule
        for rule in resolved
        if rule.contract_name is not None and rule.contract_name in contract_names
    )


@tag(layer="relspec", artifact="relspec_evidence_catalog", kind="object")
def relspec_evidence_catalog(
    relspec_input_datasets: dict[str, TableLike],
) -> EvidenceCatalog:
    """Return evidence catalog seeded from relationship input datasets."""
    return _relationship_evidence_catalog(relspec_input_datasets)


@tag(layer="relspec", artifact="relspec_rule_graph", kind="object")
def relspec_rule_graph(
    resolved_relationship_rules: tuple[RelationshipRule, ...],
) -> RuleGraph:
    """Return the rustworkx rule graph for relationship rules."""
    return build_rule_graph_from_relationship_rules(resolved_relationship_rules)


@tag(layer="relspec", artifact="relspec_rule_schedule", kind="object")
def relspec_rule_schedule(
    resolved_relationship_rules: tuple[RelationshipRule, ...],
    relspec_rule_graph: RuleGraph,
    relspec_evidence_catalog: EvidenceCatalog,
    relationship_contracts: ContractCatalog,
) -> RuleSchedule:
    """Return a deterministic schedule for relationship rules."""
    output_schemas = _relationship_output_schema_map(
        resolved_relationship_rules,
        contracts=relationship_contracts,
    )
    return schedule_rules(
        relspec_rule_graph,
        evidence=relspec_evidence_catalog,
        output_schema_for=output_schemas.get,
    )


@cache()
@tag(layer="relspec", artifact="compiled_relationship_outputs", kind="object")
def compiled_relationship_outputs(
    resolved_relationship_rules: tuple[RelationshipRule, ...],
    relspec_resolver: PlanResolver[IbisPlan],
    relationship_contracts: ContractCatalog,  # add dep
    ctx: ExecutionContext,
) -> dict[str, CompiledOutput]:
    """Compile relationship rules into executable outputs.

    Returns
    -------
    dict[str, CompiledOutput]
        Compiled relationship outputs.
    """
    compiler = RelationshipRuleCompiler(resolver=relspec_resolver)
    return _compile_relationship_outputs(
        resolved_relationship_rules,
        compiler=compiler,
        ctx=ctx,
        contracts=relationship_contracts,
        edge_validation=EdgeContractValidationConfig(),
    )


_HASH_JOIN_SEPARATOR = "\x1f"


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _coalesce_cast(name: str, *, null_sentinel: str) -> str:
    return f"coalesce(CAST({_sql_identifier(name)} AS STRING), {_sql_literal(null_sentinel)})"


def _hash_expr_sql(spec: HashExprSpec) -> str:
    parts: list[str] = [_sql_literal(spec.prefix)]
    parts.extend(_sql_literal(value) for value in spec.extra_literals)
    parts.extend(_coalesce_cast(name, null_sentinel=spec.null_sentinel) for name in spec.cols)
    if len(parts) == 1:
        joined = parts[0]
    else:
        delimiter = _sql_literal(_HASH_JOIN_SEPARATOR)
        joined = f"concat_ws({delimiter}, {', '.join(parts)})"
    hash_name = "stable_hash128" if spec.as_string else "stable_hash64"
    hashed = f"{hash_name}({joined})"
    if spec.as_string:
        return f"concat_ws(':', {_sql_literal(spec.prefix)}, {hashed})"
    return hashed


def _datafusion_context_from_exec(ctx: ExecutionContext) -> SessionContext:
    profile = ctx.runtime.datafusion
    if profile is None:
        msg = "DataFusion runtime profile is required to hash edge owner file ids."
        raise TypeError(msg)
    try:
        return profile.session_context()
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = "Failed to create DataFusion SessionContext for hash computation."
        raise RuntimeError(msg) from exc


def _add_edge_owner_file_id(
    table: TableLike,
    *,
    repo_id: str | None,
    ctx: ExecutionContext,
    schema: SchemaLike,
) -> TableLike:
    if "edge_owner_file_id" in table.column_names:
        return table
    spec = replace(repo_file_id_spec(repo_id), out_col="edge_owner_file_id")
    if spec.out_col is None:
        msg = "Edge owner file id spec must declare an output column."
        raise ValueError(msg)
    df_ctx = _datafusion_context_from_exec(ctx)
    resolved = coerce_table_like(table)
    if isinstance(resolved, pa.RecordBatchReader):
        reader = cast("RecordBatchReaderLike", resolved)
        resolved_table = pa.Table.from_batches(list(reader))
    else:
        resolved_table = cast("pa.Table", resolved)
    table_name = f"_edge_owner_file_id_{uuid.uuid4().hex}"
    df_ctx.register_record_batches(table_name, [resolved_table.to_batches()])
    try:
        expr_sql = _hash_expr_sql(spec)
        sql = (
            f"SELECT *, {expr_sql} AS {_sql_identifier(spec.out_col)} "
            f"FROM {_sql_identifier(table_name)}"
        )
        sql_options = sql_options_for_profile(ctx.runtime.datafusion)
        updated = df_ctx.sql_with_options(sql, sql_options).to_arrow_table()
    finally:
        deregister = getattr(df_ctx, "deregister_table", None)
        if callable(deregister):
            deregister(table_name)
    return align_table(
        updated,
        schema=schema,
        safe_cast=ctx.safe_cast,
        keep_extra_columns=ctx.provenance,
    )


@tag(layer="relspec", artifact="relationship_table_inputs", kind="object")
def relationship_table_inputs(
    compiled_relationship_outputs: dict[str, CompiledOutput],
    relspec_rule_graph: RuleGraph,
    relspec_rule_schedule: RuleSchedule,
    relspec_evidence_catalog: EvidenceCatalog,
    relspec_resolver: PlanResolver[IbisPlan],
    relationship_contracts: ContractCatalog,
    engine_session: EngineSession,
    incremental_config: IncrementalConfig,
) -> RelationshipTableInputs:
    """Bundle inputs required to materialize relationship tables.

    Returns
    -------
    RelationshipTableInputs
        Inputs for relationship table execution.
    """
    return RelationshipTableInputs(
        compiled_relationship_outputs=compiled_relationship_outputs,
        relspec_rule_graph=relspec_rule_graph,
        relspec_rule_schedule=relspec_rule_schedule,
        relspec_evidence_catalog=relspec_evidence_catalog,
        relspec_resolver=relspec_resolver,
        relationship_contracts=relationship_contracts,
        engine_session=engine_session,
        incremental_config=incremental_config,
    )


def _execute_relationship_rule_graph(
    *,
    graph: RuleGraph,
    compiled_outputs: Mapping[str, CompiledOutput],
    evidence: EvidenceCatalog,
    base_resolver: PlanResolver[IbisPlan],
    relationship_contracts: ContractCatalog,
    engine_session: EngineSession,
    execution_context: RelationshipExecutionContext,
    incremental_config: IncrementalConfig,
) -> tuple[dict[str, TableLike], RuleExecutionEventCollector]:
    exec_ctx = execution_context.ctx
    execution_policy = execution_context.execution_policy
    ibis_backend = execution_context.ibis_backend
    relspec_param_bindings = execution_context.relspec_param_bindings
    working = evidence.clone()
    seed_nodes = _seed_graph_nodes(graph, working.sources)
    sorter = _make_graph_sorter(graph, seed_nodes=seed_nodes)
    out: dict[str, TableLike] = {}
    dynamic_outputs: dict[str, TableLike] = {}
    executed_outputs: set[str] = set()
    visited_rules: set[str] = set()
    event_collector = RuleExecutionEventCollector(diagnostics=engine_session.diagnostics)
    exec_options = CompiledOutputExecutionOptions(
        contracts=relationship_contracts,
        params=relspec_param_bindings,
        execution_policy=execution_policy,
        ibis_backend=ibis_backend,
        rule_exec_observer=event_collector,
        surface_policy=execution_context.surface_policy,
    )
    while sorter.is_active():
        ready = list(sorter.get_ready())
        if not ready:
            break
        done_nodes: list[int] = []
        for idx in ready:
            node = graph.graph[idx]
            if node.kind != "rule":
                continue
            payload = node.payload
            if not isinstance(payload, RuleNode):
                msg = "Expected RuleNode payload for rule graph node."
                raise TypeError(msg)
            if not working.satisfies(payload.evidence, inputs=payload.inputs):
                msg = f"Relationship rule graph cannot resolve evidence for: {payload.name!r}."
                raise RelspecValidationError(msg)
            visited_rules.add(payload.name)
            done_nodes.append(idx)
        for idx in ready:
            node = graph.graph[idx]
            if node.kind != "evidence":
                continue
            payload = node.payload
            if not isinstance(payload, EvidenceNode):
                msg = "Expected EvidenceNode payload for evidence graph node."
                raise TypeError(msg)
            name = payload.name
            if _is_output_node(graph, idx):
                compiled = compiled_outputs.get(name)
                if compiled is None:
                    msg = f"Missing compiled output for {name!r}."
                    raise RelspecValidationError(msg)
                if name not in executed_outputs:
                    resolver = _resolver_with_dynamic_outputs(
                        base_resolver,
                        dynamic_outputs,
                        ibis_backend=ibis_backend,
                    )
                    result = compiled.execute(
                        ctx=exec_ctx,
                        resolver=resolver,
                        options=exec_options,
                    )
                    contract_schema = _output_contract_schema(
                        compiled,
                        contracts=relationship_contracts,
                    )
                    updated = _add_edge_owner_file_id(
                        result.good,
                        repo_id=incremental_config.repo_id,
                        ctx=exec_ctx,
                        schema=contract_schema,
                    )
                    out[name] = updated
                    dynamic_outputs[name] = updated
                    _register_output_evidence(
                        working,
                        name=name,
                        compiled=compiled,
                        table=updated,
                        contracts=relationship_contracts,
                    )
                    executed_outputs.add(name)
            done_nodes.append(idx)
        sorter.done(done_nodes)
    missing = sorted(set(graph.rule_idx) - visited_rules)
    if missing:
        msg = f"Relationship rule graph cannot resolve evidence for: {missing}."
        raise RelspecValidationError(msg)
    return out, event_collector


def _seed_graph_nodes(graph: RuleGraph, seed_sources: Iterable[str]) -> list[int]:
    nodes: list[int] = []
    for name in seed_sources:
        idx = graph.evidence_idx.get(name)
        if idx is not None:
            nodes.append(idx)
    return nodes


def _make_graph_sorter(graph: RuleGraph, *, seed_nodes: list[int]) -> rx.TopologicalSorter:
    try:
        return rx.TopologicalSorter(graph.graph, check_cycle=True, initial=seed_nodes)
    except (TypeError, ValueError) as exc:
        msg = "Relationship rule graph contains a cycle or invalid dependency."
        raise RelspecValidationError(msg) from exc


def _is_output_node(graph: RuleGraph, node_idx: int) -> bool:
    for pred_idx in graph.graph.predecessor_indices(node_idx):
        pred = graph.graph[pred_idx]
        if pred.kind == "rule":
            return True
    return False


def _resolver_with_dynamic_outputs(
    base_resolver: PlanResolver[IbisPlan],
    outputs: Mapping[str, TableLike],
    *,
    ibis_backend: BaseBackend,
) -> PlanResolver[IbisPlan]:
    if not outputs:
        return base_resolver
    dynamic_resolver = InMemoryPlanResolver(outputs, backend=ibis_backend)
    return _CompositePlanResolver(
        primary=dynamic_resolver,
        fallback=base_resolver,
        primary_names=frozenset(outputs),
    )


def _output_contract_schema(
    compiled: CompiledOutput,
    *,
    contracts: ContractCatalog,
) -> SchemaLike:
    if compiled.contract_name is None:
        return relation_output_schema()
    return contracts.get(compiled.contract_name).schema


def _register_output_evidence(
    evidence: EvidenceCatalog,
    *,
    name: str,
    compiled: CompiledOutput,
    table: TableLike,
    contracts: ContractCatalog,
) -> None:
    if compiled.contract_name is not None:
        evidence.register(name, contracts.get(compiled.contract_name).schema)
        return
    schema = getattr(table, "schema", None)
    if schema is None or not hasattr(schema, "names"):
        evidence.sources.add(name)
        return
    evidence.register(name, schema)


@cache()
@extract_fields(
    {
        "rel_name_symbol": TableLike,
        "rel_import_symbol": TableLike,
        "rel_def_symbol": TableLike,
        "rel_callsite_symbol": TableLike,
        "rel_callsite_qname": TableLike,
        "relspec_rule_exec_events": pa.Table,
    }
)
@tag(layer="relspec", artifact="relationship_tables", kind="bundle")
def relationship_tables(
    relationship_execution_context: RelationshipExecutionContext,
    relationship_table_inputs: RelationshipTableInputs,
) -> dict[str, TableLike]:
    """Execute compiled relationship outputs into tables.

    Returns
    -------
    dict[str, TableLike]
        Relationship tables keyed by output dataset.
    """
    exec_ctx = relationship_execution_context.ctx
    execution_policy = relationship_execution_context.execution_policy
    ibis_backend = relationship_execution_context.ibis_backend
    relspec_param_bindings = relationship_execution_context.relspec_param_bindings

    out, event_collector = _execute_relationship_rule_graph(
        graph=relationship_table_inputs.relspec_rule_graph,
        compiled_outputs=relationship_table_inputs.compiled_relationship_outputs,
        evidence=relationship_table_inputs.relspec_evidence_catalog,
        base_resolver=relationship_table_inputs.relspec_resolver,
        relationship_contracts=relationship_table_inputs.relationship_contracts,
        engine_session=relationship_table_inputs.engine_session,
        execution_context=relationship_execution_context,
        incremental_config=relationship_table_inputs.incremental_config,
    )

    # Ensure expected keys exist for extract_fields
    out.setdefault(
        "rel_name_symbol",
        empty_table(
            relationship_table_inputs.relationship_contracts.get("rel_name_symbol_v1").schema
        ),
    )
    out.setdefault(
        "rel_import_symbol",
        empty_table(
            relationship_table_inputs.relationship_contracts.get("rel_import_symbol_v1").schema
        ),
    )
    out.setdefault(
        "rel_def_symbol",
        empty_table(
            relationship_table_inputs.relationship_contracts.get("rel_def_symbol_v1").schema
        ),
    )
    out.setdefault(
        "rel_callsite_symbol",
        empty_table(
            relationship_table_inputs.relationship_contracts.get("rel_callsite_symbol_v1").schema
        ),
    )
    out.setdefault(
        "rel_callsite_qname",
        empty_table(
            relationship_table_inputs.relationship_contracts.get("rel_callsite_qname_v1").schema
        ),
    )
    out["relspec_rule_exec_events"] = event_collector.table()
    return out


@tag(layer="relspec", artifact="relationship_output_tables", kind="bundle")
def relationship_output_tables(
    rel_name_symbol: TableLike,
    rel_import_symbol: TableLike,
    rel_def_symbol: TableLike,
    rel_callsite_symbol: TableLike,
    rel_callsite_qname: TableLike,
) -> RelationshipOutputTables:
    """Bundle relationship output tables for downstream consumers.

    Returns
    -------
    RelationshipOutputTables
        Relationship output bundle.
    """
    return RelationshipOutputTables(
        rel_name_symbol=rel_name_symbol,
        rel_import_symbol=rel_import_symbol,
        rel_def_symbol=rel_def_symbol,
        rel_callsite_symbol=rel_callsite_symbol,
        rel_callsite_qname=rel_callsite_qname,
    )


# -----------------------------
# CPG build
# -----------------------------


@tag(layer="cpg", artifact="cst_build_inputs", kind="bundle")
def cst_build_inputs(
    cst_refs: TableLike | DatasetSource | ViewReference,
    cst_imports_norm: TableLike | DatasetSource | ViewReference,
    cst_callsites: TableLike | DatasetSource | ViewReference,
    cst_defs_norm: TableLike | DatasetSource | ViewReference,
) -> CstBuildInputs:
    """Bundle CST inputs for CPG builds.

    Returns
    -------
    CstBuildInputs
        CST build input bundle.
    """
    return CstBuildInputs(
        cst_refs=cst_refs,
        cst_imports_norm=cst_imports_norm,
        cst_callsites=cst_callsites,
        cst_defs_norm=cst_defs_norm,
    )


@tag(layer="cpg", artifact="scip_build_inputs", kind="bundle")
def scip_build_inputs(
    scip_symbol_information: TableLike | DatasetSource | ViewReference,
    scip_occurrences_norm: TableLike | DatasetSource | ViewReference,
    scip_symbol_relationships: TableLike | DatasetSource | ViewReference,
    scip_external_symbol_information: TableLike | DatasetSource | ViewReference,
) -> ScipBuildInputs:
    """Bundle SCIP inputs for CPG builds.

    Returns
    -------
    ScipBuildInputs
        SCIP build input bundle.
    """
    return ScipBuildInputs(
        scip_symbol_information=scip_symbol_information,
        scip_occurrences_norm=scip_occurrences_norm,
        scip_symbol_relationships=scip_symbol_relationships,
        scip_external_symbol_information=scip_external_symbol_information,
    )


@dataclass(frozen=True)
class SymtableCoreInputs:
    """Core symtable inputs for relationship builds."""

    symtable_scopes: TableLike | DatasetSource | ViewReference
    symtable_symbols: TableLike | DatasetSource | ViewReference
    symtable_scope_edges: TableLike | DatasetSource | ViewReference
    symtable_bindings: TableLike | DatasetSource | ViewReference


@dataclass(frozen=True)
class SymtableSiteInputs:
    """Symtable site inputs for relationship builds."""

    symtable_def_sites: TableLike | DatasetSource | ViewReference
    symtable_use_sites: TableLike | DatasetSource | ViewReference
    symtable_type_params: TableLike | DatasetSource | ViewReference
    symtable_type_param_edges: TableLike | DatasetSource | ViewReference


@tag(layer="cpg", artifact="symtable_core_inputs", kind="bundle")
def symtable_core_inputs(
    symtable_scopes: TableLike | DatasetSource | ViewReference,
    symtable_symbols: TableLike | DatasetSource | ViewReference,
    symtable_scope_edges: TableLike | DatasetSource | ViewReference,
    symtable_bindings: TableLike | DatasetSource | ViewReference,
) -> SymtableCoreInputs:
    """Bundle core symtable inputs for CPG builds.

    Returns
    -------
    SymtableCoreInputs
        Core symtable build input bundle.
    """
    return SymtableCoreInputs(
        symtable_scopes=symtable_scopes,
        symtable_symbols=symtable_symbols,
        symtable_scope_edges=symtable_scope_edges,
        symtable_bindings=symtable_bindings,
    )


@tag(layer="cpg", artifact="symtable_site_inputs", kind="bundle")
def symtable_site_inputs(
    symtable_def_sites: TableLike | DatasetSource | ViewReference,
    symtable_use_sites: TableLike | DatasetSource | ViewReference,
    symtable_type_params: TableLike | DatasetSource | ViewReference,
    symtable_type_param_edges: TableLike | DatasetSource | ViewReference,
) -> SymtableSiteInputs:
    """Bundle symtable site inputs for CPG builds.

    Returns
    -------
    SymtableSiteInputs
        Symtable site build input bundle.
    """
    return SymtableSiteInputs(
        symtable_def_sites=symtable_def_sites,
        symtable_use_sites=symtable_use_sites,
        symtable_type_params=symtable_type_params,
        symtable_type_param_edges=symtable_type_param_edges,
    )


@tag(layer="cpg", artifact="symtable_build_inputs", kind="bundle")
def symtable_build_inputs(
    symtable_core_inputs: SymtableCoreInputs,
    symtable_site_inputs: SymtableSiteInputs,
) -> SymtableBuildInputs:
    """Bundle symtable inputs for CPG builds.

    Returns
    -------
    SymtableBuildInputs
        Symtable build input bundle.
    """
    return SymtableBuildInputs(
        symtable_scopes=symtable_core_inputs.symtable_scopes,
        symtable_symbols=symtable_core_inputs.symtable_symbols,
        symtable_scope_edges=symtable_core_inputs.symtable_scope_edges,
        symtable_bindings=symtable_core_inputs.symtable_bindings,
        symtable_def_sites=symtable_site_inputs.symtable_def_sites,
        symtable_use_sites=symtable_site_inputs.symtable_use_sites,
        symtable_type_params=symtable_site_inputs.symtable_type_params,
        symtable_type_param_edges=symtable_site_inputs.symtable_type_param_edges,
    )


@tag(layer="cpg", artifact="cpg_base_inputs", kind="bundle")
def cpg_base_inputs(
    repo_files: TableLike | DatasetSource | ViewReference,
    dim_qualified_names: TableLike | DatasetSource | ViewReference,
    cst_build_inputs: CstBuildInputs,
    scip_build_inputs: ScipBuildInputs,
    symtable_build_inputs: SymtableBuildInputs,
) -> CpgBaseInputs:
    """Bundle shared inputs for CPG nodes and properties.

    Returns
    -------
    CpgBaseInputs
        Shared CPG input bundle.
    """
    return CpgBaseInputs(
        repo_files=repo_files,
        dim_qualified_names=dim_qualified_names,
        cst_build_inputs=cst_build_inputs,
        scip_build_inputs=scip_build_inputs,
        symtable_build_inputs=symtable_build_inputs,
    )


@tag(layer="cpg", artifact="tree_sitter_inputs", kind="bundle")
def tree_sitter_inputs(
    ts_nodes: TableLike | DatasetSource | ViewReference,
    ts_errors: TableLike | DatasetSource | ViewReference,
    ts_missing: TableLike | DatasetSource | ViewReference,
) -> TreeSitterInputs:
    """Bundle tree-sitter inputs for CPG construction.

    Returns
    -------
    TreeSitterInputs
        Tree-sitter input bundle.
    """
    return TreeSitterInputs(ts_nodes=ts_nodes, ts_errors=ts_errors, ts_missing=ts_missing)


@tag(layer="cpg", artifact="type_inputs", kind="bundle")
def type_inputs(
    type_exprs_norm: TableLike | DatasetSource | ViewReference,
    types_norm: TableLike | DatasetSource | ViewReference,
) -> TypeInputs:
    """Bundle type inputs for CPG construction.

    Returns
    -------
    TypeInputs
        Type input bundle.
    """
    return TypeInputs(type_exprs_norm=type_exprs_norm, types_norm=types_norm)


@tag(layer="cpg", artifact="diagnostics_inputs", kind="bundle")
def diagnostics_inputs(
    diagnostics_norm: TableLike | DatasetSource | ViewReference,
) -> DiagnosticsInputs:
    """Bundle diagnostics inputs for CPG construction.

    Returns
    -------
    DiagnosticsInputs
        Diagnostics input bundle.
    """
    return DiagnosticsInputs(diagnostics_norm=diagnostics_norm)


@tag(layer="cpg", artifact="runtime_inputs", kind="bundle")
def runtime_inputs(
    rt_objects: TableLike | DatasetSource | ViewReference,
    rt_signatures: TableLike | DatasetSource | ViewReference,
    rt_signature_params: TableLike | DatasetSource | ViewReference,
    rt_members: TableLike | DatasetSource | ViewReference,
) -> RuntimeInputs:
    """Bundle runtime inspection inputs for CPG construction.

    Returns
    -------
    RuntimeInputs
        Runtime inspection input bundle.
    """
    return RuntimeInputs(
        rt_objects=rt_objects,
        rt_signatures=rt_signatures,
        rt_signature_params=rt_signature_params,
        rt_members=rt_members,
    )


@tag(layer="cpg", artifact="cpg_extra_inputs", kind="bundle")
def cpg_extra_inputs(
    tree_sitter_inputs: TreeSitterInputs,
    type_inputs: TypeInputs,
    diagnostics_inputs: DiagnosticsInputs,
    runtime_inputs: RuntimeInputs,
) -> CpgExtraInputs:
    """Bundle optional CPG inputs.

    Returns
    -------
    CpgExtraInputs
        Optional CPG input bundle.
    """
    return CpgExtraInputs(
        ts_nodes=tree_sitter_inputs.ts_nodes,
        ts_errors=tree_sitter_inputs.ts_errors,
        ts_missing=tree_sitter_inputs.ts_missing,
        type_exprs_norm=type_inputs.type_exprs_norm,
        types_norm=type_inputs.types_norm,
        diagnostics_norm=diagnostics_inputs.diagnostics_norm,
        rt_objects=runtime_inputs.rt_objects,
        rt_signatures=runtime_inputs.rt_signatures,
        rt_signature_params=runtime_inputs.rt_signature_params,
        rt_members=runtime_inputs.rt_members,
    )


@cache()
@tag(layer="cpg", artifact="symtable_binding_resolutions", kind="table")
def symtable_binding_resolutions(
    cpg_base_inputs: CpgBaseInputs,
    engine_session: EngineSession,
) -> ViewReference:
    """Resolve symtable binding references to outer scopes.

    Returns
    -------
    ViewReference
        Binding resolution view with ambiguity grouping.
    """
    _ = cpg_base_inputs.symtable_build_inputs
    return _register_view_reference(
        engine_session.ibis_backend,
        name="symtable_binding_resolutions",
        builder=symtable_binding_resolutions_df,
        runtime_profile=engine_session.datafusion_profile,
    )


@tag(layer="cpg", artifact="cpg_node_inputs", kind="bundle")
def cpg_node_inputs(
    cpg_base_inputs: CpgBaseInputs,
    cpg_extra_inputs: CpgExtraInputs,
) -> NodeInputTables:
    """Build node input tables from base and optional inputs.

    Returns
    -------
    NodeInputTables
        Node input table bundle.
    """
    return NodeInputTables(
        repo_files=cpg_base_inputs.repo_files,
        cst_refs=cpg_base_inputs.cst_build_inputs.cst_refs,
        cst_imports=cpg_base_inputs.cst_build_inputs.cst_imports_norm,
        cst_callsites=cpg_base_inputs.cst_build_inputs.cst_callsites,
        cst_defs=cpg_base_inputs.cst_build_inputs.cst_defs_norm,
        dim_qualified_names=cpg_base_inputs.dim_qualified_names,
        scip_symbol_information=cpg_base_inputs.scip_build_inputs.scip_symbol_information,
        scip_occurrences=cpg_base_inputs.scip_build_inputs.scip_occurrences_norm,
        scip_external_symbol_information=(
            cpg_base_inputs.scip_build_inputs.scip_external_symbol_information
        ),
        scip_symbol_relationships=cpg_base_inputs.scip_build_inputs.scip_symbol_relationships,
        symtable_scopes=cpg_base_inputs.symtable_build_inputs.symtable_scopes,
        symtable_symbols=cpg_base_inputs.symtable_build_inputs.symtable_symbols,
        symtable_bindings=cpg_base_inputs.symtable_build_inputs.symtable_bindings,
        symtable_def_sites=cpg_base_inputs.symtable_build_inputs.symtable_def_sites,
        symtable_use_sites=cpg_base_inputs.symtable_build_inputs.symtable_use_sites,
        symtable_type_params=cpg_base_inputs.symtable_build_inputs.symtable_type_params,
        ts_nodes=cpg_extra_inputs.ts_nodes,
        ts_errors=cpg_extra_inputs.ts_errors,
        ts_missing=cpg_extra_inputs.ts_missing,
        type_exprs_norm=cpg_extra_inputs.type_exprs_norm,
        types_norm=cpg_extra_inputs.types_norm,
        diagnostics_norm=cpg_extra_inputs.diagnostics_norm,
        rt_objects=cpg_extra_inputs.rt_objects,
        rt_signatures=cpg_extra_inputs.rt_signatures,
        rt_signature_params=cpg_extra_inputs.rt_signature_params,
        rt_members=cpg_extra_inputs.rt_members,
    )


@tag(layer="cpg", artifact="cpg_edge_inputs", kind="bundle")
def cpg_edge_inputs(
    relationship_output_tables: RelationshipOutputTables,
    cpg_base_inputs: CpgBaseInputs,
    cpg_extra_inputs: CpgExtraInputs,
    symtable_binding_resolutions: TableLike | DatasetSource | ViewReference,
) -> EdgeBuildInputs:
    """Build edge input tables from base and optional inputs.

    Returns
    -------
    EdgeBuildInputs
        Edge input table bundle.
    """
    return EdgeBuildInputs(
        relationship_outputs=relationship_output_tables.as_dict(),
        scip_symbol_relationships=cpg_base_inputs.scip_build_inputs.scip_symbol_relationships,
        diagnostics_norm=cpg_extra_inputs.diagnostics_norm,
        repo_files=cpg_base_inputs.repo_files,
        type_exprs_norm=cpg_extra_inputs.type_exprs_norm,
        rt_signatures=cpg_extra_inputs.rt_signatures,
        rt_signature_params=cpg_extra_inputs.rt_signature_params,
        rt_members=cpg_extra_inputs.rt_members,
        symtable_scope_edges=cpg_base_inputs.symtable_build_inputs.symtable_scope_edges,
        symtable_bindings=cpg_base_inputs.symtable_build_inputs.symtable_bindings,
        symtable_def_sites=cpg_base_inputs.symtable_build_inputs.symtable_def_sites,
        symtable_use_sites=cpg_base_inputs.symtable_build_inputs.symtable_use_sites,
        symtable_binding_resolutions=symtable_binding_resolutions,
        symtable_type_param_edges=cpg_base_inputs.symtable_build_inputs.symtable_type_param_edges,
    )


@tag(layer="cpg", artifact="cpg_props_inputs", kind="bundle")
def cpg_props_inputs(
    cpg_base_inputs: CpgBaseInputs,
    cpg_extra_inputs: CpgExtraInputs,
    cpg_edges_delta: TableLike,
) -> PropsInputTables:
    """Build property input tables from base and optional inputs.

    Returns
    -------
    PropsInputTables
        Property input table bundle.
    """
    return PropsInputTables(
        repo_files=cpg_base_inputs.repo_files,
        cst_refs=cpg_base_inputs.cst_build_inputs.cst_refs,
        cst_imports=cpg_base_inputs.cst_build_inputs.cst_imports_norm,
        cst_callsites=cpg_base_inputs.cst_build_inputs.cst_callsites,
        cst_defs=cpg_base_inputs.cst_build_inputs.cst_defs_norm,
        dim_qualified_names=cpg_base_inputs.dim_qualified_names,
        scip_symbol_information=cpg_base_inputs.scip_build_inputs.scip_symbol_information,
        scip_occurrences=cpg_base_inputs.scip_build_inputs.scip_occurrences_norm,
        scip_external_symbol_information=(
            cpg_base_inputs.scip_build_inputs.scip_external_symbol_information
        ),
        symtable_scopes=cpg_base_inputs.symtable_build_inputs.symtable_scopes,
        symtable_symbols=cpg_base_inputs.symtable_build_inputs.symtable_symbols,
        symtable_bindings=cpg_base_inputs.symtable_build_inputs.symtable_bindings,
        symtable_def_sites=cpg_base_inputs.symtable_build_inputs.symtable_def_sites,
        symtable_use_sites=cpg_base_inputs.symtable_build_inputs.symtable_use_sites,
        symtable_type_params=cpg_base_inputs.symtable_build_inputs.symtable_type_params,
        ts_nodes=cpg_extra_inputs.ts_nodes,
        ts_errors=cpg_extra_inputs.ts_errors,
        ts_missing=cpg_extra_inputs.ts_missing,
        type_exprs_norm=cpg_extra_inputs.type_exprs_norm,
        types_norm=cpg_extra_inputs.types_norm,
        diagnostics_norm=cpg_extra_inputs.diagnostics_norm,
        rt_objects=cpg_extra_inputs.rt_objects,
        rt_signatures=cpg_extra_inputs.rt_signatures,
        rt_signature_params=cpg_extra_inputs.rt_signature_params,
        rt_members=cpg_extra_inputs.rt_members,
        cpg_edges=cpg_edges_delta,
    )


@cache()
@tag(layer="cpg", artifact="cpg_nodes_finalize", kind="object")
def cpg_nodes_finalize(
    engine_session: EngineSession,
    cpg_node_inputs: NodeInputTables,
    adapter_execution_policy: AdapterExecutionPolicy,
) -> CpgBuildArtifacts:
    """Build finalized CPG nodes with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalized nodes bundle plus quality table.
    """
    return build_cpg_nodes(
        session=engine_session,
        config=NodeBuildConfig(
            inputs=cpg_node_inputs,
            execution_policy=adapter_execution_policy,
        ),
    )


@cache(format="delta")
@tag(layer="cpg", artifact="cpg_nodes_delta", kind="table")
def cpg_nodes_delta(
    cpg_nodes_finalize: CpgBuildArtifacts,
) -> TableLike:
    """Return incremental CPG node updates for the current run.

    Returns
    -------
    TableLike
        Node rows produced for the current run.
    """
    return cpg_nodes_finalize.finalize.good


@cache(format="delta")
@tag(layer="cpg", artifact="cpg_nodes_final", kind="table")
def cpg_nodes_final(
    cpg_nodes_finalize: CpgBuildArtifacts,
    incremental_state_store: StateStore | None,
    incremental_config: IncrementalConfig,
    incremental_cpg_nodes_updates: Mapping[str, str] | None,
) -> TableLike:
    """Build the final CPG nodes table.

    Returns
    -------
    TableLike
        Final CPG nodes table.
    """
    if incremental_config.enabled and incremental_state_store is not None:
        updates = incremental_cpg_nodes_updates or {}
        return publish_cpg_nodes(
            state_store=incremental_state_store,
            dataset_path=updates.get("cpg_nodes_v1"),
            fallback=cpg_nodes_finalize.finalize.good,
        )
    return cpg_nodes_finalize.finalize.good


@cache(format="delta")
@tag(layer="cpg", artifact="cpg_nodes_quality", kind="table")
def cpg_nodes_quality(
    cpg_nodes_finalize: CpgBuildArtifacts,
) -> TableLike:
    """Return quality artifacts for CPG nodes.

    Returns
    -------
    TableLike
        Quality table.
    """
    return cpg_nodes_finalize.quality


@cache(format="delta")
@tag(layer="cpg", artifact="cpg_edges_delta", kind="table")
def cpg_edges_delta(
    cpg_edges_finalize: CpgBuildArtifacts,
) -> TableLike:
    """Return incremental CPG edge updates for the current run.

    Returns
    -------
    TableLike
        Edge rows produced for the current run.
    """
    return cpg_edges_finalize.finalize.good


@cache(format="delta")
@tag(layer="cpg", artifact="cpg_edges_final", kind="table")
def cpg_edges_final(
    cpg_edges_finalize: CpgBuildArtifacts,
    incremental_state_store: StateStore | None,
    incremental_config: IncrementalConfig,
    incremental_cpg_edges_updates: Mapping[str, str] | None,
) -> TableLike:
    """Build the final CPG edges table.

    Returns
    -------
    TableLike
        Final CPG edges table.
    """
    if incremental_config.enabled and incremental_state_store is not None:
        updates = incremental_cpg_edges_updates or {}
        return publish_cpg_edges(
            state_store=incremental_state_store,
            dataset_path=updates.get("cpg_edges_v1"),
            fallback=cpg_edges_finalize.finalize.good,
        )
    return cpg_edges_finalize.finalize.good


@cache()
@tag(layer="cpg", artifact="cpg_edges_finalize", kind="object")
def cpg_edges_finalize(
    engine_session: EngineSession,
    cpg_edge_inputs: EdgeBuildInputs,
    adapter_execution_policy: AdapterExecutionPolicy,
) -> CpgBuildArtifacts:
    """Build finalized CPG edges with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalized edges bundle plus quality table.
    """
    return build_cpg_edges(
        session=engine_session,
        config=EdgeBuildConfig(
            inputs=cpg_edge_inputs,
            execution_policy=adapter_execution_policy,
        ),
    )


@cache(format="delta")
@tag(layer="cpg", artifact="cpg_edges_quality", kind="table")
def cpg_edges_quality(
    cpg_edges_finalize: CpgBuildArtifacts,
) -> TableLike:
    """Return quality artifacts for CPG edges.

    Returns
    -------
    TableLike
        Quality table.
    """
    return cpg_edges_finalize.quality


@cache(format="delta")
@tag(layer="cpg", artifact="cpg_props_delta", kind="table")
def cpg_props_delta(
    cpg_props_finalize: CpgBuildArtifacts,
) -> TableLike:
    """Return incremental CPG property updates for the current run.

    Returns
    -------
    TableLike
        Property rows produced for the current run.
    """
    return cpg_props_finalize.finalize.good


@cache(format="delta")
@tag(layer="cpg", artifact="cpg_props_json", kind="table")
def cpg_props_json(
    cpg_props_finalize: CpgBuildArtifacts,
) -> TableLike | None:
    """Return optional JSON-heavy CPG property rows for the current run.

    Returns
    -------
    TableLike | None
        JSON-heavy property rows when enabled.
    """
    return cpg_props_finalize.extra_outputs.get("cpg_props_json")


@cache(format="delta")
@tag(layer="cpg", artifact="cpg_props_final", kind="table")
def cpg_props_final(
    cpg_props_finalize: CpgBuildArtifacts,
    incremental_state_store: StateStore | None,
    incremental_config: IncrementalConfig,
    incremental_cpg_props_updates: Mapping[str, str] | None,
) -> TableLike:
    """Build the final CPG properties table.

    Returns
    -------
    TableLike
        Final CPG properties table.
    """
    if incremental_config.enabled and incremental_state_store is not None:
        updates = incremental_cpg_props_updates or {}
        return publish_cpg_props(
            state_store=incremental_state_store,
            by_file_path=updates.get("cpg_props_by_file_id_v1"),
            global_path=updates.get("cpg_props_global_v1"),
            fallback=cpg_props_finalize.finalize.good,
        )
    return cpg_props_finalize.finalize.good


@cache()
@tag(layer="cpg", artifact="cpg_props_finalize", kind="object")
def cpg_props_finalize(
    engine_session: EngineSession,
    cpg_props_inputs: PropsInputTables,
    cpg_props_options: PropsBuildOptions,
    adapter_execution_policy: AdapterExecutionPolicy,
) -> CpgBuildArtifacts:
    """Build finalized CPG properties with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalized properties bundle plus quality table.
    """
    return build_cpg_props(
        session=engine_session,
        config=PropsBuildConfig(
            inputs=cpg_props_inputs,
            options=cpg_props_options,
            execution_policy=adapter_execution_policy,
        ),
    )


@cache(format="delta")
@tag(layer="cpg", artifact="cpg_props_quality", kind="table")
def cpg_props_quality(
    cpg_props_finalize: CpgBuildArtifacts,
) -> TableLike:
    """Return quality artifacts for CPG properties.

    Returns
    -------
    TableLike
        Quality table.
    """
    return cpg_props_finalize.quality
