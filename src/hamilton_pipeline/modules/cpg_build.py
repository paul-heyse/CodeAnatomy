"""Hamilton CPG build stage and relationship rule wiring."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
from hamilton.function_modifiers import cache, extract_fields, tag
from ibis.backends import BaseBackend
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.io.parquet import (
    NamedDatasetWriteConfig,
    ParquetWriteOptions,
    write_named_datasets_parquet,
)
from arrowdsl.plan.query import ScanTelemetry
from arrowdsl.plan.runner import AdapterRunOptions, run_plan_adapter
from arrowdsl.plan.scan_io import DatasetSource
from arrowdsl.schema.schema import empty_table
from config import AdapterMode
from cpg.build_edges import EdgeBuildConfig, EdgeBuildInputs, build_cpg_edges
from cpg.build_nodes import NodeInputTables, build_cpg_nodes
from cpg.build_props import PropsInputTables, build_cpg_props
from cpg.constants import CpgBuildArtifacts
from cpg.schemas import register_cpg_specs
from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel
from hamilton_pipeline.pipeline_types import (
    CpgBaseInputs,
    CpgExtraInputs,
    CstBuildInputs,
    CstRelspecInputs,
    DiagnosticsInputs,
    ParamBundle,
    QnameInputs,
    RelationshipOutputTables,
    RuntimeInputs,
    ScipBuildInputs,
    ScipOccurrenceInputs,
    TreeSitterInputs,
    TypeInputs,
)
from ibis_engine.param_tables import (
    ParamTablePolicy,
    ParamTableRegistry,
    ParamTableSpec,
    param_table_name,
)
from ibis_engine.params_bridge import IbisParamRegistry, registry_from_specs, specs_from_rel_ops
from ibis_engine.plan import IbisPlan
from normalize.utils import encoding_policy_from_schema
from relspec.adapters import (
    CpgRuleAdapter,
    ExtractRuleAdapter,
    NormalizeRuleAdapter,
    RelspecRuleAdapter,
)
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
from relspec.compiler_graph import order_rules
from relspec.contracts import relation_output_contract
from relspec.edge_contract_validator import (
    EdgeContractValidationConfig,
    validate_relationship_output_contracts_for_edge_kinds,
)
from relspec.model import DatasetRef, RelationshipRule
from relspec.param_deps import RuleDependencyReport
from relspec.policies import evidence_spec_from_schema
from relspec.registry import ContractCatalog, DatasetCatalog, DatasetLocation
from relspec.rules.compiler import RuleCompiler
from relspec.rules.evidence import EvidenceCatalog
from relspec.rules.handlers.cpg import RelationshipRuleHandler
from relspec.rules.registry import RuleRegistry
from relspec.rules.validation import SqlGlotDiagnosticsConfig, rule_dependency_reports
from schema_spec.specs import ArrowFieldSpec, TableSchemaSpec, call_span_bundle, span_bundle
from schema_spec.system import (
    GLOBAL_SCHEMA_REGISTRY,
    ContractCatalogSpec,
    DataFusionScanOptions,
    DatasetSpec,
    DedupeSpecSpec,
    SchemaRegistry,
    SortKeySpec,
    TableSpecConstraints,
    VirtualFieldSpec,
    make_contract_spec,
    make_dataset_spec,
    make_table_spec,
    table_spec_from_schema,
)
from sqlglot_tools.bridge import IbisCompilerBackend

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable

# -----------------------------
# Relationship contracts
# -----------------------------

SCHEMA_VERSION = 1
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
    adapter_mode: AdapterMode
    execution_policy: AdapterExecutionPolicy
    ibis_backend: BaseBackend
    relspec_param_bindings: Mapping[IbisValue, object]


@tag(layer="relspec", artifact="relationship_execution_context", kind="object")
def relationship_execution_context(
    ctx: ExecutionContext,
    adapter_mode: AdapterMode,
    adapter_execution_policy: AdapterExecutionPolicy,
    ibis_backend: BaseBackend,
    relspec_param_bindings: Mapping[IbisValue, object],
) -> RelationshipExecutionContext:
    """Bundle execution settings for relationship table materialization.

    Returns
    -------
    RelationshipExecutionContext
        Execution settings for relationship output execution.
    """
    return RelationshipExecutionContext(
        ctx=ctx,
        adapter_mode=adapter_mode,
        execution_policy=adapter_execution_policy,
        ibis_backend=ibis_backend,
        relspec_param_bindings=relspec_param_bindings,
    )


def _datafusion_scan_options(
    name: str,
    *,
    table_spec: TableSchemaSpec | None,
    dataset_spec: DatasetSpec | None,
) -> DataFusionScanOptions:
    if dataset_spec is not None and dataset_spec.datafusion_scan is not None:
        return dataset_spec.datafusion_scan
    file_sort_order: tuple[str, ...] = ()
    if table_spec is not None and table_spec.key_fields:
        file_sort_order = tuple(table_spec.key_fields)
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
    rel_name_symbol_spec = GLOBAL_SCHEMA_REGISTRY.register_dataset(
        make_dataset_spec(
            table_spec=make_table_spec(
                name="rel_name_symbol_v1",
                version=SCHEMA_VERSION,
                bundles=(span_bundle(),),
                fields=[
                    ArrowFieldSpec(name="name_ref_id", dtype=pa.string()),
                    ArrowFieldSpec(name="symbol", dtype=pa.string()),
                    ArrowFieldSpec(name="symbol_roles", dtype=pa.int32()),
                    ArrowFieldSpec(name="path", dtype=pa.string()),
                    ArrowFieldSpec(name="resolution_method", dtype=pa.string()),
                    ArrowFieldSpec(name="confidence", dtype=pa.float32()),
                    ArrowFieldSpec(name="score", dtype=pa.float32()),
                    ArrowFieldSpec(name="rule_name", dtype=pa.string()),
                    ArrowFieldSpec(name="rule_priority", dtype=pa.int32()),
                ],
                constraints=TableSpecConstraints(required_non_null=("name_ref_id", "symbol")),
            )
        )
    )

    rel_import_symbol_spec = GLOBAL_SCHEMA_REGISTRY.register_dataset(
        make_dataset_spec(
            table_spec=make_table_spec(
                name="rel_import_symbol_v1",
                version=SCHEMA_VERSION,
                bundles=(span_bundle(),),
                fields=[
                    ArrowFieldSpec(name="import_alias_id", dtype=pa.string()),
                    ArrowFieldSpec(name="symbol", dtype=pa.string()),
                    ArrowFieldSpec(name="symbol_roles", dtype=pa.int32()),
                    ArrowFieldSpec(name="path", dtype=pa.string()),
                    ArrowFieldSpec(name="resolution_method", dtype=pa.string()),
                    ArrowFieldSpec(name="confidence", dtype=pa.float32()),
                    ArrowFieldSpec(name="score", dtype=pa.float32()),
                    ArrowFieldSpec(name="rule_name", dtype=pa.string()),
                    ArrowFieldSpec(name="rule_priority", dtype=pa.int32()),
                ],
                constraints=TableSpecConstraints(required_non_null=("import_alias_id", "symbol")),
            )
        )
    )

    rel_callsite_symbol_spec = GLOBAL_SCHEMA_REGISTRY.register_dataset(
        make_dataset_spec(
            table_spec=make_table_spec(
                name="rel_callsite_symbol_v1",
                version=SCHEMA_VERSION,
                bundles=(),
                fields=[
                    ArrowFieldSpec(name="call_id", dtype=pa.string()),
                    ArrowFieldSpec(name="symbol", dtype=pa.string()),
                    ArrowFieldSpec(name="symbol_roles", dtype=pa.int32()),
                    ArrowFieldSpec(name="path", dtype=pa.string()),
                    *call_span_bundle().fields,
                    ArrowFieldSpec(name="resolution_method", dtype=pa.string()),
                    ArrowFieldSpec(name="confidence", dtype=pa.float32()),
                    ArrowFieldSpec(name="score", dtype=pa.float32()),
                    ArrowFieldSpec(name="rule_name", dtype=pa.string()),
                    ArrowFieldSpec(name="rule_priority", dtype=pa.int32()),
                ],
                constraints=TableSpecConstraints(required_non_null=("call_id", "symbol")),
            )
        )
    )

    rel_callsite_qname_spec = GLOBAL_SCHEMA_REGISTRY.register_dataset(
        make_dataset_spec(
            table_spec=make_table_spec(
                name="rel_callsite_qname_v1",
                version=SCHEMA_VERSION,
                bundles=(),
                fields=[
                    ArrowFieldSpec(name="call_id", dtype=pa.string()),
                    ArrowFieldSpec(name="qname_id", dtype=pa.string()),
                    ArrowFieldSpec(name="qname_source", dtype=pa.string()),
                    ArrowFieldSpec(name="path", dtype=pa.string()),
                    *call_span_bundle().fields,
                    ArrowFieldSpec(name="confidence", dtype=pa.float32()),
                    ArrowFieldSpec(name="score", dtype=pa.float32()),
                    ArrowFieldSpec(name="ambiguity_group_id", dtype=pa.string()),
                    ArrowFieldSpec(name="rule_name", dtype=pa.string()),
                    ArrowFieldSpec(name="rule_priority", dtype=pa.int32()),
                ],
                constraints=TableSpecConstraints(required_non_null=("call_id", "qname_id")),
            )
        )
    )

    return ContractCatalogSpec(
        contracts={
            "rel_name_symbol_v1": make_contract_spec(
                table_spec=rel_name_symbol_spec.table_spec,
                virtual=VirtualFieldSpec(fields=("origin",)),
                dedupe=DedupeSpecSpec(
                    keys=("name_ref_id", "symbol", "path", "bstart", "bend"),
                    tie_breakers=(
                        SortKeySpec(column="score", order="descending"),
                        SortKeySpec(column="confidence", order="descending"),
                        SortKeySpec(column="rule_priority", order="ascending"),
                    ),
                    strategy="KEEP_FIRST_AFTER_SORT",
                ),
                canonical_sort=(
                    SortKeySpec(column="path", order="ascending"),
                    SortKeySpec(column="bstart", order="ascending"),
                    SortKeySpec(column="name_ref_id", order="ascending"),
                ),
                version=SCHEMA_VERSION,
            ),
            "rel_import_symbol_v1": make_contract_spec(
                table_spec=rel_import_symbol_spec.table_spec,
                virtual=VirtualFieldSpec(fields=("origin",)),
                dedupe=DedupeSpecSpec(
                    keys=("import_alias_id", "symbol", "path", "bstart", "bend"),
                    tie_breakers=(
                        SortKeySpec(column="score", order="descending"),
                        SortKeySpec(column="rule_priority", order="ascending"),
                    ),
                    strategy="KEEP_FIRST_AFTER_SORT",
                ),
                canonical_sort=(
                    SortKeySpec(column="path", order="ascending"),
                    SortKeySpec(column="bstart", order="ascending"),
                    SortKeySpec(column="import_alias_id", order="ascending"),
                ),
                version=SCHEMA_VERSION,
            ),
            "rel_callsite_symbol_v1": make_contract_spec(
                table_spec=rel_callsite_symbol_spec.table_spec,
                virtual=VirtualFieldSpec(fields=("origin",)),
                dedupe=DedupeSpecSpec(
                    keys=("call_id", "symbol", "path", "call_bstart", "call_bend"),
                    tie_breakers=(
                        SortKeySpec(column="score", order="descending"),
                        SortKeySpec(column="rule_priority", order="ascending"),
                    ),
                    strategy="KEEP_FIRST_AFTER_SORT",
                ),
                canonical_sort=(
                    SortKeySpec(column="path", order="ascending"),
                    SortKeySpec(column="call_bstart", order="ascending"),
                    SortKeySpec(column="call_id", order="ascending"),
                ),
                version=SCHEMA_VERSION,
            ),
            "rel_callsite_qname_v1": make_contract_spec(
                table_spec=rel_callsite_qname_spec.table_spec,
                virtual=VirtualFieldSpec(fields=("origin",)),
                dedupe=DedupeSpecSpec(
                    keys=("call_id", "qname_id"),
                    tie_breakers=(
                        SortKeySpec(column="score", order="descending"),
                        SortKeySpec(column="rule_priority", order="ascending"),
                    ),
                    strategy="KEEP_FIRST_AFTER_SORT",
                ),
                canonical_sort=(
                    SortKeySpec(column="call_id", order="ascending"),
                    SortKeySpec(column="qname_id", order="ascending"),
                ),
                version=SCHEMA_VERSION,
            ),
        }
    )


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


@tag(layer="relspec", artifact="schema_registry", kind="registry")
def schema_registry(
    relationship_contract_spec: ContractCatalogSpec,
) -> SchemaRegistry:
    """Build a schema registry with CPG and relationship specs.

    Returns
    -------
    SchemaRegistry
        Registry containing table and contract specs.
    """
    registry = GLOBAL_SCHEMA_REGISTRY
    register_cpg_specs(registry)
    return relationship_contract_spec.register_into(registry)


# -----------------------------
# Relationship rules
# -----------------------------


@tag(layer="relspec", artifact="rule_registry", kind="registry")
def rule_registry(
    param_table_specs: tuple[ParamTableSpec, ...],
    param_table_policy: ParamTablePolicy,
) -> RuleRegistry:
    """Build the central rule registry.

    Returns
    -------
    RuleRegistry
        Centralized registry for CPG/normalize/extract rules.
    """
    return RuleRegistry(
        adapters=(
            CpgRuleAdapter(),
            RelspecRuleAdapter(),
            NormalizeRuleAdapter(),
            ExtractRuleAdapter(),
        ),
        param_table_specs=param_table_specs,
        param_table_policy=param_table_policy,
    )


@tag(layer="relspec", artifact="relspec_param_registry", kind="object")
def relspec_param_registry(rule_registry: RuleRegistry) -> IbisParamRegistry:
    """Build the relspec parameter registry from rule definitions.

    Returns
    -------
    IbisParamRegistry
        Registry populated with parameter specs from rule rel_ops.
    """
    specs = []
    for rule in rule_registry.rules_for_domain("cpg"):
        specs.extend(specs_from_rel_ops(rule.rel_ops))
    return registry_from_specs(specs)


@tag(layer="relspec", artifact="relspec_param_dependency_reports", kind="object")
def relspec_param_dependency_reports(
    rule_registry: RuleRegistry,
    ibis_backend: BaseBackend,
    param_table_specs: tuple[ParamTableSpec, ...],
    param_table_policy: ParamTablePolicy,
    ctx: ExecutionContext,
) -> tuple[RuleDependencyReport, ...]:
    """Return inferred param dependency reports for relspec rules.

    Returns
    -------
    tuple[RuleDependencyReport, ...]
        Dependency reports for relspec rules.
    """
    rules = rule_registry.rule_definitions()
    return rule_dependency_reports(
        rules,
        config=SqlGlotDiagnosticsConfig(
            backend=cast("IbisCompilerBackend", ibis_backend),
            registry=GLOBAL_SCHEMA_REGISTRY,
            ctx=ctx,
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


@tag(layer="relspec", artifact="relspec_cst_inputs", kind="bundle")
def relspec_cst_inputs(
    cst_name_refs: TableLike,
    cst_imports_norm: TableLike,
    cst_callsites: TableLike,
) -> CstRelspecInputs:
    """Bundle CST tables needed for relationship inputs.

    Returns
    -------
    CstRelspecInputs
        CST relationship input bundle.
    """
    return CstRelspecInputs(
        cst_name_refs=cst_name_refs,
        cst_imports_norm=cst_imports_norm,
        cst_callsites=cst_callsites,
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


def _require_table(name: str, value: TableLike | DatasetSource) -> TableLike:
    if isinstance(value, DatasetSource):
        msg = f"Relspec input {name!r} must be a table, not a dataset source."
        raise TypeError(msg)
    return value


@tag(layer="relspec", artifact="relspec_input_datasets", kind="bundle")
def relspec_input_datasets(
    relspec_cst_inputs: CstRelspecInputs,
    relspec_scip_inputs: ScipOccurrenceInputs,
    relspec_qname_inputs: QnameInputs,
    scip_build_inputs: ScipBuildInputs,
    cpg_extra_inputs: CpgExtraInputs,
) -> dict[str, TableLike]:
    """Build the canonical dataset-name to table mapping.

    Important: dataset keys must match the DatasetRef(...) names in ``rule_registry()``.

    Returns
    -------
    dict[str, TableLike]
        Mapping from dataset names to tables.
    """
    return {
        "cst_name_refs": relspec_cst_inputs.cst_name_refs,
        "cst_imports": relspec_cst_inputs.cst_imports_norm,
        "cst_callsites": relspec_cst_inputs.cst_callsites,
        "scip_occurrences": relspec_scip_inputs.scip_occurrences_norm,
        "scip_symbol_relationships": _require_table(
            "scip_symbol_relationships",
            scip_build_inputs.scip_symbol_relationships,
        ),
        "callsite_qname_candidates": relspec_qname_inputs.callsite_qname_candidates,
        "dim_qualified_names": relspec_qname_inputs.dim_qualified_names,
        "type_exprs_norm": _require_table("type_exprs_norm", cpg_extra_inputs.type_exprs_norm),
        "diagnostics_norm": _require_table(
            "diagnostics_norm",
            cpg_extra_inputs.diagnostics_norm,
        ),
        "rt_signatures": _require_table("rt_signatures", cpg_extra_inputs.rt_signatures),
        "rt_signature_params": _require_table(
            "rt_signature_params",
            cpg_extra_inputs.rt_signature_params,
        ),
        "rt_members": _require_table("rt_members", cpg_extra_inputs.rt_members),
    }


@cache()
@tag(layer="relspec", artifact="persisted_relspec_inputs", kind="object")
def persist_relspec_input_datasets(
    relspec_mode: Literal["memory", "filesystem"],
    relspec_input_datasets: dict[str, TableLike],
    relspec_input_dataset_dir: str,
    *,
    overwrite_intermediate_datasets: bool,
) -> dict[str, DatasetLocation]:
    """Write relationship input datasets to disk in filesystem mode.

    Returns mapping: dataset_name -> DatasetLocation for FilesystemPlanResolver.
    In memory mode, returns {} and performs no I/O.

    Returns
    -------
    dict[str, DatasetLocation]
        Dataset locations for persisted inputs.
    """
    mode = (relspec_mode or "memory").lower().strip()
    if mode != "filesystem":
        return {}

    # Write as Parquet dataset dirs so Acero scans can project/filter cheaply.
    schemas = {name: table.schema for name, table in relspec_input_datasets.items()}
    dataset_specs = {
        name: GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(name) for name in relspec_input_datasets
    }
    encoding_policies = {}
    for name, table in relspec_input_datasets.items():
        spec = dataset_specs.get(name)
        policy = spec.encoding_policy() if spec is not None else None
        encoding_policies[name] = policy or encoding_policy_from_schema(table.schema)
    paths = write_named_datasets_parquet(
        relspec_input_datasets,
        relspec_input_dataset_dir,
        config=NamedDatasetWriteConfig(
            opts=ParquetWriteOptions(),
            overwrite=bool(overwrite_intermediate_datasets),
            schemas=schemas,
            encoding_policies=encoding_policies,
        ),
    )

    out: dict[str, DatasetLocation] = {}
    for name, path in paths.items():
        table = relspec_input_datasets.get(name)
        table_spec = table_spec_from_schema(name, table.schema) if table is not None else None
        dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(name)
        if dataset_spec is None and table_spec is not None:
            dataset_spec = make_dataset_spec(table_spec=table_spec)
        out[name] = DatasetLocation(
            path=path,
            format="parquet",
            partitioning="hive",
            filesystem=None,
            table_spec=table_spec,
            dataset_spec=dataset_spec,
            datafusion_scan=_datafusion_scan_options(
                name,
                table_spec=table_spec,
                dataset_spec=dataset_spec,
            ),
            datafusion_provider="listing",
        )
    return out


@tag(layer="relspec", artifact="relspec_resolver", kind="runtime")
def relspec_resolver(
    relspec_mode: Literal["memory", "filesystem"],
    relspec_input_datasets: dict[str, TableLike],
    persist_relspec_input_datasets: dict[str, DatasetLocation],
    ibis_backend: BaseBackend,
    relspec_resolver_context: RelspecResolverContext,
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
    mode = (relspec_mode or "memory").lower().strip()
    if relspec_resolver_context.param_table_registry is None:
        param_tables: Mapping[str, IbisTable] = {}
        policy = ParamTablePolicy()
    else:
        param_tables = relspec_resolver_context.param_table_registry.ibis_tables(ibis_backend)
        policy = relspec_resolver_context.param_table_registry.policy
    param_aliases = _param_table_aliases(param_tables, policy=policy)
    if mode == "filesystem":
        cat = DatasetCatalog()
        for name, loc in persist_relspec_input_datasets.items():
            cat.register(name, loc)
        base = FilesystemPlanResolver(
            cat,
            backend=ibis_backend,
            runtime_profile=relspec_resolver_context.ctx.runtime.datafusion,
        )
        if not param_aliases:
            return base
        param_resolver = InMemoryPlanResolver(param_aliases, backend=ibis_backend)
        return _CompositePlanResolver(
            primary=param_resolver,
            fallback=base,
            primary_names=frozenset(param_aliases),
        )

    combined: dict[str, TableLike | IbisPlan | IbisTable] = dict(relspec_input_datasets)
    for name, table in param_aliases.items():
        combined.setdefault(name, table)
    return InMemoryPlanResolver(combined, backend=ibis_backend)


def _relationship_rule_schema(
    rule: RelationshipRule,
    *,
    contracts: ContractCatalog | None,
) -> SchemaLike | None:
    if rule.contract_name is None:
        return None
    if contracts is not None:
        contract = contracts.get(rule.contract_name)
        dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(contract.name)
        if dataset_spec is not None:
            return dataset_spec.schema()
        return contract.schema
    dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(rule.contract_name)
    if dataset_spec is not None:
        return dataset_spec.schema()
    return None


def _resolve_relationship_rules(
    rules: Sequence[RelationshipRule],
    *,
    contracts: ContractCatalog | None,
) -> tuple[RelationshipRule, ...]:
    resolved: list[RelationshipRule] = []
    for rule in rules:
        schema = _relationship_rule_schema(rule, contracts=contracts)
        updated = rule
        if schema is not None:
            updated = apply_policy_defaults(rule, schema)
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


def _runtime_telemetry(ctx: ExecutionContext) -> Mapping[str, object]:
    profile = ctx.runtime.datafusion
    if profile is None:
        return {}
    return {"datafusion": profile.telemetry_payload()}


def _param_table_aliases(
    param_tables: Mapping[str, IbisTable],
    *,
    policy: ParamTablePolicy,
) -> dict[str, IbisTable]:
    aliases: dict[str, IbisTable] = {}
    for logical_name, table in param_tables.items():
        table_name = param_table_name(policy, logical_name)
        names = (
            logical_name,
            table_name,
            f"{policy.schema}.{table_name}",
            f"{policy.catalog}.{policy.schema}.{table_name}",
        )
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
    for out_name, output_rules in by_output.items():
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
@tag(layer="relspec", artifact="compiled_relationship_outputs", kind="object")
def compiled_relationship_outputs(
    rule_registry: RuleRegistry,
    relspec_input_datasets: dict[str, TableLike],
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
    rule_compiler = RuleCompiler(handlers={"cpg": RelationshipRuleHandler()})
    compiled = rule_compiler.compile_rules(rule_registry.rules_for_domain("cpg"), ctx=ctx)
    rules = cast("tuple[RelationshipRule, ...]", compiled)
    resolved = _resolve_relationship_rules(rules, contracts=relationship_contracts)
    contract_names = set(relationship_contracts.names())
    resolved = tuple(
        rule
        for rule in resolved
        if rule.contract_name is not None and rule.contract_name in contract_names
    )
    evidence = _relationship_evidence_catalog(relspec_input_datasets)
    ordered = order_rules(resolved, evidence=evidence)
    return _compile_relationship_outputs(
        ordered,
        compiler=compiler,
        ctx=ctx,
        contracts=relationship_contracts,
        edge_validation=EdgeContractValidationConfig(),
    )


@cache()
@extract_fields(
    {
        "rel_name_symbol": TableLike,
        "rel_import_symbol": TableLike,
        "rel_callsite_symbol": TableLike,
        "rel_callsite_qname": TableLike,
    }
)
@tag(layer="relspec", artifact="relationship_tables", kind="bundle")
def relationship_tables(
    compiled_relationship_outputs: dict[str, CompiledOutput],
    relspec_resolver: PlanResolver[IbisPlan],
    relationship_contracts: ContractCatalog,
    relationship_execution_context: RelationshipExecutionContext,
) -> dict[str, TableLike]:
    """Execute compiled relationship outputs into tables.

    Returns
    -------
    dict[str, TableLike]
        Relationship tables keyed by output dataset.
    """
    exec_ctx = relationship_execution_context.ctx
    adapter_mode = relationship_execution_context.adapter_mode
    execution_policy = relationship_execution_context.execution_policy
    ibis_backend = relationship_execution_context.ibis_backend
    relspec_param_bindings = relationship_execution_context.relspec_param_bindings

    def _executor(
        plan: IbisPlan,
        exec_ctx: ExecutionContext,
        params: Mapping[IbisValue, object] | None,
        execution_label: ExecutionLabel | None = None,
    ) -> TableLike:
        result = run_plan_adapter(
            plan,
            ctx=exec_ctx,
            options=AdapterRunOptions(
                adapter_mode=adapter_mode,
                prefer_reader=False,
                execution_policy=execution_policy,
                execution_label=execution_label,
                ibis_backend=ibis_backend,
                ibis_params=params,
            ),
        )
        return cast("TableLike", result.value)

    out: dict[str, TableLike] = {}
    dynamic_outputs: dict[str, TableLike] = {}
    exec_options = CompiledOutputExecutionOptions(
        contracts=relationship_contracts,
        params=relspec_param_bindings,
        plan_executor=_executor,
        adapter_mode=adapter_mode,
        execution_policy=execution_policy,
        ibis_backend=ibis_backend,
    )
    for key, compiled in compiled_relationship_outputs.items():
        resolver = relspec_resolver
        if dynamic_outputs:
            dynamic_resolver = InMemoryPlanResolver(dynamic_outputs, backend=ibis_backend)
            resolver = _CompositePlanResolver(
                primary=dynamic_resolver,
                fallback=relspec_resolver,
                primary_names=frozenset(dynamic_outputs),
            )
        res = compiled.execute(
            ctx=exec_ctx,
            resolver=resolver,
            options=exec_options,
        )
        out[key] = res.good
        dynamic_outputs[key] = res.good

    # Ensure expected keys exist for extract_fields
    out.setdefault(
        "rel_name_symbol",
        empty_table(relationship_contracts.get("rel_name_symbol_v1").schema),
    )
    out.setdefault(
        "rel_import_symbol",
        empty_table(relationship_contracts.get("rel_import_symbol_v1").schema),
    )
    out.setdefault(
        "rel_callsite_symbol",
        empty_table(relationship_contracts.get("rel_callsite_symbol_v1").schema),
    )
    out.setdefault(
        "rel_callsite_qname",
        empty_table(relationship_contracts.get("rel_callsite_qname_v1").schema),
    )
    return out


@tag(layer="relspec", artifact="relationship_output_tables", kind="bundle")
def relationship_output_tables(
    rel_name_symbol: TableLike,
    rel_import_symbol: TableLike,
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
        rel_callsite_symbol=rel_callsite_symbol,
        rel_callsite_qname=rel_callsite_qname,
    )


# -----------------------------
# CPG build
# -----------------------------


@tag(layer="cpg", artifact="cst_build_inputs", kind="bundle")
def cst_build_inputs(
    cst_name_refs: TableLike | DatasetSource,
    cst_imports_norm: TableLike | DatasetSource,
    cst_callsites: TableLike | DatasetSource,
    cst_defs_norm: TableLike | DatasetSource,
) -> CstBuildInputs:
    """Bundle CST inputs for CPG builds.

    Returns
    -------
    CstBuildInputs
        CST build input bundle.
    """
    return CstBuildInputs(
        cst_name_refs=cst_name_refs,
        cst_imports_norm=cst_imports_norm,
        cst_callsites=cst_callsites,
        cst_defs_norm=cst_defs_norm,
    )


@tag(layer="cpg", artifact="scip_build_inputs", kind="bundle")
def scip_build_inputs(
    scip_symbol_information: TableLike | DatasetSource,
    scip_occurrences_norm: TableLike | DatasetSource,
    scip_symbol_relationships: TableLike | DatasetSource,
    scip_external_symbol_information: TableLike | DatasetSource,
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


@tag(layer="cpg", artifact="cpg_base_inputs", kind="bundle")
def cpg_base_inputs(
    repo_files: TableLike | DatasetSource,
    dim_qualified_names: TableLike | DatasetSource,
    cst_build_inputs: CstBuildInputs,
    scip_build_inputs: ScipBuildInputs,
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
    )


@tag(layer="cpg", artifact="tree_sitter_inputs", kind="bundle")
def tree_sitter_inputs(
    ts_nodes: TableLike | DatasetSource,
    ts_errors: TableLike | DatasetSource,
    ts_missing: TableLike | DatasetSource,
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
    type_exprs_norm: TableLike | DatasetSource,
    types_norm: TableLike | DatasetSource,
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
    diagnostics_norm: TableLike | DatasetSource,
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
    rt_objects: TableLike | DatasetSource,
    rt_signatures: TableLike | DatasetSource,
    rt_signature_params: TableLike | DatasetSource,
    rt_members: TableLike | DatasetSource,
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
        cst_name_refs=cpg_base_inputs.cst_build_inputs.cst_name_refs,
        cst_imports=cpg_base_inputs.cst_build_inputs.cst_imports_norm,
        cst_callsites=cpg_base_inputs.cst_build_inputs.cst_callsites,
        cst_defs=cpg_base_inputs.cst_build_inputs.cst_defs_norm,
        dim_qualified_names=cpg_base_inputs.dim_qualified_names,
        scip_symbol_information=cpg_base_inputs.scip_build_inputs.scip_symbol_information,
        scip_occurrences=cpg_base_inputs.scip_build_inputs.scip_occurrences_norm,
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
    )


@tag(layer="cpg", artifact="cpg_props_inputs", kind="bundle")
def cpg_props_inputs(
    cpg_base_inputs: CpgBaseInputs,
    cpg_extra_inputs: CpgExtraInputs,
    cpg_edges_final: TableLike,
) -> PropsInputTables:
    """Build property input tables from base and optional inputs.

    Returns
    -------
    PropsInputTables
        Property input table bundle.
    """
    return PropsInputTables(
        repo_files=cpg_base_inputs.repo_files,
        cst_name_refs=cpg_base_inputs.cst_build_inputs.cst_name_refs,
        cst_imports=cpg_base_inputs.cst_build_inputs.cst_imports_norm,
        cst_callsites=cpg_base_inputs.cst_build_inputs.cst_callsites,
        cst_defs=cpg_base_inputs.cst_build_inputs.cst_defs_norm,
        dim_qualified_names=cpg_base_inputs.dim_qualified_names,
        scip_symbol_information=cpg_base_inputs.scip_build_inputs.scip_symbol_information,
        scip_occurrences=cpg_base_inputs.scip_build_inputs.scip_occurrences_norm,
        scip_external_symbol_information=(
            cpg_base_inputs.scip_build_inputs.scip_external_symbol_information
        ),
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
        cpg_edges=cpg_edges_final,
    )


@cache(format="parquet")
@tag(layer="cpg", artifact="cpg_nodes_final", kind="table")
def cpg_nodes_final(
    cpg_nodes_finalize: CpgBuildArtifacts,
) -> TableLike:
    """Build the final CPG nodes table.

    Returns
    -------
    TableLike
        Final CPG nodes table.
    """
    return cpg_nodes_finalize.finalize.good


@cache()
@tag(layer="cpg", artifact="cpg_nodes_finalize", kind="object")
def cpg_nodes_finalize(
    ctx: ExecutionContext,
    cpg_node_inputs: NodeInputTables,
) -> CpgBuildArtifacts:
    """Build finalized CPG nodes with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalized nodes bundle plus quality table.
    """
    return build_cpg_nodes(ctx=ctx, inputs=cpg_node_inputs)


@cache(format="parquet")
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


@cache(format="parquet")
@tag(layer="cpg", artifact="cpg_edges_final", kind="table")
def cpg_edges_final(
    cpg_edges_finalize: CpgBuildArtifacts,
) -> TableLike:
    """Build the final CPG edges table.

    Returns
    -------
    TableLike
        Final CPG edges table.
    """
    return cpg_edges_finalize.finalize.good


@cache()
@tag(layer="cpg", artifact="cpg_edges_finalize", kind="object")
def cpg_edges_finalize(
    ctx: ExecutionContext,
    cpg_edge_inputs: EdgeBuildInputs,
    adapter_mode: AdapterMode,
    adapter_execution_policy: AdapterExecutionPolicy,
    ibis_backend: BaseBackend,
) -> CpgBuildArtifacts:
    """Build finalized CPG edges with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalized edges bundle plus quality table.
    """
    return build_cpg_edges(
        ctx=ctx,
        config=EdgeBuildConfig(
            inputs=cpg_edge_inputs,
            adapter_mode=adapter_mode,
            execution_policy=adapter_execution_policy,
            ibis_backend=ibis_backend,
        ),
    )


@cache(format="parquet")
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


@cache(format="parquet")
@tag(layer="cpg", artifact="cpg_props_final", kind="table")
def cpg_props_final(
    cpg_props_finalize: CpgBuildArtifacts,
) -> TableLike:
    """Build the final CPG properties table.

    Returns
    -------
    TableLike
        Final CPG properties table.
    """
    return cpg_props_finalize.finalize.good


@cache()
@tag(layer="cpg", artifact="cpg_props_finalize", kind="object")
def cpg_props_finalize(
    ctx: ExecutionContext,
    cpg_props_inputs: PropsInputTables,
) -> CpgBuildArtifacts:
    """Build finalized CPG properties with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalized properties bundle plus quality table.
    """
    return build_cpg_props(ctx=ctx, inputs=cpg_props_inputs)


@cache(format="parquet")
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
