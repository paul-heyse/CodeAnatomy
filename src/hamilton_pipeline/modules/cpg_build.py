"""Hamilton CPG build stage and relationship rule wiring."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
from hamilton.function_modifiers import cache, extract_fields, tag

from arrowdsl.contracts import Contract, DedupeSpec, SortKey
from arrowdsl.runtime import ExecutionContext
from cpg.build_edges import EdgeBuildInputs, build_cpg_edges
from cpg.build_nodes import NodeInputTables, build_cpg_nodes
from cpg.build_props import PropsInputTables, build_cpg_props
from hamilton_pipeline.pipeline_types import (
    CpgBaseInputs,
    CpgExtraInputs,
    CstBuildInputs,
    CstRelspecInputs,
    DiagnosticsInputs,
    QnameInputs,
    RelationshipOutputTables,
    RuntimeInputs,
    ScipBuildInputs,
    ScipOccurrenceInputs,
    TreeSitterInputs,
    TypeInputs,
)
from relspec.compiler import (
    CompiledOutput,
    FilesystemPlanResolver,
    InMemoryPlanResolver,
    PlanResolver,
    RelationshipRuleCompiler,
)
from relspec.edge_contract_validator import EdgeContractValidationConfig
from relspec.model import (
    DatasetRef,
    HashJoinConfig,
    IntervalAlignConfig,
    RelationshipRule,
    RuleKind,
)
from relspec.registry import (
    ContractCatalog,
    DatasetCatalog,
    DatasetLocation,
    RelationshipRegistry,
)
from storage.parquet import ParquetWriteOptions, write_named_datasets_parquet

# -----------------------------
# Relationship contracts
# -----------------------------


@tag(layer="relspec", artifact="relationship_contracts", kind="catalog")
def relationship_contracts() -> ContractCatalog:
    """Build output contracts for relationship datasets.

    Returns
    -------
    ContractCatalog
        Contract catalog for relationship outputs.
    """
    cc = ContractCatalog()

    rel_name_symbol_schema = pa.schema(
        [
            ("name_ref_id", pa.string()),
            ("symbol", pa.string()),
            ("symbol_roles", pa.int32()),
            ("path", pa.string()),
            ("bstart", pa.int64()),
            ("bend", pa.int64()),
            ("resolution_method", pa.string()),
            ("confidence", pa.float32()),
            ("score", pa.float32()),
            ("rule_name", pa.string()),
            ("rule_priority", pa.int32()),
        ]
    )

    rel_import_symbol_schema = pa.schema(
        [
            ("import_alias_id", pa.string()),
            ("symbol", pa.string()),
            ("symbol_roles", pa.int32()),
            ("path", pa.string()),
            ("bstart", pa.int64()),
            ("bend", pa.int64()),
            ("resolution_method", pa.string()),
            ("confidence", pa.float32()),
            ("score", pa.float32()),
            ("rule_name", pa.string()),
            ("rule_priority", pa.int32()),
        ]
    )

    rel_callsite_symbol_schema = pa.schema(
        [
            ("call_id", pa.string()),
            ("symbol", pa.string()),
            ("symbol_roles", pa.int32()),
            ("path", pa.string()),
            ("call_bstart", pa.int64()),
            ("call_bend", pa.int64()),
            ("resolution_method", pa.string()),
            ("confidence", pa.float32()),
            ("score", pa.float32()),
            ("rule_name", pa.string()),
            ("rule_priority", pa.int32()),
        ]
    )

    rel_callsite_qname_schema = pa.schema(
        [
            ("call_id", pa.string()),
            ("qname_id", pa.string()),
            ("qname_source", pa.string()),
            ("path", pa.string()),
            ("call_bstart", pa.int64()),
            ("call_bend", pa.int64()),
            ("confidence", pa.float32()),
            ("score", pa.float32()),
            ("ambiguity_group_id", pa.string()),
            ("rule_name", pa.string()),
            ("rule_priority", pa.int32()),
        ]
    )

    cc.register(
        Contract(
            name="rel_name_symbol_v1",
            schema=rel_name_symbol_schema,
            required_non_null=("name_ref_id", "symbol"),
            virtual_fields=("origin",),  # NEW: injected downstream by edge emitter
            dedupe=DedupeSpec(
                keys=("name_ref_id", "symbol", "path", "bstart", "bend"),
                tie_breakers=(
                    SortKey("score", "descending"),
                    SortKey("confidence", "descending"),
                    SortKey("rule_priority", "ascending"),
                ),
                strategy="KEEP_FIRST_AFTER_SORT",
            ),
            canonical_sort=(
                SortKey("path", "ascending"),
                SortKey("bstart", "ascending"),
                SortKey("name_ref_id", "ascending"),
            ),
        )
    )

    cc.register(
        Contract(
            name="rel_import_symbol_v1",
            schema=rel_import_symbol_schema,
            required_non_null=("import_alias_id", "symbol"),
            virtual_fields=("origin",),  # NEW: injected downstream by edge emitter
            dedupe=DedupeSpec(
                keys=("import_alias_id", "symbol", "path", "bstart", "bend"),
                tie_breakers=(
                    SortKey("score", "descending"),
                    SortKey("rule_priority", "ascending"),
                ),
                strategy="KEEP_FIRST_AFTER_SORT",
            ),
            canonical_sort=(
                SortKey("path", "ascending"),
                SortKey("bstart", "ascending"),
                SortKey("import_alias_id", "ascending"),
            ),
        )
    )

    cc.register(
        Contract(
            name="rel_callsite_symbol_v1",
            schema=rel_callsite_symbol_schema,
            required_non_null=("call_id", "symbol"),
            virtual_fields=("origin",),  # NEW: injected downstream by edge emitter
            dedupe=DedupeSpec(
                keys=("call_id", "symbol", "path", "call_bstart", "call_bend"),
                tie_breakers=(
                    SortKey("score", "descending"),
                    SortKey("rule_priority", "ascending"),
                ),
                strategy="KEEP_FIRST_AFTER_SORT",
            ),
            canonical_sort=(
                SortKey("path", "ascending"),
                SortKey("call_bstart", "ascending"),
                SortKey("call_id", "ascending"),
            ),
        )
    )

    cc.register(
        Contract(
            name="rel_callsite_qname_v1",
            schema=rel_callsite_qname_schema,
            required_non_null=("call_id", "qname_id"),
            virtual_fields=("origin",),  # NEW: injected downstream by edge emitter
            dedupe=DedupeSpec(
                keys=("call_id", "qname_id"),
                tie_breakers=(
                    SortKey("score", "descending"),
                    SortKey("rule_priority", "ascending"),
                ),
                strategy="KEEP_FIRST_AFTER_SORT",
            ),
            canonical_sort=(SortKey("call_id", "ascending"), SortKey("qname_id", "ascending")),
        )
    )

    return cc


# -----------------------------
# Relationship rules
# -----------------------------


@tag(layer="relspec", artifact="relationship_registry", kind="registry")
def relationship_registry() -> RelationshipRegistry:
    """Build the relationship rule registry.

    Returns
    -------
    RelationshipRegistry
        Registry containing relationship rules.
    """
    reg = RelationshipRegistry()

    # 1) CST name refs ↔ SCIP occurrences (span align)
    reg.add(
        RelationshipRule(
            name="cst_name_refs__to__scip_occurrences",
            kind=RuleKind.INTERVAL_ALIGN,
            output_dataset="rel_name_symbol",
            contract_name="rel_name_symbol_v1",
            inputs=(DatasetRef("cst_name_refs"), DatasetRef("scip_occurrences")),
            interval_align=IntervalAlignConfig(
                mode="CONTAINED_BEST",
                how="inner",
                left_path_col="path",
                left_start_col="bstart",
                left_end_col="bend",
                right_path_col="path",
                right_start_col="bstart",
                right_end_col="bend",
                select_left=("name_ref_id", "path", "bstart", "bend"),
                select_right=("symbol", "symbol_roles"),
                emit_match_meta=False,
            ),
            priority=0,
        )
    )

    # 2) CST imports ↔ SCIP occurrences (span align)
    reg.add(
        RelationshipRule(
            name="cst_imports__to__scip_occurrences",
            kind=RuleKind.INTERVAL_ALIGN,
            output_dataset="rel_import_symbol",
            contract_name="rel_import_symbol_v1",
            inputs=(DatasetRef("cst_imports"), DatasetRef("scip_occurrences")),
            interval_align=IntervalAlignConfig(
                mode="CONTAINED_BEST",
                how="inner",
                left_path_col="path",
                left_start_col="bstart",
                left_end_col="bend",
                right_path_col="path",
                right_start_col="bstart",
                right_end_col="bend",
                select_left=("import_alias_id", "path", "bstart", "bend"),
                select_right=("symbol", "symbol_roles"),
                emit_match_meta=False,
            ),
            priority=0,
        )
    )

    # 3) CST callsites ↔ SCIP occurrences (callee span align)
    reg.add(
        RelationshipRule(
            name="cst_callsites__callee_to__scip_occurrences",
            kind=RuleKind.INTERVAL_ALIGN,
            output_dataset="rel_callsite_symbol",
            contract_name="rel_callsite_symbol_v1",
            inputs=(DatasetRef("cst_callsites"), DatasetRef("scip_occurrences")),
            interval_align=IntervalAlignConfig(
                mode="CONTAINED_BEST",
                how="inner",
                left_path_col="path",
                left_start_col="callee_bstart",
                left_end_col="callee_bend",
                right_path_col="path",
                right_start_col="bstart",
                right_end_col="bend",
                select_left=("call_id", "path", "call_bstart", "call_bend"),
                select_right=("symbol", "symbol_roles"),
                emit_match_meta=False,
            ),
            priority=0,
        )
    )

    # 4) callsite qname candidate fallback (hash join)
    reg.add(
        RelationshipRule(
            name="callsite_qname_candidates__join__dim_qualified_names",
            kind=RuleKind.HASH_JOIN,
            output_dataset="rel_callsite_qname",
            contract_name="rel_callsite_qname_v1",
            inputs=(DatasetRef("callsite_qname_candidates"), DatasetRef("dim_qualified_names")),
            hash_join=HashJoinConfig(
                join_type="inner",
                left_keys=("qname",),
                right_keys=("qname",),
                left_output=("call_id", "path", "call_bstart", "call_bend", "qname_source"),
                right_output=("qname_id",),
            ),
            priority=10,
        )
    )

    return reg


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
    cst_name_refs: pa.Table,
    cst_imports_norm: pa.Table,
    cst_callsites: pa.Table,
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
def relspec_scip_inputs(scip_occurrences_norm: pa.Table) -> ScipOccurrenceInputs:
    """Bundle SCIP occurrences for relationship inputs.

    Returns
    -------
    ScipOccurrenceInputs
        SCIP occurrence input bundle.
    """
    return ScipOccurrenceInputs(scip_occurrences_norm=scip_occurrences_norm)


@tag(layer="relspec", artifact="relspec_qname_inputs", kind="bundle")
def relspec_qname_inputs(
    callsite_qname_candidates: pa.Table,
    dim_qualified_names: pa.Table,
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


@tag(layer="relspec", artifact="relspec_input_datasets", kind="bundle")
def relspec_input_datasets(
    relspec_cst_inputs: CstRelspecInputs,
    relspec_scip_inputs: ScipOccurrenceInputs,
    relspec_qname_inputs: QnameInputs,
) -> dict[str, pa.Table]:
    """Build the canonical dataset-name to table mapping.

    Important: dataset keys must match the DatasetRef(...) names in relationship_registry().

    Returns
    -------
    dict[str, pa.Table]
        Mapping from dataset names to tables.
    """
    return {
        "cst_name_refs": relspec_cst_inputs.cst_name_refs,
        "cst_imports": relspec_cst_inputs.cst_imports_norm,
        "cst_callsites": relspec_cst_inputs.cst_callsites,
        "scip_occurrences": relspec_scip_inputs.scip_occurrences_norm,
        "callsite_qname_candidates": relspec_qname_inputs.callsite_qname_candidates,
        "dim_qualified_names": relspec_qname_inputs.dim_qualified_names,
    }


@cache()
@tag(layer="relspec", artifact="persisted_relspec_inputs", kind="object")
def persist_relspec_input_datasets(
    relspec_mode: str,
    relspec_input_datasets: dict[str, pa.Table],
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
    paths = write_named_datasets_parquet(
        relspec_input_datasets,
        relspec_input_dataset_dir,
        opts=ParquetWriteOptions(),
        overwrite=bool(overwrite_intermediate_datasets),
    )

    out: dict[str, DatasetLocation] = {}
    for name, path in paths.items():
        out[name] = DatasetLocation(
            path=path,
            format="parquet",
            partitioning="hive",
            filesystem=None,
        )
    return out


@tag(layer="relspec", artifact="relspec_resolver", kind="runtime")
def relspec_resolver(
    relspec_mode: str,
    relspec_input_datasets: dict[str, pa.Table],
    persist_relspec_input_datasets: dict[str, DatasetLocation],
) -> PlanResolver:
    """Select the relationship resolver implementation.

    Resolver selection:
      - memory      => InMemoryPlanResolver (tables already in memory)
      - filesystem  => FilesystemPlanResolver (tables scanned from Parquet datasets)

    Returns
    -------
    PlanResolver
        Resolver instance for relationship rule compilation.
    """
    mode = (relspec_mode or "memory").lower().strip()
    if mode == "filesystem":
        cat = DatasetCatalog()
        for name, loc in persist_relspec_input_datasets.items():
            cat.register(name, loc)
        return FilesystemPlanResolver(cat)

    return InMemoryPlanResolver(relspec_input_datasets)


# -----------------------------
# Relationship compilation + execution
# -----------------------------


@cache()
@tag(layer="relspec", artifact="compiled_relationship_outputs", kind="object")
def compiled_relationship_outputs(
    relationship_registry: RelationshipRegistry,
    relspec_resolver: PlanResolver,
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
    return compiler.compile_registry(
        relationship_registry.rules(),
        ctx=ctx,
        contract_catalog=relationship_contracts,
        edge_validation=EdgeContractValidationConfig(),  # optional override
    )


@cache()
@extract_fields(
    {
        "rel_name_symbol": pa.Table,
        "rel_import_symbol": pa.Table,
        "rel_callsite_symbol": pa.Table,
        "rel_callsite_qname": pa.Table,
    }
)
@tag(layer="relspec", artifact="relationship_tables", kind="bundle")
def relationship_tables(
    compiled_relationship_outputs: dict[str, CompiledOutput],
    relspec_resolver: PlanResolver,
    relationship_contracts: ContractCatalog,
    ctx: ExecutionContext,
) -> dict[str, pa.Table]:
    """Execute compiled relationship outputs into tables.

    Returns
    -------
    dict[str, pa.Table]
        Relationship tables keyed by output dataset.
    """
    out: dict[str, pa.Table] = {}
    for key, compiled in compiled_relationship_outputs.items():
        res = compiled.execute(ctx=ctx, resolver=relspec_resolver, contracts=relationship_contracts)
        out[key] = res.good

    # Ensure expected keys exist for extract_fields
    out.setdefault("rel_name_symbol", pa.Table.from_pylist([]))
    out.setdefault("rel_import_symbol", pa.Table.from_pylist([]))
    out.setdefault("rel_callsite_symbol", pa.Table.from_pylist([]))
    out.setdefault("rel_callsite_qname", pa.Table.from_pylist([]))
    return out


@tag(layer="relspec", artifact="relationship_output_tables", kind="bundle")
def relationship_output_tables(
    rel_name_symbol: pa.Table,
    rel_import_symbol: pa.Table,
    rel_callsite_symbol: pa.Table,
    rel_callsite_qname: pa.Table,
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
    cst_name_refs: pa.Table,
    cst_imports_norm: pa.Table,
    cst_callsites: pa.Table,
    cst_defs_norm: pa.Table,
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
    scip_symbol_information: pa.Table,
    scip_occurrences_norm: pa.Table,
    scip_external_symbol_information: pa.Table,
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
        scip_external_symbol_information=scip_external_symbol_information,
    )


@tag(layer="cpg", artifact="cpg_base_inputs", kind="bundle")
def cpg_base_inputs(
    repo_files: pa.Table,
    dim_qualified_names: pa.Table,
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
    ts_nodes: pa.Table,
    ts_errors: pa.Table,
    ts_missing: pa.Table,
) -> TreeSitterInputs:
    """Bundle tree-sitter inputs for CPG construction.

    Returns
    -------
    TreeSitterInputs
        Tree-sitter input bundle.
    """
    return TreeSitterInputs(ts_nodes=ts_nodes, ts_errors=ts_errors, ts_missing=ts_missing)


@tag(layer="cpg", artifact="type_inputs", kind="bundle")
def type_inputs(type_exprs_norm: pa.Table, types_norm: pa.Table) -> TypeInputs:
    """Bundle type inputs for CPG construction.

    Returns
    -------
    TypeInputs
        Type input bundle.
    """
    return TypeInputs(type_exprs_norm=type_exprs_norm, types_norm=types_norm)


@tag(layer="cpg", artifact="diagnostics_inputs", kind="bundle")
def diagnostics_inputs(diagnostics_norm: pa.Table) -> DiagnosticsInputs:
    """Bundle diagnostics inputs for CPG construction.

    Returns
    -------
    DiagnosticsInputs
        Diagnostics input bundle.
    """
    return DiagnosticsInputs(diagnostics_norm=diagnostics_norm)


@tag(layer="cpg", artifact="runtime_inputs", kind="bundle")
def runtime_inputs(
    rt_objects: pa.Table,
    rt_signatures: pa.Table,
    rt_signature_params: pa.Table,
    rt_members: pa.Table,
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
    cpg_edges_final: pa.Table,
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


@cache()
@tag(layer="cpg", artifact="cpg_nodes_final", kind="table")
def cpg_nodes_final(
    ctx: ExecutionContext,
    cpg_node_inputs: NodeInputTables,
) -> pa.Table:
    """Build the final CPG nodes table.

    Returns
    -------
    pa.Table
        Final CPG nodes table.
    """
    res = build_cpg_nodes(ctx=ctx, inputs=cpg_node_inputs)
    return res.good


@cache()
@tag(layer="cpg", artifact="cpg_edges_final", kind="table")
def cpg_edges_final(
    ctx: ExecutionContext,
    cpg_edge_inputs: EdgeBuildInputs,
) -> pa.Table:
    """Build the final CPG edges table.

    Returns
    -------
    pa.Table
        Final CPG edges table.
    """
    res = build_cpg_edges(ctx=ctx, inputs=cpg_edge_inputs)
    return res.good


@cache()
@tag(layer="cpg", artifact="cpg_props_final", kind="table")
def cpg_props_final(
    ctx: ExecutionContext,
    cpg_props_inputs: PropsInputTables,
) -> pa.Table:
    """Build the final CPG properties table.

    Returns
    -------
    pa.Table
        Final CPG properties table.
    """
    res = build_cpg_props(ctx=ctx, inputs=cpg_props_inputs)
    return res.good
