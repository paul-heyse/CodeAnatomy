from __future__ import annotations

import os
import pathlib
from typing import Any

import pyarrow as pa

from hamilton.function_modifiers import cache, extract_fields, tag

from ...arrowdsl.contracts import Contract, DedupeSpec, SortKey
from ...arrowdsl.runtime import ExecutionContext
from ...cpg.build_edges import build_cpg_edges
from ...cpg.build_nodes import build_cpg_nodes
from ...cpg.build_props import build_cpg_props
from ...relspec.compiler import (
    FilesystemPlanResolver,
    InMemoryPlanResolver,
    RelationshipRuleCompiler,
)
from ...relspec.model import (
    DatasetRef,
    HashJoinConfig,
    IntervalAlignConfig,
    RelationshipRule,
    RuleKind,
)
from ...relspec.registry import (
    ContractCatalog,
    DatasetCatalog,
    DatasetLocation,
    RelationshipRegistry,
)
from ...storage.parquet import ParquetWriteOptions, write_named_datasets_parquet

# -----------------------------
# Relationship contracts
# -----------------------------


@tag(layer="relspec", artifact="relationship_contracts", kind="catalog")
def relationship_contracts() -> ContractCatalog:
    """
    Output contracts for relationship datasets used by edge emission rules.
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
    """
    Choose a working directory for intermediate dataset materialization.

    Precedence:
      1) explicit work_dir
      2) output_dir
      3) local fallback: ./.codeintel_cpg_work
    """
    base = work_dir or output_dir or ".codeintel_cpg_work"
    pathlib.Path(base).mkdir(exist_ok=True, parents=True)
    return base


@tag(layer="relspec", artifact="relspec_input_dataset_dir", kind="scalar")
def relspec_input_dataset_dir(relspec_work_dir: str) -> str:
    """
    Where we write relationship-input datasets in filesystem resolver mode.
    """
    p = os.path.join(relspec_work_dir, "relspec_inputs")
    pathlib.Path(p).mkdir(exist_ok=True, parents=True)
    return p


@tag(layer="relspec", artifact="relspec_input_datasets", kind="bundle")
def relspec_input_datasets(
    # Use *normalized* variants where available
    cst_name_refs: pa.Table,
    cst_imports_norm: pa.Table,
    cst_callsites: pa.Table,
    scip_occurrences_norm: pa.Table,
    callsite_qname_candidates: pa.Table,
    dim_qualified_names: pa.Table,
) -> dict[str, pa.Table]:
    """
    Canonical mapping from dataset name -> table that relationship rules refer to.

    Important: dataset keys must match the DatasetRef(...) names in relationship_registry().
    """
    return {
        "cst_name_refs": cst_name_refs,
        "cst_imports": cst_imports_norm,
        "cst_callsites": cst_callsites,
        "scip_occurrences": scip_occurrences_norm,
        "callsite_qname_candidates": callsite_qname_candidates,
        "dim_qualified_names": dim_qualified_names,
    }


@cache()
@tag(layer="relspec", artifact="persisted_relspec_inputs", kind="object")
def persist_relspec_input_datasets(
    relspec_mode: str,
    relspec_input_datasets: dict[str, pa.Table],
    relspec_input_dataset_dir: str,
    overwrite_intermediate_datasets: bool,
) -> dict[str, DatasetLocation]:
    """
    Writes relspec input datasets to disk in filesystem mode.

    Returns mapping: dataset_name -> DatasetLocation for FilesystemPlanResolver.
    In memory mode, returns {} and performs no I/O.
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
) -> Any:
    """
    Resolver selection:
      - memory      => InMemoryPlanResolver (tables already in memory)
      - filesystem  => FilesystemPlanResolver (tables scanned from Parquet datasets)
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
    relspec_resolver: Any,
    relationship_contracts: ContractCatalog,  # add dep
    ctx: ExecutionContext,
) -> dict[str, Any]:
    compiler = RelationshipRuleCompiler(resolver=relspec_resolver)
    return compiler.compile_registry(
        relationship_registry.rules(),
        ctx=ctx,
        contracts=relationship_contracts,
        edge_validation=EdgeContractValidationConfig(),  # optional override
    )


@cache()
@extract_fields(
    rel_name_symbol=pa.Table,
    rel_import_symbol=pa.Table,
    rel_callsite_symbol=pa.Table,
    rel_callsite_qname=pa.Table,
)
@tag(layer="relspec", artifact="relationship_tables", kind="bundle")
def relationship_tables(
    compiled_relationship_outputs: dict[str, Any],
    relspec_resolver: Any,
    relationship_contracts: ContractCatalog,
    ctx: ExecutionContext,
) -> dict[str, pa.Table]:
    out: dict[str, pa.Table] = {}
    for key, compiled in compiled_relationship_outputs.items():
        res = compiled.execute(ctx=ctx, resolver=relspec_resolver, contracts=relationship_contracts)
        out[key] = res.table

    # Ensure expected keys exist for extract_fields
    out.setdefault("rel_name_symbol", pa.Table.from_pylist([]))
    out.setdefault("rel_import_symbol", pa.Table.from_pylist([]))
    out.setdefault("rel_callsite_symbol", pa.Table.from_pylist([]))
    out.setdefault("rel_callsite_qname", pa.Table.from_pylist([]))
    return out


# -----------------------------
# CPG build
# -----------------------------


@cache()
@tag(layer="cpg", artifact="cpg_nodes_final", kind="table")
def cpg_nodes_final(
    ctx: ExecutionContext,
    repo_files: pa.Table,
    cst_name_refs: pa.Table,
    cst_imports_norm: pa.Table,
    cst_callsites: pa.Table,
    cst_defs_norm: pa.Table,
    dim_qualified_names: pa.Table,
    scip_symbol_information: pa.Table,
    scip_occurrences_norm: pa.Table,
) -> pa.Table:
    res = build_cpg_nodes(
        ctx=ctx,
        repo_files=repo_files,
        cst_name_refs=cst_name_refs,
        cst_imports=cst_imports_norm,
        cst_callsites=cst_callsites,
        cst_defs=cst_defs_norm,
        dim_qualified_names=dim_qualified_names,
        scip_symbol_information=scip_symbol_information,
        scip_occurrences=scip_occurrences_norm,
    )
    return res.table


@cache()
@tag(layer="cpg", artifact="cpg_edges_final", kind="table")
def cpg_edges_final(
    ctx: ExecutionContext,
    rel_name_symbol: pa.Table,
    rel_import_symbol: pa.Table,
    rel_callsite_symbol: pa.Table,
    rel_callsite_qname: pa.Table,
) -> pa.Table:
    res = build_cpg_edges(
        ctx=ctx,
        rel_name_symbol=rel_name_symbol,
        rel_import_symbol=rel_import_symbol,
        rel_callsite_symbol=rel_callsite_symbol,
        rel_callsite_qname=rel_callsite_qname,
    )
    return res.table


@cache()
@tag(layer="cpg", artifact="cpg_props_final", kind="table")
def cpg_props_final(
    ctx: ExecutionContext,
    repo_files: pa.Table,
    cst_name_refs: pa.Table,
    cst_imports_norm: pa.Table,
    cst_callsites: pa.Table,
    cst_defs_norm: pa.Table,
    dim_qualified_names: pa.Table,
    scip_symbol_information: pa.Table,
    cpg_edges_final: pa.Table,
) -> pa.Table:
    res = build_cpg_props(
        ctx=ctx,
        repo_files=repo_files,
        cst_name_refs=cst_name_refs,
        cst_imports=cst_imports_norm,
        cst_callsites=cst_callsites,
        cst_defs=cst_defs_norm,
        dim_qualified_names=dim_qualified_names,
        scip_symbol_information=scip_symbol_information,
        cpg_edges=cpg_edges_final,
    )
    return res.table
