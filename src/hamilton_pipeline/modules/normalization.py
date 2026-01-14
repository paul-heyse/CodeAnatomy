"""Hamilton normalization stage functions."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import cache as memoize
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from hamilton.function_modifiers import cache, extract_fields, tag
from ibis.backends import BaseBackend

from arrowdsl.compute.kernels import (
    distinct_sorted,
    flatten_list_struct_field,
    resolve_kernel,
)
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import prefixed_hash_id
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, TableLike, pc
from arrowdsl.plan.joins import JoinConfig, left_join
from arrowdsl.schema.build import empty_table, set_or_append_column, table_from_arrays
from config import AdapterMode
from extract.evidence_plan import EvidencePlan
from normalize.catalog import (
    NormalizeCatalogInputs,
    NormalizePlanCatalog,
)
from normalize.catalog import (
    normalize_plan_catalog as build_normalize_plan_catalog,
)
from normalize.diagnostics import DiagnosticsSources
from normalize.registry_specs import (
    dataset_alias,
    dataset_contract,
    dataset_name_from_alias,
    dataset_schema,
    dataset_schema_policy,
    dataset_spec,
)
from normalize.runner import (
    NormalizeFinalizeSpec,
    NormalizeIbisPlanOptions,
    NormalizeRuleCompilation,
    compile_normalize_plans_ibis,
    compile_normalize_rules,
    run_normalize,
)
from relspec.adapters.normalize import NormalizeRuleAdapter
from relspec.rules.compiler import RuleCompiler
from relspec.rules.handlers.normalize import NormalizeRuleHandler
from relspec.rules.registry import RuleRegistry

if TYPE_CHECKING:
    from normalize.rule_model import NormalizeRule
    from relspec.rules.definitions import RuleDefinition
from normalize.ibis_spans import (
    add_ast_byte_spans_ibis,
    add_scip_occurrence_byte_spans_ibis,
    anchor_instructions_ibis,
)
from normalize.schema_infer import align_table_to_schema, infer_schema_or_registry
from normalize.span_pipeline import span_error_table
from normalize.spans import (
    RepoTextIndex,
    build_repo_text_index,
    normalize_cst_defs_spans,
    normalize_cst_imports_spans,
)
from schema_spec.specs import ArrowFieldSpec, call_span_bundle
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY, make_dataset_spec, make_table_spec

SCHEMA_VERSION = 1

QNAME_DIM_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=make_table_spec(
            name="dim_qualified_names_v1",
            version=SCHEMA_VERSION,
            bundles=(),
            fields=[
                ArrowFieldSpec(name="qname_id", dtype=pa.string()),
                ArrowFieldSpec(name="qname", dtype=pa.string()),
            ],
        )
    )
)

CALLSITE_QNAME_CANDIDATES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=make_table_spec(
            name="callsite_qname_candidates_v1",
            version=SCHEMA_VERSION,
            bundles=(),
            fields=[
                ArrowFieldSpec(name="call_id", dtype=pa.string()),
                ArrowFieldSpec(name="qname", dtype=pa.string()),
                ArrowFieldSpec(name="path", dtype=pa.string()),
                *call_span_bundle().fields,
                ArrowFieldSpec(name="qname_source", dtype=pa.string()),
            ],
        )
    )
)


def _string_or_null(values: ArrayLike | ChunkedArrayLike) -> ArrayLike:
    casted = pc.cast(values, pa.string())
    empty = pc.equal(casted, pa.scalar(""))
    return pc.if_else(empty, pa.scalar(None, type=pa.string()), casted)


def _requires_output(plan: EvidencePlan | None, name: str) -> bool:
    if plan is None:
        return True
    return plan.requires_dataset(name)


def _requires_any(plan: EvidencePlan | None, names: Sequence[str]) -> bool:
    return any(_requires_output(plan, name) for name in names)


@dataclass(frozen=True)
class NormalizeTypeSources:
    """Normalize sources for type rules."""

    cst_type_exprs: TableLike | None = None
    scip_symbol_information: TableLike | None = None


@dataclass(frozen=True)
class NormalizeBytecodeSources:
    """Normalize sources for bytecode rules."""

    py_bc_blocks: TableLike | None = None
    py_bc_cfg_edges: TableLike | None = None
    py_bc_code_units: TableLike | None = None
    py_bc_instructions: TableLike | None = None


@dataclass(frozen=True)
class NormalizeSpanSources:
    """Normalize sources for span/diagnostic rules."""

    repo_text_index: RepoTextIndex | None = None
    span_errors: TableLike | None = None


@cache()
@tag(layer="normalize", artifact="normalize_type_sources", kind="object")
def normalize_type_sources(
    cst_type_exprs: TableLike | None = None,
    scip_symbol_information: TableLike | None = None,
) -> NormalizeTypeSources:
    """Bundle type-related normalize inputs.

    Returns
    -------
    NormalizeTypeSources
        Type-related inputs for normalize rule compilation.
    """
    return NormalizeTypeSources(
        cst_type_exprs=cst_type_exprs,
        scip_symbol_information=scip_symbol_information,
    )


@cache()
@tag(layer="normalize", artifact="normalize_bytecode_sources", kind="object")
def normalize_bytecode_sources(
    py_bc_blocks: TableLike | None = None,
    py_bc_cfg_edges: TableLike | None = None,
    py_bc_code_units: TableLike | None = None,
    py_bc_instructions: TableLike | None = None,
) -> NormalizeBytecodeSources:
    """Bundle bytecode normalize inputs.

    Returns
    -------
    NormalizeBytecodeSources
        Bytecode inputs for normalize rule compilation.
    """
    return NormalizeBytecodeSources(
        py_bc_blocks=py_bc_blocks,
        py_bc_cfg_edges=py_bc_cfg_edges,
        py_bc_code_units=py_bc_code_units,
        py_bc_instructions=py_bc_instructions,
    )


@cache()
@tag(layer="normalize", artifact="normalize_span_sources", kind="object")
def normalize_span_sources(
    repo_text_index: RepoTextIndex | None = None,
    span_errors: TableLike | None = None,
) -> NormalizeSpanSources:
    """Bundle span-related normalize inputs.

    Returns
    -------
    NormalizeSpanSources
        Span-related inputs for normalize rule compilation.
    """
    return NormalizeSpanSources(repo_text_index=repo_text_index, span_errors=span_errors)


@cache()
@tag(layer="normalize", artifact="normalize_catalog_inputs", kind="object")
def normalize_catalog_inputs(
    normalize_type_sources: NormalizeTypeSources | None = None,
    diagnostics_sources: DiagnosticsSources | None = None,
    normalize_bytecode_sources: NormalizeBytecodeSources | None = None,
    normalize_span_sources: NormalizeSpanSources | None = None,
) -> NormalizeCatalogInputs:
    """Bundle plan sources for normalize rule compilation.

    Returns
    -------
    NormalizeCatalogInputs
        Bundle of normalize inputs for plan compilation.
    """
    type_sources = normalize_type_sources or NormalizeTypeSources()
    diag_sources = diagnostics_sources or DiagnosticsSources(
        cst_parse_errors=None,
        ts_errors=None,
        ts_missing=None,
        scip_diagnostics=None,
        scip_documents=None,
    )
    bytecode_sources = normalize_bytecode_sources or NormalizeBytecodeSources()
    span_sources = normalize_span_sources or NormalizeSpanSources()
    return NormalizeCatalogInputs(
        cst_type_exprs=type_sources.cst_type_exprs,
        scip_symbol_information=type_sources.scip_symbol_information,
        cst_parse_errors=diag_sources.cst_parse_errors,
        ts_errors=diag_sources.ts_errors,
        ts_missing=diag_sources.ts_missing,
        scip_diagnostics=diag_sources.scip_diagnostics,
        scip_documents=diag_sources.scip_documents,
        py_bc_blocks=bytecode_sources.py_bc_blocks,
        py_bc_cfg_edges=bytecode_sources.py_bc_cfg_edges,
        py_bc_code_units=bytecode_sources.py_bc_code_units,
        py_bc_instructions=bytecode_sources.py_bc_instructions,
        span_errors=span_sources.span_errors,
        repo_text_index=span_sources.repo_text_index,
    )


@cache()
@tag(layer="normalize", artifact="normalize_plan_catalog", kind="object")
def normalize_plan_catalog(
    normalize_catalog_inputs: NormalizeCatalogInputs,
) -> NormalizePlanCatalog:
    """Build a normalize plan catalog from pipeline inputs.

    Returns
    -------
    NormalizePlanCatalog
        Plan catalog seeded with normalize inputs.
    """
    return build_normalize_plan_catalog(normalize_catalog_inputs)


@cache()
@tag(layer="normalize", artifact="normalize_rule_compilation", kind="object")
def normalize_rule_compilation(
    normalize_plan_catalog: NormalizePlanCatalog,
    ctx: ExecutionContext,
    adapter_mode: AdapterMode,
    ibis_backend: BaseBackend,
    evidence_plan: EvidencePlan | None = None,
) -> NormalizeRuleCompilation:
    """Compile normalize rules into plan outputs.

    Returns
    -------
    NormalizeRuleCompilation
        Compiled rules, plans, and catalog updates.
    """
    rules = _normalize_rules(ctx=ctx)
    required_outputs = _required_rule_outputs(evidence_plan, rules)
    if required_outputs is not None and not required_outputs:
        return NormalizeRuleCompilation(rules=(), plans={}, catalog=normalize_plan_catalog)
    if adapter_mode.use_ibis_bridge:
        plans = compile_normalize_plans_ibis(
            normalize_plan_catalog,
            ctx=ctx,
            options=NormalizeIbisPlanOptions(
                backend=ibis_backend,
                rules=rules,
                required_outputs=required_outputs,
            ),
        )
        return NormalizeRuleCompilation(
            rules=rules,
            plans=plans,
            catalog=normalize_plan_catalog,
        )
    return compile_normalize_rules(
        normalize_plan_catalog,
        ctx=ctx,
        rules=rules,
        required_outputs=required_outputs,
    )


def _required_rule_outputs(
    plan: EvidencePlan | None,
    rules: Sequence[NormalizeRule],
) -> tuple[str, ...] | None:
    if plan is None:
        return None
    outputs = [rule.output for rule in rules if plan.requires_dataset(dataset_alias(rule.output))]
    return tuple(outputs)


@memoize
def _normalize_rule_definitions() -> tuple[RuleDefinition, ...]:
    registry = RuleRegistry(adapters=(NormalizeRuleAdapter(),))
    return registry.rules_for_domain("normalize")


def _normalize_rules(ctx: ExecutionContext) -> tuple[NormalizeRule, ...]:
    compiler = RuleCompiler(handlers={"normalize": NormalizeRuleHandler()})
    compiled = compiler.compile_rules(_normalize_rule_definitions(), ctx=ctx)
    return cast("tuple[NormalizeRule, ...]", compiled)


def _normalize_rule_output(
    compilation: NormalizeRuleCompilation,
    output: str,
    *,
    ctx: ExecutionContext,
) -> TableLike:
    plan = compilation.plans.get(output)
    if plan is None:
        return empty_table(dataset_schema(output))
    finalize_spec = NormalizeFinalizeSpec(
        metadata_spec=dataset_spec(output).metadata_spec,
        schema_policy=dataset_schema_policy(output, ctx=ctx),
    )
    return run_normalize(
        plan=plan,
        post=(),
        contract=dataset_contract(output),
        ctx=ctx,
        finalize_spec=finalize_spec,
    ).good


def _empty_scip_occurrences_norm(scip_occurrences: TableLike) -> TableLike:
    table = empty_table(scip_occurrences.schema)
    table = set_or_append_column(table, "bstart", pa.array([], type=pa.int64()))
    table = set_or_append_column(table, "bend", pa.array([], type=pa.int64()))
    table = set_or_append_column(table, "span_ok", pa.array([], type=pa.bool_()))
    table = set_or_append_column(table, "enc_bstart", pa.array([], type=pa.int64()))
    table = set_or_append_column(table, "enc_bend", pa.array([], type=pa.int64()))
    return set_or_append_column(table, "span_id", pa.array([], type=pa.string()))


def _flatten_qname_names(table: TableLike | None, column: str) -> ArrayLike:
    if table is None or column not in table.column_names:
        return pa.array([], type=pa.string())
    flattened = flatten_list_struct_field(table, list_col=column, field="name")
    return pc.drop_null(_string_or_null(flattened))


def _callsite_qname_base_table(exploded: TableLike) -> TableLike:
    call_ids = _string_or_null(exploded["call_id"])
    qname_struct = exploded["qname_struct"]
    qname_vals = _string_or_null(pc.struct_field(qname_struct, "name"))
    qname_sources = _string_or_null(pc.struct_field(qname_struct, "source"))
    schema = pa.schema(
        [
            ("call_id", pa.string()),
            ("qname", pa.string()),
            ("qname_source", pa.string()),
        ]
    )
    base = table_from_arrays(
        schema,
        columns={"call_id": call_ids, "qname": qname_vals, "qname_source": qname_sources},
        num_rows=len(call_ids),
    )
    mask = pc.and_(pc.is_valid(base["call_id"]), pc.is_valid(base["qname"]))
    mask = pc.fill_null(mask, fill_value=False)
    return base.filter(mask)


def _join_callsite_qname_meta(base: TableLike, cst_callsites: TableLike) -> TableLike:
    meta_cols = [
        col
        for col in ("call_id", "path", "call_bstart", "call_bend", "qname_source")
        if col in cst_callsites.column_names
    ]
    if len(meta_cols) <= 1:
        return base
    meta_table = cst_callsites.select(meta_cols)
    right_cols = [col for col in meta_cols if col != "call_id"]
    joined = left_join(
        base,
        meta_table,
        config=JoinConfig.on_keys(
            keys=("call_id",),
            left_output=("call_id", "qname", "qname_source"),
            right_output=right_cols,
            output_suffix_for_right="__meta",
        ),
    )
    if "qname_source__meta" not in joined.column_names:
        return joined
    primary = _string_or_null(joined["qname_source"])
    meta_source = _string_or_null(joined["qname_source__meta"])
    merged = pc.coalesce(primary, meta_source)
    joined = set_or_append_column(joined, "qname_source", merged)
    return joined.drop(["qname_source__meta"])


@cache(format="parquet")
@tag(layer="normalize", artifact="dim_qualified_names", kind="table")
def dim_qualified_names(
    cst_callsites: TableLike,
    cst_defs: TableLike,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Build a dimension table of qualified names from CST extraction.

    Expected output columns:
      - qname_id (stable)
      - qname (string)

    Returns
    -------
    TableLike
        Qualified name dimension table.
    """
    _ = ctx
    if not _requires_output(evidence_plan, "dim_qualified_names"):
        schema = infer_schema_or_registry(QNAME_DIM_SPEC.table_spec.name, [])
        return empty_table(schema)
    callsite_qnames = _flatten_qname_names(cst_callsites, "callee_qnames")
    def_qnames = _flatten_qname_names(cst_defs, "qnames")
    combined = pa.chunked_array([callsite_qnames, def_qnames])
    if len(combined) == 0:
        schema = infer_schema_or_registry(QNAME_DIM_SPEC.table_spec.name, [])
        return empty_table(schema)

    qname_array = distinct_sorted(combined)
    qname_ids = prefixed_hash_id([qname_array], prefix="qname")
    out_schema = pa.schema([("qname_id", pa.string()), ("qname", pa.string())])
    out = table_from_arrays(
        out_schema,
        columns={"qname_id": qname_ids, "qname": qname_array},
        num_rows=len(qname_array),
    )
    tables = [out] if out.num_rows > 0 else []
    schema = infer_schema_or_registry(QNAME_DIM_SPEC.table_spec.name, tables)
    return align_table_to_schema(out, schema)


@cache(format="parquet")
@tag(layer="normalize", artifact="callsite_qname_candidates", kind="table")
def callsite_qname_candidates(
    cst_callsites: TableLike,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Explode callsite qualified names into a row-per-candidate table.

    Output columns (minimum):
      - call_id
      - qname
      - path
      - call_bstart
      - call_bend
      - qname_source (optional)

    Returns
    -------
    TableLike
        Table of callsite qualified name candidates.
    """
    if not _requires_output(evidence_plan, "callsite_qname_candidates"):
        schema = infer_schema_or_registry(CALLSITE_QNAME_CANDIDATES_SPEC.table_spec.name, [])
        return empty_table(schema)
    if (
        cst_callsites is None
        or cst_callsites.num_rows == 0
        or "callee_qnames" not in cst_callsites.column_names
    ):
        schema = infer_schema_or_registry(CALLSITE_QNAME_CANDIDATES_SPEC.table_spec.name, [])
        return empty_table(schema)

    kernel = resolve_kernel("explode_list", ctx=ctx)
    exploded = kernel(
        cst_callsites,
        parent_id_col="call_id",
        list_col="callee_qnames",
        out_parent_col="call_id",
        out_value_col="qname_struct",
    )
    base = _callsite_qname_base_table(exploded)
    joined = _join_callsite_qname_meta(base, cst_callsites)

    tables = [joined] if joined.num_rows > 0 else []
    schema = infer_schema_or_registry(
        CALLSITE_QNAME_CANDIDATES_SPEC.table_spec.name,
        tables,
    )
    return align_table_to_schema(joined, schema)


@cache(format="parquet")
@tag(layer="normalize", artifact="ast_nodes_norm", kind="table")
def ast_nodes_norm(
    file_line_index: TableLike,
    ast_nodes: TableLike,
    ibis_backend: BaseBackend,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Add byte-span columns to AST nodes for join-ready alignment.

    Returns
    -------
    TableLike
        AST nodes with bstart/bend/span_ok columns appended.
    """
    _ = ctx
    if not _requires_output(evidence_plan, "ast_nodes_norm"):
        return empty_table(ast_nodes.schema)
    return add_ast_byte_spans_ibis(
        file_line_index,
        ast_nodes,
        backend=ibis_backend,
    )


@cache(format="parquet")
@tag(layer="normalize", artifact="py_bc_instructions_norm", kind="table")
def py_bc_instructions_norm(
    file_line_index: TableLike,
    py_bc_instructions: TableLike,
    ibis_backend: BaseBackend,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Anchor bytecode instructions to source byte spans.

    Returns
    -------
    TableLike
        Bytecode instruction table with bstart/bend/span_ok columns.
    """
    _ = ctx
    if not _requires_output(evidence_plan, "py_bc_instructions_norm"):
        return empty_table(py_bc_instructions.schema)
    return anchor_instructions_ibis(
        file_line_index,
        py_bc_instructions,
        backend=ibis_backend,
    )


@cache(format="parquet")
@tag(layer="normalize", artifact="py_bc_blocks_norm", kind="table")
def py_bc_blocks_norm(
    normalize_rule_compilation: NormalizeRuleCompilation,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Normalize bytecode CFG blocks with file/path metadata.

    Returns
    -------
    TableLike
        CFG block table aligned to the normalization schema.
    """
    if not _requires_output(evidence_plan, "py_bc_blocks_norm"):
        return empty_table(dataset_schema(dataset_name_from_alias("py_bc_blocks_norm")))
    return _normalize_rule_output(
        normalize_rule_compilation,
        dataset_name_from_alias("py_bc_blocks_norm"),
        ctx=ctx,
    )


@cache(format="parquet")
@tag(layer="normalize", artifact="py_bc_cfg_edges_norm", kind="table")
def py_bc_cfg_edges_norm(
    normalize_rule_compilation: NormalizeRuleCompilation,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Normalize bytecode CFG edges with file/path metadata.

    Returns
    -------
    TableLike
        CFG edge table aligned to the normalization schema.
    """
    if not _requires_output(evidence_plan, "py_bc_cfg_edges_norm"):
        return empty_table(dataset_schema(dataset_name_from_alias("py_bc_cfg_edges_norm")))
    return _normalize_rule_output(
        normalize_rule_compilation,
        dataset_name_from_alias("py_bc_cfg_edges_norm"),
        ctx=ctx,
    )


@cache(format="parquet")
@tag(layer="normalize", artifact="py_bc_def_use_events", kind="table")
def py_bc_def_use_events(
    normalize_rule_compilation: NormalizeRuleCompilation,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Derive def/use events from bytecode instructions.

    Returns
    -------
    TableLike
        Def/use events table.
    """
    if not _requires_output(evidence_plan, "py_bc_def_use_events"):
        return empty_table(dataset_schema(dataset_name_from_alias("py_bc_def_use_events")))
    return _normalize_rule_output(
        normalize_rule_compilation,
        dataset_name_from_alias("py_bc_def_use_events"),
        ctx=ctx,
    )


@cache(format="parquet")
@tag(layer="normalize", artifact="py_bc_reaching_defs", kind="table")
def py_bc_reaching_defs(
    normalize_rule_compilation: NormalizeRuleCompilation,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Compute reaching-def edges from def/use events.

    Returns
    -------
    TableLike
        Reaching-def edges table.
    """
    if not _requires_output(evidence_plan, "py_bc_reaching_defs"):
        return empty_table(dataset_schema(dataset_name_from_alias("py_bc_reaching_defs")))
    return _normalize_rule_output(
        normalize_rule_compilation,
        dataset_name_from_alias("py_bc_reaching_defs"),
        ctx=ctx,
    )


@cache(format="parquet")
@tag(layer="normalize", artifact="type_exprs_norm", kind="table")
def type_exprs_norm(
    normalize_rule_compilation: NormalizeRuleCompilation,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Normalize CST type expressions into join-ready tables.

    Returns
    -------
    TableLike
        Normalized type expressions table.
    """
    if not _requires_output(evidence_plan, "type_exprs_norm"):
        return empty_table(dataset_schema(dataset_name_from_alias("type_exprs_norm")))
    return _normalize_rule_output(
        normalize_rule_compilation,
        dataset_name_from_alias("type_exprs_norm"),
        ctx=ctx,
    )


@cache(format="parquet")
@tag(layer="normalize", artifact="types_norm", kind="table")
def types_norm(
    normalize_rule_compilation: NormalizeRuleCompilation,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Normalize type expressions into type nodes.

    Returns
    -------
    TableLike
        Normalized type node table.
    """
    if not _requires_output(evidence_plan, "types_norm"):
        return empty_table(dataset_schema(dataset_name_from_alias("types_norm")))
    return _normalize_rule_output(
        normalize_rule_compilation,
        dataset_name_from_alias("types_norm"),
        ctx=ctx,
    )


@cache()
@tag(layer="normalize", artifact="diagnostics_sources", kind="object")
def diagnostics_sources(
    cst_parse_errors: TableLike,
    ts_errors: TableLike,
    ts_missing: TableLike,
    scip_diagnostics: TableLike,
    scip_documents: TableLike,
) -> DiagnosticsSources:
    """Bundle diagnostic source tables.

    Returns
    -------
    DiagnosticsSources
        Diagnostic source tables bundle.
    """
    return DiagnosticsSources(
        cst_parse_errors=cst_parse_errors,
        ts_errors=ts_errors,
        ts_missing=ts_missing,
        scip_diagnostics=scip_diagnostics,
        scip_documents=scip_documents,
    )


@cache(format="parquet")
@tag(layer="normalize", artifact="diagnostics_norm", kind="table")
def diagnostics_norm(
    normalize_rule_compilation: NormalizeRuleCompilation,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Aggregate diagnostics into a normalized table.

    Returns
    -------
    TableLike
        Normalized diagnostics table.
    """
    if not _requires_output(evidence_plan, "diagnostics_norm"):
        return empty_table(dataset_schema(dataset_name_from_alias("diagnostics_norm")))
    return _normalize_rule_output(
        normalize_rule_compilation,
        dataset_name_from_alias("diagnostics_norm"),
        ctx=ctx,
    )


@cache()
@tag(layer="normalize", artifact="repo_text_index", kind="object")
def repo_text_index(
    repo_root: str,
    repo_files: TableLike,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> RepoTextIndex:
    """Build a repo text index for line/column to byte offsets.

    Returns
    -------
    RepoTextIndex
        Repository text index for span conversion.
    """
    if evidence_plan is not None and not (
        evidence_plan.requires_dataset("repo_text_index")
        or evidence_plan.requires_dataset("diagnostics_norm")
    ):
        return RepoTextIndex(by_file_id={}, by_path={})
    return build_repo_text_index(repo_root=repo_root, repo_files=repo_files, ctx=ctx)


@cache()
@extract_fields(
    {
        "scip_occurrences_norm": TableLike,
        "scip_span_errors": TableLike,
    }
)
@tag(layer="normalize", artifact="scip_occurrences_norm_bundle", kind="bundle")
def scip_occurrences_norm_bundle(
    scip_documents: TableLike,
    scip_occurrences: TableLike,
    file_line_index: TableLike,
    ibis_backend: BaseBackend,
    evidence_plan: EvidencePlan | None = None,
) -> dict[str, TableLike]:
    """Convert SCIP occurrences into byte offsets.

    Returns
    -------
    dict[str, TableLike]
        Bundle with normalized occurrences and span errors.
    """
    if not _requires_any(evidence_plan, ("scip_occurrences_norm", "scip_span_errors")):
        return {
            "scip_occurrences_norm": _empty_scip_occurrences_norm(scip_occurrences),
            "scip_span_errors": span_error_table([]),
        }
    occ, errs = add_scip_occurrence_byte_spans_ibis(
        file_line_index,
        scip_documents,
        scip_occurrences,
        backend=ibis_backend,
    )
    return {"scip_occurrences_norm": occ, "scip_span_errors": errs}


@cache(format="parquet")
@tag(layer="normalize", artifact="cst_imports_norm", kind="table")
def cst_imports_norm(
    cst_imports: TableLike,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Normalize CST import spans into bstart/bend.

    Returns
    -------
    TableLike
        Normalized CST imports table.
    """
    _ = ctx
    if not _requires_output(evidence_plan, "cst_imports_norm"):
        return empty_table(cst_imports.schema)
    return normalize_cst_imports_spans(py_cst_imports=cst_imports)


@cache(format="parquet")
@tag(layer="normalize", artifact="cst_defs_norm", kind="table")
def cst_defs_norm(
    cst_defs: TableLike,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Normalize CST def spans into bstart/bend.

    Returns
    -------
    TableLike
        Normalized CST definitions table.
    """
    _ = ctx
    if not _requires_output(evidence_plan, "cst_defs_norm"):
        return empty_table(cst_defs.schema)
    return normalize_cst_defs_spans(py_cst_defs=cst_defs)
