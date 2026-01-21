"""Hamilton normalization stage functions."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import cache as memoize
from typing import TYPE_CHECKING, Protocol, cast

import pyarrow as pa
import pyarrow.compute as pc
from hamilton.function_modifiers import cache, extract_fields, tag
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.expr_types import ExplodeSpec
from arrowdsl.core.ids import prefixed_hash_id
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    RecordBatchReaderLike,
    TableLike,
    coerce_table_like,
)
from arrowdsl.core.joins import JoinConfig, left_join
from arrowdsl.schema.build import const_array, empty_table, set_or_append_column, table_from_arrays
from datafusion_engine.compute_ops import (
    and_,
    cast_values,
    coalesce,
    distinct_sorted,
    drop_null,
    equal,
    fill_null,
    flatten_list_struct_field,
    if_else,
    invert,
    is_valid,
    list_flatten,
    struct_field,
)
from datafusion_engine.kernel_registry import resolve_kernel
from datafusion_engine.nested_tables import materialize_sql_fragment, register_nested_table
from datafusion_engine.query_fragments import (
    SqlFragment,
    ast_nodes_sql,
    bytecode_blocks_sql,
    bytecode_cfg_edges_sql,
    bytecode_code_units_sql,
    bytecode_instructions_sql,
    libcst_call_args_sql,
    libcst_callsites_sql,
    libcst_defs_sql,
    libcst_imports_sql,
    libcst_parse_errors_sql,
    libcst_type_exprs_sql,
    scip_diagnostics_sql,
    scip_documents_sql,
    scip_occurrences_sql,
    scip_symbol_information_sql,
    tree_sitter_errors_sql,
    tree_sitter_missing_sql,
)
from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel
from extract.evidence_plan import EvidencePlan
from ibis_engine.registry import datafusion_context
from normalize.catalog import IbisPlanCatalog, NormalizeCatalogInputs
from normalize.catalog import normalize_plan_catalog as build_normalize_plan_catalog
from normalize.ibis_api import DiagnosticsSources
from normalize.ibis_plan_builders import IbisPlanSource
from normalize.ibis_spans import (
    add_ast_byte_spans_ibis,
    add_scip_occurrence_byte_spans_ibis,
    anchor_instructions_ibis,
)
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
    NormalizeRunOptions,
    compile_normalize_plans_ibis,
    resolve_normalize_rules,
    run_normalize,
)
from normalize.schema_infer import (
    SchemaInferOptions,
    align_table_to_schema,
    infer_schema_or_registry,
)
from normalize.span_pipeline import span_error_table
from normalize.spans import (
    RepoTextIndex,
    build_repo_text_index,
    normalize_cst_defs_spans,
    normalize_cst_imports_spans,
)
from normalize.text_index import ENC_UTF8, ENC_UTF16, ENC_UTF32
from relspec.pipeline_policy import PipelinePolicy
from relspec.registry.rules import RuleRegistry
from relspec.rules.discovery import discover_bundles

if TYPE_CHECKING:
    from relspec.rules.definitions import RuleDefinition

LOGGER = logging.getLogger(__name__)
VALID_SCIP_POSITION_ENCODINGS: frozenset[int] = frozenset((ENC_UTF8, ENC_UTF16, ENC_UTF32))

DEFAULT_MATERIALIZE_OUTPUTS: tuple[str, ...] = ()

QNAME_DIM_NAME = "dim_qualified_names_v1"
CALLSITE_QNAME_CANDIDATES_NAME = "callsite_qname_candidates_v1"
CALLSITE_ARG_SUMMARY_SCHEMA = pa.schema(
    [
        ("call_id", pa.string()),
        ("arg_count", pa.int32()),
        ("keyword_count", pa.int32()),
        ("star_arg_count", pa.int32()),
        ("star_kwarg_count", pa.int32()),
        ("positional_count", pa.int32()),
    ]
)


def _string_or_null(values: ArrayLike | ChunkedArrayLike) -> ArrayLike:
    casted = cast_values(values, pa.string())
    empty = equal(casted, pa.scalar(""))
    return if_else(empty, pa.scalar(None, type=pa.string()), casted)


def _requires_output(plan: EvidencePlan | None, name: str) -> bool:
    if plan is None:
        return True
    return plan.requires_dataset(name)


def _requires_any(plan: EvidencePlan | None, names: Sequence[str]) -> bool:
    return any(_requires_output(plan, name) for name in names)


def _is_datafusion_backend(backend: BaseBackend) -> bool:
    name = getattr(backend, "name", "")
    return str(name).lower() == "datafusion"


def _posenc_value(value: object | None) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        return int(text) if text.isdigit() else None
    return None


def _posenc_stats_from_rows(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    missing = 0
    invalid = 0
    total = 0
    valid_values: set[int] = set()
    invalid_values: set[str] = set()
    for row in rows:
        raw_value = row.get("position_encoding")
        if raw_value is None and "values" in row:
            raw_value = row.get("values")
        raw_count = row.get("doc_count")
        if raw_count is None and "counts" in row:
            raw_count = row.get("counts")
        count = int(raw_count) if raw_count is not None else 0
        total += count
        if raw_value is None:
            missing += count
            continue
        value = _posenc_value(raw_value)
        if value is None:
            invalid += count
            invalid_values.add(str(raw_value))
            continue
        if value not in VALID_SCIP_POSITION_ENCODINGS:
            invalid += count
            invalid_values.add(str(value))
            continue
        valid_values.add(value)
    return {
        "document_count": total,
        "missing_position_encoding": missing,
        "invalid_position_encoding": invalid,
        "valid_position_encodings": sorted(valid_values),
        "invalid_position_encoding_values": sorted(invalid_values),
    }


def _scip_position_encoding_stats(
    scip_documents: TableLike | SqlFragment,
    *,
    backend: BaseBackend,
) -> dict[str, object] | None:
    if isinstance(scip_documents, SqlFragment):
        ctx = datafusion_context(backend)
        if ctx is None:
            return None
        query = (
            "SELECT position_encoding, COUNT(*) AS doc_count "
            f"FROM ({scip_documents.sql}) docs GROUP BY position_encoding"
        )
        table = ctx.sql(query).to_arrow_table()
        return _posenc_stats_from_rows(table.to_pylist())
    resolved = coerce_table_like(scip_documents)
    table = resolved.read_all() if isinstance(resolved, RecordBatchReaderLike) else resolved
    if not isinstance(table, pa.Table):
        return None
    if "position_encoding" not in table.column_names:
        return None
    counts = pc.value_counts(table["position_encoding"])
    rows = counts.to_pylist()
    return _posenc_stats_from_rows(rows)


def _record_scip_position_encoding_stats(
    ctx: ExecutionContext,
    stats: Mapping[str, object],
) -> None:
    runtime = ctx.runtime.datafusion
    if runtime is None or runtime.diagnostics_sink is None:
        return
    payload = {"event_time_unix_ms": int(time.time() * 1000), **dict(stats)}
    runtime.diagnostics_sink.record_artifact("scip_position_encoding_v1", payload)


def _require_datafusion_backend(backend: BaseBackend) -> None:
    if _is_datafusion_backend(backend):
        return
    msg = "Legacy span normalization is disabled; use the DataFusion Ibis backend."
    raise ValueError(msg)


def _materialize_fragment(
    backend: BaseBackend | None,
    source: TableLike | SqlFragment,
) -> TableLike:
    if not isinstance(source, SqlFragment):
        return source
    return materialize_sql_fragment(backend, source)


class _DatafusionQuery(Protocol):
    def schema(self) -> object: ...


class _DatafusionContext(Protocol):
    def sql(self, query: str) -> _DatafusionQuery: ...


def _schema_from_fragment(
    fragment: SqlFragment,
    *,
    backend: BaseBackend | None,
) -> object | None:
    if backend is None:
        return None
    ctx = datafusion_context(backend)
    if ctx is None:
        return None
    fragment_sql = f"SELECT * FROM ({fragment.sql}) AS fragment LIMIT 0"
    df_ctx = cast("_DatafusionContext", ctx)
    return df_ctx.sql(fragment_sql).schema()


def _schema_from_source(
    source: TableLike | SqlFragment | None,
    *,
    backend: BaseBackend | None,
    fallback: str,
) -> pa.Schema:
    fallback_schema = infer_schema_or_registry(fallback)
    if source is None:
        return fallback_schema
    if isinstance(source, SqlFragment):
        schema = _schema_from_fragment(source, backend=backend)
    else:
        schema = source.schema
    if isinstance(schema, pa.Schema):
        return schema
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        return to_pyarrow()
    return fallback_schema


@dataclass(frozen=True)
class NormalizeTypeSourceInputs:
    """Raw inputs for type normalization sources."""

    cst_type_exprs: IbisPlanSource | None = None
    scip_symbol_information: IbisPlanSource | None = None
    libcst_files: TableLike | RecordBatchReaderLike | None = None


@dataclass(frozen=True)
class NormalizeTypeSources:
    """Normalize sources for type rules."""

    cst_type_exprs: IbisPlanSource | None = None
    scip_symbol_information: IbisPlanSource | None = None


@dataclass(frozen=True)
class NormalizeBytecodeSourceInputs:
    """Raw inputs for bytecode normalization sources."""

    py_bc_blocks: IbisPlanSource | None = None
    py_bc_cfg_edges: IbisPlanSource | None = None
    py_bc_code_units: IbisPlanSource | None = None
    py_bc_instructions: IbisPlanSource | None = None
    bytecode_files: TableLike | RecordBatchReaderLike | None = None


@dataclass(frozen=True)
class NormalizeBytecodeSources:
    """Normalize sources for bytecode rules."""

    py_bc_blocks: IbisPlanSource | None = None
    py_bc_cfg_edges: IbisPlanSource | None = None
    py_bc_code_units: IbisPlanSource | None = None
    py_bc_instructions: IbisPlanSource | None = None


@dataclass(frozen=True)
class NormalizeSpanSources:
    """Normalize sources for span/diagnostic rules."""

    repo_text_index: RepoTextIndex | None = None
    span_errors: IbisPlanSource | None = None


@dataclass(frozen=True)
class NormalizeQnameContext:
    """Context inputs for qualified name normalization."""

    normalize_execution_context: NormalizeExecutionContext | None
    libcst_files: TableLike | RecordBatchReaderLike | None
    ctx: ExecutionContext
    evidence_plan: EvidencePlan | None


@dataclass(frozen=True)
class DiagnosticsFragmentInputs:
    """Optional diagnostic fragment inputs."""

    cst_parse_errors: IbisPlanSource | None = None
    ts_errors: IbisPlanSource | None = None
    ts_missing: IbisPlanSource | None = None
    scip_diagnostics: IbisPlanSource | None = None
    scip_documents: IbisPlanSource | None = None


@dataclass(frozen=True)
class DiagnosticsTableInputs:
    """Nested table inputs for diagnostics normalization."""

    libcst_files: TableLike | RecordBatchReaderLike | None = None
    tree_sitter_files: TableLike | RecordBatchReaderLike | None = None


@dataclass(frozen=True)
class SpanNormalizeContext:
    """Shared inputs for span normalization helpers."""

    file_line_index: TableLike
    ibis_backend: BaseBackend
    repo_text_index: RepoTextIndex | None
    ctx: ExecutionContext


@dataclass(frozen=True)
class NormalizeExecutionContext:
    """Execution settings for normalize compilation and execution."""

    ctx: ExecutionContext
    execution_policy: AdapterExecutionPolicy
    ibis_backend: BaseBackend


@cache()
@tag(layer="normalize", artifact="normalize_execution_context", kind="object")
def normalize_execution_context(
    ctx: ExecutionContext,
    adapter_execution_policy: AdapterExecutionPolicy,
    ibis_backend: BaseBackend,
) -> NormalizeExecutionContext:
    """Bundle execution settings for normalize pipelines.

    Returns
    -------
    NormalizeExecutionContext
        Execution settings for normalize compilation and execution.
    """
    return NormalizeExecutionContext(
        ctx=ctx,
        execution_policy=adapter_execution_policy,
        ibis_backend=ibis_backend,
    )


@cache()
@tag(layer="normalize", artifact="normalize_type_source_inputs", kind="object")
def normalize_type_source_inputs(
    cst_type_exprs: IbisPlanSource | None = None,
    scip_symbol_information: IbisPlanSource | None = None,
    libcst_files: TableLike | RecordBatchReaderLike | None = None,
) -> NormalizeTypeSourceInputs:
    """Bundle raw inputs for type normalization sources.

    Returns
    -------
    NormalizeTypeSourceInputs
        Raw inputs for type normalization.
    """
    return NormalizeTypeSourceInputs(
        cst_type_exprs=cst_type_exprs,
        scip_symbol_information=scip_symbol_information,
        libcst_files=libcst_files,
    )


@cache()
@tag(layer="normalize", artifact="normalize_type_sources", kind="object")
def normalize_type_sources(
    normalize_type_source_inputs: NormalizeTypeSourceInputs,
    normalize_execution_context: NormalizeExecutionContext | None = None,
) -> NormalizeTypeSources:
    """Bundle type-related normalize inputs.

    Returns
    -------
    NormalizeTypeSources
        Type-related inputs for normalize rule compilation.
    """
    backend = normalize_execution_context.ibis_backend if normalize_execution_context else None
    register_nested_table(
        backend,
        name="libcst_files_v1",
        table=normalize_type_source_inputs.libcst_files,
    )
    cst_type_exprs = normalize_type_source_inputs.cst_type_exprs
    scip_symbol_information = normalize_type_source_inputs.scip_symbol_information
    if cst_type_exprs is None and normalize_type_source_inputs.libcst_files is not None:
        cst_type_exprs = SqlFragment("cst_type_exprs", libcst_type_exprs_sql())
    if scip_symbol_information is None and backend is not None:
        scip_symbol_information = SqlFragment(
            "scip_symbol_information",
            scip_symbol_information_sql(),
        )
    return NormalizeTypeSources(
        cst_type_exprs=cst_type_exprs,
        scip_symbol_information=scip_symbol_information,
    )


@cache()
@tag(layer="normalize", artifact="normalize_bytecode_source_inputs", kind="object")
def normalize_bytecode_source_inputs(
    py_bc_blocks: IbisPlanSource | None = None,
    py_bc_cfg_edges: IbisPlanSource | None = None,
    py_bc_code_units: IbisPlanSource | None = None,
    py_bc_instructions: IbisPlanSource | None = None,
    bytecode_files: TableLike | RecordBatchReaderLike | None = None,
) -> NormalizeBytecodeSourceInputs:
    """Bundle raw inputs for bytecode normalization sources.

    Returns
    -------
    NormalizeBytecodeSourceInputs
        Raw inputs for bytecode normalization.
    """
    return NormalizeBytecodeSourceInputs(
        py_bc_blocks=py_bc_blocks,
        py_bc_cfg_edges=py_bc_cfg_edges,
        py_bc_code_units=py_bc_code_units,
        py_bc_instructions=py_bc_instructions,
        bytecode_files=bytecode_files,
    )


@cache()
@tag(layer="normalize", artifact="normalize_bytecode_sources", kind="object")
def normalize_bytecode_sources(
    normalize_bytecode_source_inputs: NormalizeBytecodeSourceInputs,
    normalize_execution_context: NormalizeExecutionContext | None = None,
) -> NormalizeBytecodeSources:
    """Bundle bytecode normalize inputs.

    Returns
    -------
    NormalizeBytecodeSources
        Bytecode inputs for normalize rule compilation.
    """
    backend = normalize_execution_context.ibis_backend if normalize_execution_context else None
    register_nested_table(
        backend,
        name="bytecode_files_v1",
        table=normalize_bytecode_source_inputs.bytecode_files,
    )
    py_bc_blocks = normalize_bytecode_source_inputs.py_bc_blocks
    py_bc_cfg_edges = normalize_bytecode_source_inputs.py_bc_cfg_edges
    py_bc_code_units = normalize_bytecode_source_inputs.py_bc_code_units
    py_bc_instructions = normalize_bytecode_source_inputs.py_bc_instructions
    bytecode_files = normalize_bytecode_source_inputs.bytecode_files
    if py_bc_blocks is None and bytecode_files is not None:
        py_bc_blocks = SqlFragment("py_bc_blocks", bytecode_blocks_sql())
    if py_bc_cfg_edges is None and bytecode_files is not None:
        py_bc_cfg_edges = SqlFragment("py_bc_cfg_edges", bytecode_cfg_edges_sql())
    if py_bc_code_units is None and bytecode_files is not None:
        py_bc_code_units = SqlFragment("py_bc_code_units", bytecode_code_units_sql())
    if py_bc_instructions is None and bytecode_files is not None:
        py_bc_instructions = SqlFragment("py_bc_instructions", bytecode_instructions_sql())
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
@tag(layer="normalize", artifact="normalize_qname_context", kind="object")
def normalize_qname_context(
    normalize_execution_context: NormalizeExecutionContext | None = None,
    libcst_files: TableLike | RecordBatchReaderLike | None = None,
    *,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> NormalizeQnameContext:
    """Bundle execution context for qualified name normalization.

    Returns
    -------
    NormalizeQnameContext
        Context inputs for qualified name normalization.
    """
    return NormalizeQnameContext(
        normalize_execution_context=normalize_execution_context,
        libcst_files=libcst_files,
        ctx=ctx,
        evidence_plan=evidence_plan,
    )


@cache()
@tag(layer="normalize", artifact="span_normalize_context", kind="object")
def span_normalize_context(
    file_line_index: TableLike,
    ibis_backend: BaseBackend,
    repo_text_index: RepoTextIndex | None,
    ctx: ExecutionContext,
) -> SpanNormalizeContext:
    """Bundle inputs for span normalization helpers.

    Returns
    -------
    SpanNormalizeContext
        Shared inputs for span normalization operations.
    """
    _require_datafusion_backend(ibis_backend)
    return SpanNormalizeContext(
        file_line_index=file_line_index,
        ibis_backend=ibis_backend,
        repo_text_index=repo_text_index,
        ctx=ctx,
    )


@cache()
@tag(layer="normalize", artifact="normalize_catalog_inputs", kind="object")
def normalize_catalog_inputs(
    normalize_type_sources: NormalizeTypeSources | None = None,
    diagnostics_sources: DiagnosticsSources | None = None,
    normalize_bytecode_sources: NormalizeBytecodeSources | None = None,
    normalize_span_sources: NormalizeSpanSources | None = None,
    file_line_index: TableLike | None = None,
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
        file_line_index=file_line_index,
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
    normalize_execution_context: NormalizeExecutionContext,
) -> IbisPlanCatalog:
    """Build a normalize plan catalog from pipeline inputs.

    Returns
    -------
    IbisPlanCatalog
        Ibis-backed plan catalog seeded with normalize inputs.

    Raises
    ------
    ValueError
        Raised when the Ibis backend is unavailable.
    """
    ibis_backend = normalize_execution_context.ibis_backend
    if ibis_backend is None:
        msg = "Normalize catalog requires an Ibis backend."
        raise ValueError(msg)
    return build_normalize_plan_catalog(normalize_catalog_inputs, backend=ibis_backend)


@cache()
@tag(layer="normalize", artifact="normalize_rule_compilation", kind="object")
def normalize_rule_compilation(
    normalize_plan_catalog: IbisPlanCatalog,
    normalize_execution_context: NormalizeExecutionContext,
    pipeline_policy: PipelinePolicy,
    evidence_plan: EvidencePlan | None = None,
    materialize_outputs: Sequence[str] | None = None,
) -> NormalizeRuleCompilation:
    """Compile normalize rules into plan outputs.

    Returns
    -------
    NormalizeRuleCompilation
        Compiled rules, plans, and catalog updates.

    Raises
    ------
    ValueError
        Raised when Ibis-backed normalization is required but not enabled.
    """
    ctx = normalize_execution_context.ctx
    execution_policy = normalize_execution_context.execution_policy
    ibis_backend = normalize_execution_context.ibis_backend
    rule_definitions = _normalize_rule_definitions()
    resolved_rules = resolve_normalize_rules(
        rule_definitions,
        policy_registry=pipeline_policy.policy_registry,
        scan_provenance_columns=ctx.runtime.scan.scan_provenance_columns,
    )
    required_outputs = _required_rule_outputs(evidence_plan, rule_definitions)
    if required_outputs is not None and not required_outputs:
        return NormalizeRuleCompilation(
            rules=(),
            resolved_rules=(),
            plans={},
            ibis_catalog=normalize_plan_catalog,
        )
    if ibis_backend is None:
        msg = "Normalize compilation requires an Ibis backend."
        raise ValueError(msg)
    materialize = (
        tuple(materialize_outputs)
        if materialize_outputs is not None
        else DEFAULT_MATERIALIZE_OUTPUTS
    )
    plans = compile_normalize_plans_ibis(
        normalize_plan_catalog,
        ctx=ctx,
        options=NormalizeIbisPlanOptions(
            backend=ibis_backend,
            rules=rule_definitions,
            required_outputs=required_outputs,
            materialize_outputs=materialize,
            execution_policy=execution_policy,
            policy_registry=pipeline_policy.policy_registry,
            scan_provenance_columns=ctx.runtime.scan.scan_provenance_columns,
        ),
    )
    output_storage = {name: ("materialized" if name in materialize else "view") for name in plans}
    return NormalizeRuleCompilation(
        rules=rule_definitions,
        resolved_rules=resolved_rules,
        plans=plans,
        ibis_catalog=normalize_plan_catalog,
        output_storage=output_storage,
    )


def _required_rule_outputs(
    plan: EvidencePlan | None,
    rules: Sequence[RuleDefinition],
) -> tuple[str, ...] | None:
    if plan is None:
        return None
    outputs = [rule.output for rule in rules if plan.requires_dataset(dataset_alias(rule.output))]
    return tuple(outputs)


@memoize
def _normalize_rule_definitions() -> tuple[RuleDefinition, ...]:
    bundles = tuple(bundle for bundle in discover_bundles() if bundle.domain == "normalize")
    registry = RuleRegistry(bundles=bundles, include_contract_rules=False)
    return registry.rules_for_domain("normalize")


def _normalize_rule_output(
    compilation: NormalizeRuleCompilation,
    output: str,
    *,
    normalize_execution_context: NormalizeExecutionContext,
) -> TableLike:
    ctx = normalize_execution_context.ctx
    plan = compilation.plans.get(output)
    if plan is None:
        return empty_table(dataset_schema(output))
    rule_name = next(
        (rule.name for rule in compilation.rules if rule.output == output),
        output,
    )
    execution_label = ExecutionLabel(rule_name=rule_name, output_dataset=output)
    finalize_spec = NormalizeFinalizeSpec(
        metadata_spec=dataset_spec(output).metadata_spec,
        schema_policy=dataset_schema_policy(output, ctx=ctx),
    )
    return run_normalize(
        plan=plan,
        post=(),
        contract=dataset_contract(output),
        ctx=ctx,
        options=NormalizeRunOptions(
            finalize_spec=finalize_spec,
            execution_policy=normalize_execution_context.execution_policy,
            execution_label=execution_label,
            ibis_backend=normalize_execution_context.ibis_backend,
        ),
    ).good


def _empty_scip_occurrences_norm(schema: pa.Schema) -> TableLike:
    table = empty_table(schema)
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
    return drop_null(_string_or_null(flattened))


def _flatten_string_list(table: TableLike | None, column: str) -> ArrayLike:
    if table is None or column not in table.column_names:
        return pa.array([], type=pa.string())
    flattened = list_flatten(table[column])
    return drop_null(_string_or_null(flattened))


def _count_mask(values: ArrayLike) -> ArrayLike:
    mask = fill_null(values, fill_value=False)
    return cast_values(mask, pa.int32())


def _callsite_arg_summary(call_args: TableLike) -> TableLike:
    if call_args.num_rows == 0 or "call_id" not in call_args.column_names:
        return empty_table(CALLSITE_ARG_SUMMARY_SCHEMA)
    call_ids = _string_or_null(call_args["call_id"])
    num_rows = len(call_ids)
    if num_rows == 0:
        return empty_table(CALLSITE_ARG_SUMMARY_SCHEMA)
    if "keyword" in call_args.column_names:
        keyword_vals = _string_or_null(call_args["keyword"])
    else:
        keyword_vals = pa.nulls(num_rows, type=pa.string())
    is_keyword = fill_null(is_valid(keyword_vals), fill_value=False)
    if "star" in call_args.column_names:
        star_vals = _string_or_null(call_args["star"])
    else:
        star_vals = pa.nulls(num_rows, type=pa.string())
    is_star_arg = fill_null(equal(star_vals, pa.scalar("*")), fill_value=False)
    is_star_kwarg = fill_null(equal(star_vals, pa.scalar("**")), fill_value=False)
    positional = and_(
        invert(is_keyword),
        and_(invert(is_star_arg), invert(is_star_kwarg)),
    )
    base = table_from_arrays(
        CALLSITE_ARG_SUMMARY_SCHEMA,
        columns={
            "call_id": call_ids,
            "arg_count": const_array(num_rows, 1, dtype=pa.int32()),
            "keyword_count": _count_mask(is_keyword),
            "star_arg_count": _count_mask(is_star_arg),
            "star_kwarg_count": _count_mask(is_star_kwarg),
            "positional_count": _count_mask(positional),
        },
        num_rows=num_rows,
    )
    valid_mask = fill_null(is_valid(base["call_id"]), fill_value=False)
    base = base.filter(valid_mask)
    if base.num_rows == 0:
        return empty_table(CALLSITE_ARG_SUMMARY_SCHEMA)
    grouped = base.group_by(["call_id"]).aggregate(
        [
            ("arg_count", "sum"),
            ("keyword_count", "sum"),
            ("star_arg_count", "sum"),
            ("star_kwarg_count", "sum"),
            ("positional_count", "sum"),
        ]
    )
    if grouped.num_rows == 0:
        return empty_table(CALLSITE_ARG_SUMMARY_SCHEMA)
    summary = table_from_arrays(
        CALLSITE_ARG_SUMMARY_SCHEMA,
        columns={
            "call_id": _string_or_null(grouped["call_id"]),
            "arg_count": cast_values(grouped["arg_count_sum"], pa.int32()),
            "keyword_count": cast_values(grouped["keyword_count_sum"], pa.int32()),
            "star_arg_count": cast_values(grouped["star_arg_count_sum"], pa.int32()),
            "star_kwarg_count": cast_values(grouped["star_kwarg_count_sum"], pa.int32()),
            "positional_count": cast_values(grouped["positional_count_sum"], pa.int32()),
        },
        num_rows=grouped.num_rows,
    )
    return summary


def _callsite_qname_base_table(exploded: TableLike) -> TableLike:
    call_ids = _string_or_null(exploded["call_id"])
    qname_struct = exploded["qname_struct"]
    qname_vals = _string_or_null(struct_field(qname_struct, "name"))
    qname_sources = _string_or_null(struct_field(qname_struct, "source"))
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
    mask = and_(is_valid(base["call_id"]), is_valid(base["qname"]))
    mask = fill_null(mask, fill_value=False)
    return base.filter(mask)


def _callsite_fqn_base_table(exploded: TableLike) -> TableLike:
    call_ids = _string_or_null(exploded["call_id"])
    fqn_vals = _string_or_null(exploded["fqn_value"])
    qname_sources = pa.array(["fully_qualified"] * len(call_ids), type=pa.string())
    schema = pa.schema(
        [
            ("call_id", pa.string()),
            ("qname", pa.string()),
            ("qname_source", pa.string()),
        ]
    )
    base = table_from_arrays(
        schema,
        columns={"call_id": call_ids, "qname": fqn_vals, "qname_source": qname_sources},
        num_rows=len(call_ids),
    )
    mask = and_(is_valid(base["call_id"]), is_valid(base["qname"]))
    mask = fill_null(mask, fill_value=False)
    return base.filter(mask)


def _join_callsite_qname_meta(base: TableLike, cst_callsites: TableLike) -> TableLike:
    meta_cols = [
        col
        for col in (
            "call_id",
            "path",
            "call_bstart",
            "call_bend",
            "arg_count",
            "qname_source",
        )
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
    merged = coalesce(primary, meta_source)
    joined = set_or_append_column(joined, "qname_source", merged)
    return joined.drop(["qname_source__meta"])


def _join_callsite_arg_summary(base: TableLike, summary: TableLike) -> TableLike:
    if summary.num_rows == 0 or "call_id" not in summary.column_names:
        return base
    right_cols = [col for col in summary.column_names if col != "call_id"]
    joined = left_join(
        base,
        summary,
        config=JoinConfig.on_keys(
            keys=("call_id",),
            left_output=tuple(base.column_names),
            right_output=right_cols,
            output_suffix_for_right="__args",
        ),
    )
    metrics = (
        "arg_count",
        "keyword_count",
        "star_arg_count",
        "star_kwarg_count",
        "positional_count",
    )
    for metric in metrics:
        arg_col = f"{metric}__args"
        if arg_col not in joined.column_names:
            continue
        primary = joined[arg_col]
        fallback = joined[metric] if metric in joined.column_names else None
        merged = coalesce(primary, fallback) if fallback is not None else primary
        joined = set_or_append_column(joined, metric, merged)
        joined = joined.drop([arg_col])
    return joined


@cache(format="delta")
@tag(layer="normalize", artifact="dim_qualified_names", kind="table")
def dim_qualified_names(
    normalize_qname_context: NormalizeQnameContext,
    cst_callsites: TableLike | SqlFragment | None = None,
    cst_defs: TableLike | SqlFragment | None = None,
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
    _ = normalize_qname_context.ctx
    backend = (
        normalize_qname_context.normalize_execution_context.ibis_backend
        if normalize_qname_context.normalize_execution_context
        else None
    )
    register_nested_table(
        backend,
        name="libcst_files_v1",
        table=normalize_qname_context.libcst_files,
    )
    if cst_callsites is None and normalize_qname_context.libcst_files is not None:
        cst_callsites = SqlFragment("cst_callsites", libcst_callsites_sql())
    if cst_defs is None and normalize_qname_context.libcst_files is not None:
        cst_defs = SqlFragment("cst_defs", libcst_defs_sql())
    if not _requires_output(normalize_qname_context.evidence_plan, "dim_qualified_names"):
        schema = infer_schema_or_registry(QNAME_DIM_NAME)
        return empty_table(schema)
    if cst_callsites is None or cst_defs is None:
        schema = infer_schema_or_registry(QNAME_DIM_NAME)
        return empty_table(schema)
    cst_callsites_table = _materialize_fragment(backend, cst_callsites)
    cst_defs_table = _materialize_fragment(backend, cst_defs)
    callsite_qnames = _flatten_qname_names(cst_callsites_table, "callee_qnames")
    callsite_fqns = _flatten_string_list(cst_callsites_table, "callee_fqns")
    def_qnames = _flatten_qname_names(cst_defs_table, "qnames")
    def_fqns = _flatten_string_list(cst_defs_table, "def_fqns")
    combined = pa.chunked_array([callsite_qnames, callsite_fqns, def_qnames, def_fqns])
    if len(combined) == 0:
        schema = infer_schema_or_registry(QNAME_DIM_NAME)
        return empty_table(schema)

    qname_array = distinct_sorted(combined)
    qname_ids = prefixed_hash_id([qname_array], prefix="qname")
    out_schema = pa.schema([("qname_id", pa.string()), ("qname", pa.string())])
    out = table_from_arrays(
        out_schema,
        columns={"qname_id": qname_ids, "qname": qname_array},
        num_rows=len(qname_array),
    )
    schema = infer_schema_or_registry(QNAME_DIM_NAME)
    return align_table_to_schema(out, schema)


@cache(format="delta")
@tag(layer="normalize", artifact="callsite_qname_candidates", kind="table")
def callsite_qname_candidates(
    cst_callsites: TableLike | SqlFragment | None = None,
    cst_call_args: TableLike | SqlFragment | None = None,
    normalize_execution_context: NormalizeExecutionContext | None = None,
    libcst_files: TableLike | RecordBatchReaderLike | None = None,
    *,
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
      - arg_count (optional)
      - keyword_count (optional)
      - star_arg_count (optional)
      - star_kwarg_count (optional)
      - positional_count (optional)
      - qname_source (optional)

    Returns
    -------
    TableLike
        Table of callsite qualified name candidates.
    """
    if not _requires_output(evidence_plan, "callsite_qname_candidates"):
        schema = infer_schema_or_registry(CALLSITE_QNAME_CANDIDATES_NAME)
        return empty_table(schema)
    backend = normalize_execution_context.ibis_backend if normalize_execution_context else None
    register_nested_table(backend, name="libcst_files_v1", table=libcst_files)
    if cst_callsites is None and libcst_files is not None:
        cst_callsites = SqlFragment("cst_callsites", libcst_callsites_sql())
    if cst_call_args is None and libcst_files is not None:
        cst_call_args = SqlFragment("cst_call_args", libcst_call_args_sql())
    if cst_callsites is None:
        schema = infer_schema_or_registry(CALLSITE_QNAME_CANDIDATES_NAME)
        return empty_table(schema)
    cst_callsites_table = _materialize_fragment(backend, cst_callsites)
    if cst_callsites_table.num_rows == 0:
        schema = infer_schema_or_registry(CALLSITE_QNAME_CANDIDATES_NAME)
        return empty_table(schema)
    has_qnames = "callee_qnames" in cst_callsites_table.column_names
    has_fqns = "callee_fqns" in cst_callsites_table.column_names
    if not has_qnames and not has_fqns:
        schema = infer_schema_or_registry(CALLSITE_QNAME_CANDIDATES_NAME)
        return empty_table(schema)

    kernel = resolve_kernel("explode_list", ctx=ctx)
    tables: list[TableLike] = []
    if has_qnames:
        spec = ExplodeSpec(
            parent_keys=("call_id",),
            list_col="callee_qnames",
            value_col="qname_struct",
            idx_col=None,
            keep_empty=True,
        )
        exploded = kernel(cst_callsites_table, spec=spec, out_parent_col="call_id")
        base = _callsite_qname_base_table(exploded)
        joined = _join_callsite_qname_meta(base, cst_callsites_table)
        if joined.num_rows > 0:
            tables.append(joined)
    if has_fqns:
        spec = ExplodeSpec(
            parent_keys=("call_id",),
            list_col="callee_fqns",
            value_col="fqn_value",
            idx_col=None,
            keep_empty=True,
        )
        exploded = kernel(cst_callsites_table, spec=spec, out_parent_col="call_id")
        base = _callsite_fqn_base_table(exploded)
        joined = _join_callsite_qname_meta(base, cst_callsites_table)
        if joined.num_rows > 0:
            tables.append(joined)
    if not tables:
        schema = infer_schema_or_registry(CALLSITE_QNAME_CANDIDATES_NAME)
        return empty_table(schema)
    joined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    if cst_call_args is not None:
        try:
            call_args_table = _materialize_fragment(backend, cst_call_args)
        except ValueError:
            call_args_table = None
        if call_args_table is not None:
            arg_summary = _callsite_arg_summary(call_args_table)
            joined = _join_callsite_arg_summary(joined, arg_summary)

    schema = infer_schema_or_registry(CALLSITE_QNAME_CANDIDATES_NAME)
    return align_table_to_schema(joined, schema)


@cache(format="delta")
@tag(layer="normalize", artifact="ast_nodes_norm", kind="table")
def ast_nodes_norm(
    span_normalize_context: SpanNormalizeContext,
    ast_nodes: TableLike | SqlFragment | None = None,
    ast_files: TableLike | RecordBatchReaderLike | None = None,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Add byte-span columns to AST nodes for join-ready alignment.

    Returns
    -------
    TableLike
        AST nodes with bstart/bend/span_ok columns appended.
    """
    backend = span_normalize_context.ibis_backend
    register_nested_table(backend, name="ast_files_v1", table=ast_files)
    if ast_nodes is None and ast_files is not None:
        ast_nodes = SqlFragment("ast_nodes", ast_nodes_sql())
    if not _requires_output(evidence_plan, "ast_nodes_norm"):
        schema = _schema_from_source(
            ast_nodes,
            backend=backend,
            fallback="py_ast_nodes_v1",
        )
        return empty_table(schema)
    if ast_nodes is None:
        schema = _schema_from_source(
            ast_nodes,
            backend=backend,
            fallback="py_ast_nodes_v1",
        )
        return empty_table(schema)
    _require_datafusion_backend(backend)
    table = add_ast_byte_spans_ibis(
        span_normalize_context.file_line_index,
        ast_nodes,
        backend=backend,
    )
    schema = infer_schema_or_registry("py_ast_nodes_v1")
    return align_table_to_schema(table, schema, opts=SchemaInferOptions(keep_extra_columns=True))


@cache(format="delta")
@tag(layer="normalize", artifact="py_bc_instructions_norm", kind="table")
def py_bc_instructions_norm(
    span_normalize_context: SpanNormalizeContext,
    py_bc_instructions: TableLike | SqlFragment | None = None,
    bytecode_files: TableLike | RecordBatchReaderLike | None = None,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Anchor bytecode instructions to source byte spans.

    Returns
    -------
    TableLike
        Bytecode instruction table with bstart/bend/span_ok columns.
    """
    backend = span_normalize_context.ibis_backend
    register_nested_table(backend, name="bytecode_files_v1", table=bytecode_files)
    if py_bc_instructions is None and bytecode_files is not None:
        py_bc_instructions = SqlFragment(
            "py_bc_instructions",
            bytecode_instructions_sql(),
        )
    if not _requires_output(evidence_plan, "py_bc_instructions_norm"):
        schema = _schema_from_source(
            py_bc_instructions,
            backend=backend,
            fallback="py_bc_instructions_v1",
        )
        return empty_table(schema)
    if py_bc_instructions is None:
        schema = _schema_from_source(
            py_bc_instructions,
            backend=backend,
            fallback="py_bc_instructions_v1",
        )
        return empty_table(schema)
    _require_datafusion_backend(backend)
    table = anchor_instructions_ibis(
        span_normalize_context.file_line_index,
        py_bc_instructions,
        backend=backend,
    )
    schema = infer_schema_or_registry("py_bc_instructions_v1")
    return align_table_to_schema(table, schema, opts=SchemaInferOptions(keep_extra_columns=True))


@cache(format="delta")
@tag(layer="normalize", artifact="py_bc_blocks_norm", kind="table")
def py_bc_blocks_norm(
    normalize_rule_compilation: NormalizeRuleCompilation,
    normalize_execution_context: NormalizeExecutionContext,
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
        normalize_execution_context=normalize_execution_context,
    )


@cache(format="delta")
@tag(layer="normalize", artifact="py_bc_cfg_edges_norm", kind="table")
def py_bc_cfg_edges_norm(
    normalize_rule_compilation: NormalizeRuleCompilation,
    normalize_execution_context: NormalizeExecutionContext,
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
        normalize_execution_context=normalize_execution_context,
    )


@cache(format="delta")
@tag(layer="normalize", artifact="py_bc_def_use_events", kind="table")
def py_bc_def_use_events(
    normalize_rule_compilation: NormalizeRuleCompilation,
    normalize_execution_context: NormalizeExecutionContext,
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
        normalize_execution_context=normalize_execution_context,
    )


@cache(format="delta")
@tag(layer="normalize", artifact="py_bc_reaching_defs", kind="table")
def py_bc_reaching_defs(
    normalize_rule_compilation: NormalizeRuleCompilation,
    normalize_execution_context: NormalizeExecutionContext,
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
        normalize_execution_context=normalize_execution_context,
    )


@cache(format="delta")
@tag(layer="normalize", artifact="type_exprs_norm", kind="table")
def type_exprs_norm(
    normalize_rule_compilation: NormalizeRuleCompilation,
    normalize_execution_context: NormalizeExecutionContext,
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
        normalize_execution_context=normalize_execution_context,
    )


@cache(format="delta")
@tag(layer="normalize", artifact="types_norm", kind="table")
def types_norm(
    normalize_rule_compilation: NormalizeRuleCompilation,
    normalize_execution_context: NormalizeExecutionContext,
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
        normalize_execution_context=normalize_execution_context,
    )


@cache()
@tag(layer="normalize", artifact="diagnostics_fragment_inputs", kind="object")
def diagnostics_fragment_inputs(
    cst_parse_errors: IbisPlanSource | None = None,
    ts_errors: IbisPlanSource | None = None,
    ts_missing: IbisPlanSource | None = None,
    scip_diagnostics: IbisPlanSource | None = None,
    scip_documents: IbisPlanSource | None = None,
) -> DiagnosticsFragmentInputs:
    """Bundle raw diagnostic fragment inputs.

    Returns
    -------
    DiagnosticsFragmentInputs
        Optional diagnostic fragments for normalization.
    """
    return DiagnosticsFragmentInputs(
        cst_parse_errors=cst_parse_errors,
        ts_errors=ts_errors,
        ts_missing=ts_missing,
        scip_diagnostics=scip_diagnostics,
        scip_documents=scip_documents,
    )


@cache()
@tag(layer="normalize", artifact="diagnostics_table_inputs", kind="object")
def diagnostics_table_inputs(
    libcst_files: TableLike | RecordBatchReaderLike | None = None,
    tree_sitter_files: TableLike | RecordBatchReaderLike | None = None,
) -> DiagnosticsTableInputs:
    """Bundle nested table inputs for diagnostics normalization.

    Returns
    -------
    DiagnosticsTableInputs
        Nested tables required for diagnostics normalization.
    """
    return DiagnosticsTableInputs(
        libcst_files=libcst_files,
        tree_sitter_files=tree_sitter_files,
    )


@cache()
@tag(layer="normalize", artifact="diagnostics_sources", kind="object")
def diagnostics_sources(
    diagnostics_fragment_inputs: DiagnosticsFragmentInputs,
    diagnostics_table_inputs: DiagnosticsTableInputs,
    normalize_execution_context: NormalizeExecutionContext | None = None,
) -> DiagnosticsSources:
    """Bundle diagnostic source tables.

    Returns
    -------
    DiagnosticsSources
        Diagnostic source tables bundle.
    """
    backend = normalize_execution_context.ibis_backend if normalize_execution_context else None
    register_nested_table(
        backend,
        name="libcst_files_v1",
        table=diagnostics_table_inputs.libcst_files,
    )
    register_nested_table(
        backend,
        name="tree_sitter_files_v1",
        table=diagnostics_table_inputs.tree_sitter_files,
    )
    cst_parse_errors = diagnostics_fragment_inputs.cst_parse_errors
    ts_errors = diagnostics_fragment_inputs.ts_errors
    ts_missing = diagnostics_fragment_inputs.ts_missing
    scip_diagnostics = diagnostics_fragment_inputs.scip_diagnostics
    scip_documents = diagnostics_fragment_inputs.scip_documents
    if cst_parse_errors is None and diagnostics_table_inputs.libcst_files is not None:
        cst_parse_errors = SqlFragment("cst_parse_errors", libcst_parse_errors_sql())
    if ts_errors is None and diagnostics_table_inputs.tree_sitter_files is not None:
        ts_errors = SqlFragment("ts_errors", tree_sitter_errors_sql())
    if ts_missing is None and diagnostics_table_inputs.tree_sitter_files is not None:
        ts_missing = SqlFragment("ts_missing", tree_sitter_missing_sql())
    if scip_diagnostics is None and backend is not None:
        scip_diagnostics = SqlFragment("scip_diagnostics", scip_diagnostics_sql())
    if scip_documents is None and backend is not None:
        scip_documents = SqlFragment("scip_documents", scip_documents_sql())
    return DiagnosticsSources(
        cst_parse_errors=cst_parse_errors,
        ts_errors=ts_errors,
        ts_missing=ts_missing,
        scip_diagnostics=scip_diagnostics,
        scip_documents=scip_documents,
    )


@cache(format="delta")
@tag(layer="normalize", artifact="diagnostics_norm", kind="table")
def diagnostics_norm(
    normalize_rule_compilation: NormalizeRuleCompilation,
    normalize_execution_context: NormalizeExecutionContext,
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
        normalize_execution_context=normalize_execution_context,
    )


@cache()
@tag(layer="normalize", artifact="repo_text_index", kind="object")
def repo_text_index(
    repo_root: str,
    repo_files_extract: TableLike,
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
    return build_repo_text_index(repo_root=repo_root, repo_files=repo_files_extract, ctx=ctx)


@cache()
@extract_fields(
    {
        "scip_occurrences_norm": TableLike,
        "scip_span_errors": TableLike,
    }
)
@tag(layer="normalize", artifact="scip_occurrences_norm_bundle", kind="bundle")
def scip_occurrences_norm_bundle(
    span_normalize_context: SpanNormalizeContext,
    scip_documents: TableLike | SqlFragment | None = None,
    scip_occurrences: TableLike | SqlFragment | None = None,
    evidence_plan: EvidencePlan | None = None,
) -> dict[str, TableLike]:
    """Convert SCIP occurrences into byte offsets.

    Returns
    -------
    dict[str, TableLike]
        Bundle with normalized occurrences and span errors.
    """
    backend = span_normalize_context.ibis_backend
    if scip_documents is None and backend is not None:
        scip_documents = SqlFragment("scip_documents", scip_documents_sql())
    if scip_occurrences is None and backend is not None:
        scip_occurrences = SqlFragment("scip_occurrences", scip_occurrences_sql())
    fallback_schema = _schema_from_source(
        scip_occurrences,
        backend=backend,
        fallback="scip_occurrences_v1",
    )
    if not _requires_any(
        evidence_plan,
        ("scip_occurrences_norm", "scip_span_errors", "scip_occurrences"),
    ):
        return {
            "scip_occurrences_norm": _empty_scip_occurrences_norm(fallback_schema),
            "scip_span_errors": span_error_table([]),
        }
    if scip_documents is None or scip_occurrences is None:
        return {
            "scip_occurrences_norm": _empty_scip_occurrences_norm(fallback_schema),
            "scip_span_errors": span_error_table([]),
        }
    _require_datafusion_backend(backend)
    stats = _scip_position_encoding_stats(scip_documents, backend=backend)
    if stats is not None:
        _record_scip_position_encoding_stats(span_normalize_context.ctx, stats)
        missing = int(stats.get("missing_position_encoding", 0) or 0)
        invalid = int(stats.get("invalid_position_encoding", 0) or 0)
        encodings = stats.get("valid_position_encodings") or []
        invalid_values = stats.get("invalid_position_encoding_values") or []
        if missing or invalid or len(encodings) > 1:
            LOGGER.warning(
                "SCIP position encoding guard: missing=%d invalid=%d encodings=%s invalid_values=%s",
                missing,
                invalid,
                encodings,
                invalid_values,
            )
    occ, errs = add_scip_occurrence_byte_spans_ibis(
        span_normalize_context.file_line_index,
        scip_documents,
        scip_occurrences,
        backend=backend,
    )
    schema = infer_schema_or_registry("scip_occurrences_v1")
    occ = align_table_to_schema(occ, schema, opts=SchemaInferOptions(keep_extra_columns=True))
    return {"scip_occurrences_norm": occ, "scip_span_errors": errs}


@cache(format="delta")
@tag(layer="normalize", artifact="cst_imports_norm", kind="table")
def cst_imports_norm(
    cst_imports: TableLike | SqlFragment | None = None,
    normalize_execution_context: NormalizeExecutionContext | None = None,
    libcst_files: TableLike | RecordBatchReaderLike | None = None,
    *,
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
    backend = normalize_execution_context.ibis_backend if normalize_execution_context else None
    register_nested_table(backend, name="libcst_files_v1", table=libcst_files)
    if cst_imports is None and libcst_files is not None:
        cst_imports = SqlFragment("cst_imports", libcst_imports_sql())
    if cst_imports is None:
        return empty_table(dataset_schema(dataset_name_from_alias("cst_imports_norm")))
    if not _requires_any(evidence_plan, ("cst_imports_norm", "cst_imports")):
        schema = _schema_from_source(
            cst_imports,
            backend=backend,
            fallback="py_cst_imports_v1",
        )
        return empty_table(schema)
    table = _materialize_fragment(backend, cst_imports)
    return normalize_cst_imports_spans(py_cst_imports=table)


@cache(format="delta")
@tag(layer="normalize", artifact="cst_defs_norm", kind="table")
def cst_defs_norm(
    cst_defs: TableLike | SqlFragment | None = None,
    normalize_execution_context: NormalizeExecutionContext | None = None,
    libcst_files: TableLike | RecordBatchReaderLike | None = None,
    *,
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
    backend = normalize_execution_context.ibis_backend if normalize_execution_context else None
    register_nested_table(backend, name="libcst_files_v1", table=libcst_files)
    if cst_defs is None and libcst_files is not None:
        cst_defs = SqlFragment("cst_defs", libcst_defs_sql())
    if cst_defs is None:
        return empty_table(dataset_schema(dataset_name_from_alias("cst_defs_norm")))
    if not _requires_any(evidence_plan, ("cst_defs_norm", "cst_defs")):
        schema = _schema_from_source(
            cst_defs,
            backend=backend,
            fallback="py_cst_defs_v1",
        )
        return empty_table(schema)
    table = _materialize_fragment(backend, cst_defs)
    return normalize_cst_defs_spans(py_cst_defs=table)


@cache()
@tag(layer="normalize", artifact="normalize_outputs_group_a", kind="object")
def normalize_outputs_group_a(
    cst_imports_norm: TableLike,
    cst_defs_norm: TableLike,
    scip_occurrences_norm: TableLike,
    callsite_qname_candidates: TableLike,
) -> Mapping[str, TableLike]:
    """Return group A normalized outputs used in incremental updates.

    Returns
    -------
    Mapping[str, TableLike]
        Group A normalized output tables.
    """
    return {
        "cst_imports_norm": cst_imports_norm,
        "cst_defs_norm": cst_defs_norm,
        "scip_occurrences_norm": scip_occurrences_norm,
        "callsite_qname_candidates": callsite_qname_candidates,
    }


@cache()
@tag(layer="normalize", artifact="normalize_outputs_group_b", kind="object")
def normalize_outputs_group_b(
    dim_qualified_names: TableLike,
    type_exprs_norm: TableLike,
    types_norm: TableLike,
    diagnostics_norm: TableLike,
) -> Mapping[str, TableLike]:
    """Return group B normalized outputs used in incremental updates.

    Returns
    -------
    Mapping[str, TableLike]
        Group B normalized output tables.
    """
    return {
        "dim_qualified_names": dim_qualified_names,
        "type_exprs_norm": type_exprs_norm,
        "types_norm": types_norm,
        "diagnostics_norm": diagnostics_norm,
    }


@cache()
@tag(layer="normalize", artifact="normalize_outputs_bundle", kind="object")
def normalize_outputs_bundle(
    normalize_outputs_group_a: Mapping[str, TableLike],
    normalize_outputs_group_b: Mapping[str, TableLike],
) -> Mapping[str, TableLike]:
    """Return a mapping of normalized outputs used in incremental updates.

    Returns
    -------
    Mapping[str, TableLike]
        All normalized output tables for incremental updates.
    """
    return {**normalize_outputs_group_a, **normalize_outputs_group_b}
