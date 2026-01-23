"""Hamilton normalization stage functions."""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import cache as memoize
from typing import TYPE_CHECKING, Protocol, cast
from uuid import uuid4

import ibis
import pyarrow as pa
from hamilton.function_modifiers import cache, extract_fields, tag
from ibis.backends import BaseBackend
from ibis.expr.types import Table

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.expr_types import ExplodeSpec
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.schema.build import empty_table, set_or_append_column
from arrowdsl.schema.semantic_types import SPAN_STORAGE
from datafusion_engine.kernel_registry import resolve_kernel
from datafusion_engine.nested_tables import (
    ViewReference,
    materialize_view_reference,
    register_nested_table,
)
from datafusion_engine.runtime import (
    AdapterExecutionPolicy,
    ExecutionLabel,
    align_table_to_schema,
    dataset_schema_from_context,
)
from datafusion_engine.sql_options import sql_options_for_profile
from extract.evidence_plan import EvidencePlan
from ibis_engine.builtin_udfs import prefixed_hash64
from ibis_engine.catalog import IbisPlanSource
from ibis_engine.execution import materialize_ibis_plan
from ibis_engine.execution_factory import ibis_execution_from_ctx
from ibis_engine.plan import IbisPlan
from ibis_engine.registry import datafusion_context
from ibis_engine.sources import SourceToIbisOptions, source_to_ibis
from normalize.catalog import IbisPlanCatalog, NormalizeCatalogInputs
from normalize.catalog import normalize_plan_catalog as build_normalize_plan_catalog
from normalize.ibis_api import DiagnosticsSources
from normalize.ibis_spans import (
    add_ast_span_struct_ibis,
    add_scip_occurrence_span_struct_ibis,
    anchor_instructions_span_struct_ibis,
    normalize_cst_defs_span_struct_ibis,
    normalize_cst_imports_span_struct_ibis,
)
from normalize.registry_runtime import (
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
from normalize.runtime import NormalizeRuntime, build_normalize_runtime
from normalize.schemas import SPAN_ERROR_SCHEMA
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
        count = _coerce_int(raw_count)
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


def _coerce_int(value: object | None) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        with contextlib.suppress(ValueError):
            return int(value)
    return 0


def _scip_position_encoding_stats(
    scip_documents: TableLike | ViewReference,
    *,
    backend: BaseBackend,
) -> dict[str, object] | None:
    if not isinstance(scip_documents, ViewReference):
        msg = "SCIP position encoding stats require a DataFusion view reference."
        raise TypeError(msg)
    query = (
        "SELECT position_encoding, COUNT(*) AS doc_count "
        f"FROM {scip_documents.name} GROUP BY position_encoding"
    )
    ctx = datafusion_context(backend)
    table = ctx.sql_with_options(query, sql_options_for_profile(None)).to_arrow_table()
    return _posenc_stats_from_rows(table.to_pylist())


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
    source: TableLike | ViewReference,
) -> TableLike:
    if not isinstance(source, ViewReference):
        return source
    return materialize_view_reference(backend, source)


def _materialize_ibis_table(table: Table, *, runtime: NormalizeRuntime) -> TableLike:
    execution = ibis_execution_from_ctx(runtime.execution_ctx, backend=runtime.ibis_backend)
    plan = IbisPlan(expr=table)
    return materialize_ibis_plan(plan, execution=execution)


def _ibis_table_from_source(
    backend: BaseBackend,
    source: TableLike | ViewReference,
    *,
    name: str,
) -> Table:
    if isinstance(source, ViewReference):
        return backend.table(source.name)
    plan = source_to_ibis(source, options=SourceToIbisOptions(backend=backend, name=name))
    return plan.expr


class _DatafusionQuery(Protocol):
    def schema(self) -> object: ...

    def to_arrow_table(self) -> pa.Table: ...


class _DatafusionContext(Protocol):
    def sql(self, query: str) -> _DatafusionQuery: ...

    def sql_with_options(self, query: str, options: object) -> _DatafusionQuery: ...

    def register_record_batches(self, name: str, batches: list[list[pa.RecordBatch]]) -> None: ...

    def deregister_table(self, name: str) -> None: ...


def _schema_from_fragment(
    fragment: ViewReference,
    *,
    backend: BaseBackend | None,
) -> object | None:
    if backend is None:
        return None
    return backend.table(fragment.name).schema()


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _nullif_empty(expr: str) -> str:
    return f"NULLIF(CAST({expr} AS STRING), '')"


def _ensure_arrow_table(value: TableLike | RecordBatchReaderLike) -> pa.Table:
    if isinstance(value, RecordBatchReaderLike):
        return pa.Table.from_batches(list(value))
    return value if isinstance(value, pa.Table) else pa.table(value)


@contextlib.contextmanager
def _temporary_table(
    ctx: _DatafusionContext,
    table: TableLike | RecordBatchReaderLike,
    *,
    name_prefix: str,
) -> Iterator[str]:
    name = f"_{name_prefix}_{uuid4().hex}"
    resolved = _ensure_arrow_table(table)
    ctx.register_record_batches(name, [resolved.to_batches()])
    try:
        yield name
    finally:
        with contextlib.suppress(KeyError, RuntimeError, ValueError, TypeError):
            ctx.deregister_table(name)


def _schema_from_source(
    source: TableLike | ViewReference | None,
    *,
    backend: BaseBackend | None,
    fallback: str,
) -> pa.Schema:
    fallback_schema = dataset_schema_from_context(fallback)
    if source is None:
        return fallback_schema
    if isinstance(source, ViewReference):
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

    span_errors: IbisPlanSource | None = None


@dataclass(frozen=True)
class NormalizeQnameContext:
    """Context inputs for qualified name normalization."""

    normalize_execution_context: NormalizeExecutionContext | None
    libcst_files: TableLike | RecordBatchReaderLike | None
    ctx: ExecutionContext
    evidence_plan: EvidencePlan | None


@dataclass(frozen=True)
class CallsiteQnameSources:
    """Bundle inputs for callsite qualified name candidates."""

    cst_callsites: TableLike | ViewReference | None = None
    cst_call_args: TableLike | ViewReference | None = None
    libcst_files: TableLike | RecordBatchReaderLike | None = None


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
    runtime: NormalizeRuntime
    ctx: ExecutionContext

    @property
    def ibis_backend(self) -> BaseBackend:
        """Return the Ibis backend for span normalization."""
        return self.runtime.ibis_backend


@dataclass(frozen=True)
class NormalizeExecutionContext:
    """Execution settings for normalize compilation and execution."""

    ctx: ExecutionContext
    execution_policy: AdapterExecutionPolicy
    runtime: NormalizeRuntime

    @property
    def ibis_backend(self) -> BaseBackend:
        """Return the Ibis backend for normalize execution."""
        return self.runtime.ibis_backend


@cache()
@tag(layer="normalize", artifact="normalize_execution_context", kind="object")
def normalize_execution_context(
    ctx: ExecutionContext,
    adapter_execution_policy: AdapterExecutionPolicy,
) -> NormalizeExecutionContext:
    """Bundle execution settings for normalize pipelines.

    Returns
    -------
    NormalizeExecutionContext
        Execution settings for normalize compilation and execution.
    """
    runtime = build_normalize_runtime(ctx)
    _require_datafusion_backend(runtime.ibis_backend)
    return NormalizeExecutionContext(
        ctx=ctx,
        execution_policy=adapter_execution_policy,
        runtime=runtime,
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
        cst_type_exprs = ViewReference("cst_type_exprs")
    if scip_symbol_information is None and backend is not None:
        scip_symbol_information = ViewReference("scip_symbol_information")
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
        py_bc_blocks = ViewReference("py_bc_blocks")
    if py_bc_cfg_edges is None and bytecode_files is not None:
        py_bc_cfg_edges = ViewReference("py_bc_cfg_edges")
    if py_bc_code_units is None and bytecode_files is not None:
        py_bc_code_units = ViewReference("py_bc_code_units")
    if py_bc_instructions is None and bytecode_files is not None:
        py_bc_instructions = ViewReference("py_bc_instructions")
    return NormalizeBytecodeSources(
        py_bc_blocks=py_bc_blocks,
        py_bc_cfg_edges=py_bc_cfg_edges,
        py_bc_code_units=py_bc_code_units,
        py_bc_instructions=py_bc_instructions,
    )


@cache()
@tag(layer="normalize", artifact="normalize_span_sources", kind="object")
def normalize_span_sources(
    span_errors: TableLike | None = None,
) -> NormalizeSpanSources:
    """Bundle span-related normalize inputs.

    Returns
    -------
    NormalizeSpanSources
        Span-related inputs for normalize rule compilation.
    """
    return NormalizeSpanSources(span_errors=span_errors)


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
    normalize_execution_context: NormalizeExecutionContext,
) -> SpanNormalizeContext:
    """Bundle inputs for span normalization helpers.

    Returns
    -------
    SpanNormalizeContext
        Shared inputs for span normalization operations.
    """
    ibis_backend = normalize_execution_context.ibis_backend
    _require_datafusion_backend(ibis_backend)
    return SpanNormalizeContext(
        file_line_index=file_line_index,
        runtime=normalize_execution_context.runtime,
        ctx=normalize_execution_context.ctx,
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
            runtime=normalize_execution_context.runtime,
        ),
    ).good


def _empty_scip_occurrences_norm(schema: pa.Schema) -> TableLike:
    table = empty_table(schema)
    table = set_or_append_column(table, "bstart", pa.array([], type=pa.int64()))
    table = set_or_append_column(table, "bend", pa.array([], type=pa.int64()))
    table = set_or_append_column(table, "span", pa.array([], type=SPAN_STORAGE))
    table = set_or_append_column(table, "span_ok", pa.array([], type=pa.bool_()))
    table = set_or_append_column(table, "enc_bstart", pa.array([], type=pa.int64()))
    table = set_or_append_column(table, "enc_bend", pa.array([], type=pa.int64()))
    return set_or_append_column(table, "span_id", pa.array([], type=pa.string()))


def _callsite_arg_summary(call_args: TableLike, *, ctx: _DatafusionContext) -> TableLike:
    if call_args.num_rows == 0 or "call_id" not in call_args.column_names:
        return empty_table(CALLSITE_ARG_SUMMARY_SCHEMA)
    keyword_expr = (
        _nullif_empty("keyword") if "keyword" in call_args.column_names else "CAST(NULL AS STRING)"
    )
    star_expr = (
        _nullif_empty("star") if "star" in call_args.column_names else "CAST(NULL AS STRING)"
    )
    call_id_expr = _nullif_empty("call_id")
    positional_expr = (
        f"CASE WHEN {keyword_expr} IS NULL "
        f"AND COALESCE({star_expr}, '') NOT IN ('*', '**') "
        "THEN 1 ELSE 0 END"
    )
    with _temporary_table(ctx, call_args, name_prefix="call_args") as table_name:
        sql = (
            "WITH base AS ("
            f"SELECT {call_id_expr} AS call_id, "
            "1 AS arg_count, "
            f"CASE WHEN {keyword_expr} IS NOT NULL THEN 1 ELSE 0 END AS keyword_count, "
            f"CASE WHEN {star_expr} = '*' THEN 1 ELSE 0 END AS star_arg_count, "
            f"CASE WHEN {star_expr} = '**' THEN 1 ELSE 0 END AS star_kwarg_count, "
            f"{positional_expr} AS positional_count "
            f"FROM {_sql_identifier(table_name)}"
            ") "
            "SELECT call_id, "
            "SUM(arg_count) AS arg_count, "
            "SUM(keyword_count) AS keyword_count, "
            "SUM(star_arg_count) AS star_arg_count, "
            "SUM(star_kwarg_count) AS star_kwarg_count, "
            "SUM(positional_count) AS positional_count "
            "FROM base "
            "WHERE call_id IS NOT NULL "
            "GROUP BY call_id"
        )
        grouped = ctx.sql_with_options(sql, sql_options_for_profile(None)).to_arrow_table()
    if grouped.num_rows == 0:
        return empty_table(CALLSITE_ARG_SUMMARY_SCHEMA)
    return grouped


def _callsite_qname_base_table(exploded: TableLike, *, ctx: _DatafusionContext) -> TableLike:
    with _temporary_table(ctx, exploded, name_prefix="qname_exploded") as table_name:
        call_id_expr = _nullif_empty("call_id")
        qname_expr = _nullif_empty("get_field(qname_struct, 'name')")
        source_expr = _nullif_empty("get_field(qname_struct, 'source')")
        sql = (
            "WITH base AS ("
            f"SELECT {call_id_expr} AS call_id, "
            f"{qname_expr} AS qname, "
            f"{source_expr} AS qname_source "
            f"FROM {_sql_identifier(table_name)}"
            ") "
            "SELECT call_id, qname, qname_source "
            "FROM base WHERE call_id IS NOT NULL AND qname IS NOT NULL"
        )
        return ctx.sql_with_options(sql, sql_options_for_profile(None)).to_arrow_table()


def _callsite_fqn_base_table(exploded: TableLike, *, ctx: _DatafusionContext) -> TableLike:
    qname_source = _sql_literal("fully_qualified")
    with _temporary_table(ctx, exploded, name_prefix="fqn_exploded") as table_name:
        call_id_expr = _nullif_empty("call_id")
        qname_expr = _nullif_empty("fqn_value")
        sql = (
            "WITH base AS ("
            f"SELECT {call_id_expr} AS call_id, "
            f"{qname_expr} AS qname, "
            f"{qname_source} AS qname_source "
            f"FROM {_sql_identifier(table_name)}"
            ") "
            "SELECT call_id, qname, qname_source "
            "FROM base WHERE call_id IS NOT NULL AND qname IS NOT NULL"
        )
        return ctx.sql_with_options(sql, sql_options_for_profile(None)).to_arrow_table()


def _join_callsite_qname_meta(
    base: TableLike,
    cst_callsites: TableLike,
    *,
    ctx: _DatafusionContext,
) -> TableLike:
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
    base_cols = base.column_names
    if "call_id" not in base_cols or "qname" not in base_cols:
        return base
    with (
        _temporary_table(ctx, base, name_prefix="qname_base") as base_name,
        _temporary_table(ctx, cst_callsites, name_prefix="qname_meta") as meta_name,
    ):
        selections = [
            "base.call_id AS call_id",
            "base.qname AS qname",
        ]
        if "qname_source" in meta_cols:
            selections.append(
                "COALESCE("
                f"{_nullif_empty('base.qname_source')}, "
                f"{_nullif_empty('meta.qname_source')}"
                ") AS qname_source"
            )
        else:
            selections.append(f"{_nullif_empty('base.qname_source')} AS qname_source")
        for col in meta_cols:
            if col in {"call_id", "qname_source"}:
                continue
            selections.append(f"meta.{_sql_identifier(col)} AS {_sql_identifier(col)}")
        sql = (
            f"SELECT {', '.join(selections)} "
            f"FROM {_sql_identifier(base_name)} AS base "
            f"LEFT JOIN {_sql_identifier(meta_name)} AS meta "
            "ON base.call_id = meta.call_id"
        )
        return ctx.sql_with_options(sql, sql_options_for_profile(None)).to_arrow_table()


def _join_callsite_arg_summary(
    base: TableLike,
    summary: TableLike,
    *,
    ctx: _DatafusionContext,
) -> TableLike:
    if summary.num_rows == 0 or "call_id" not in summary.column_names:
        return base
    metrics = (
        "arg_count",
        "keyword_count",
        "star_arg_count",
        "star_kwarg_count",
        "positional_count",
    )
    with (
        _temporary_table(ctx, base, name_prefix="qname_base") as base_name,
        _temporary_table(ctx, summary, name_prefix="qname_summary") as summary_name,
    ):
        selections: list[str] = []
        base_cols = base.column_names
        for col in base_cols:
            if col in metrics:
                continue
            selections.append(f"base.{_sql_identifier(col)} AS {_sql_identifier(col)}")
        for metric in metrics:
            base_has = metric in base_cols
            summary_has = metric in summary.column_names
            if summary_has and base_has:
                selections.append(f"COALESCE(summary.{metric}, base.{metric}) AS {metric}")
            elif summary_has:
                selections.append(f"summary.{metric} AS {metric}")
            elif base_has:
                selections.append(f"base.{metric} AS {metric}")
        sql = (
            f"SELECT {', '.join(selections)} "
            f"FROM {_sql_identifier(base_name)} AS base "
            f"LEFT JOIN {_sql_identifier(summary_name)} AS summary "
            "ON base.call_id = summary.call_id"
        )
        return ctx.sql_with_options(sql, sql_options_for_profile(None)).to_arrow_table()


@cache(format="delta")
@tag(layer="normalize", artifact="dim_qualified_names", kind="table")
def dim_qualified_names(
    normalize_qname_context: NormalizeQnameContext,
    cst_callsites: TableLike | ViewReference | None = None,
    cst_defs: TableLike | ViewReference | None = None,
) -> TableLike:
    """Build a dimension table of qualified names from CST extraction.

    Expected output columns:
      - qname_id (stable)
      - qname (string)

    Returns
    -------
    TableLike
        Qualified name dimension table.

    Raises
    ------
    ValueError
        Raised when an Ibis backend is required but unavailable.
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
    cst_callsites, cst_defs = _resolve_cst_sources(
        cst_callsites,
        cst_defs,
        libcst_files=normalize_qname_context.libcst_files,
    )
    if not _requires_output(normalize_qname_context.evidence_plan, "dim_qualified_names"):
        schema = dataset_schema_from_context(QNAME_DIM_NAME)
        return empty_table(schema)
    if cst_callsites is None or cst_defs is None:
        schema = dataset_schema_from_context(QNAME_DIM_NAME)
        return empty_table(schema)
    if backend is None:
        msg = "Qualified name normalization requires an Ibis backend."
        raise ValueError(msg)
    _require_datafusion_backend(backend)

    callsites_expr = _ibis_table_from_source(backend, cst_callsites, name="cst_callsites")
    defs_expr = _ibis_table_from_source(backend, cst_defs, name="cst_defs")
    sources = _qname_sources(callsites_expr, defs_expr)
    if not sources:
        schema = dataset_schema_from_context(QNAME_DIM_NAME)
        return empty_table(schema)
    combined = _union_tables(sources)
    combined = combined.filter(combined.qname.notnull())
    distinct_qnames = combined.distinct()
    qname_id = prefixed_hash64(ibis.literal("qname"), distinct_qnames.qname.cast("string"))
    result = distinct_qnames.mutate(qname_id=qname_id).select("qname_id", "qname").order_by("qname")
    schema = dataset_schema_from_context(QNAME_DIM_NAME)
    return align_table_to_schema(result.to_pyarrow(), schema=schema)


def _resolve_cst_sources(
    cst_callsites: TableLike | ViewReference | None,
    cst_defs: TableLike | ViewReference | None,
    *,
    libcst_files: TableLike | RecordBatchReaderLike | None,
) -> tuple[TableLike | ViewReference | None, TableLike | ViewReference | None]:
    if libcst_files is None:
        return cst_callsites, cst_defs
    resolved_callsites = cst_callsites or ViewReference("cst_callsites")
    resolved_defs = cst_defs or ViewReference("cst_defs")
    return resolved_callsites, resolved_defs


def _struct_qnames(table: Table, *, list_col: str, field: str) -> Table | None:
    if list_col not in table.columns:
        return None
    unnested = table.unnest(list_col)
    struct_col = unnested[list_col]
    return unnested.select(qname=struct_col[field])


def _scalar_qnames(table: Table, *, list_col: str) -> Table | None:
    if list_col not in table.columns:
        return None
    unnested = table.unnest(list_col)
    return unnested.select(qname=unnested[list_col])


def _qname_sources(callsites_expr: Table, defs_expr: Table) -> list[Table]:
    return [
        table
        for table in (
            _struct_qnames(callsites_expr, list_col="callee_qnames", field="name"),
            _scalar_qnames(callsites_expr, list_col="callee_fqns"),
            _struct_qnames(defs_expr, list_col="qnames", field="name"),
            _scalar_qnames(defs_expr, list_col="def_fqns"),
        )
        if table is not None
    ]


def _union_tables(tables: Sequence[Table]) -> Table:
    combined = tables[0]
    for table in tables[1:]:
        combined = combined.union(table, distinct=False)
    return combined


def callsite_qname_sources(
    cst_callsites: TableLike | ViewReference | None = None,
    cst_call_args: TableLike | ViewReference | None = None,
    libcst_files: TableLike | RecordBatchReaderLike | None = None,
) -> CallsiteQnameSources:
    """Bundle callsite qualified name source inputs.

    Returns
    -------
    CallsiteQnameSources
        Bundled callsite inputs for candidate generation.
    """
    return CallsiteQnameSources(
        cst_callsites=cst_callsites,
        cst_call_args=cst_call_args,
        libcst_files=libcst_files,
    )


@cache(format="delta")
@tag(layer="normalize", artifact="callsite_qname_candidates", kind="table")
def callsite_qname_candidates(
    sources: CallsiteQnameSources,
    normalize_execution_context: NormalizeExecutionContext | None = None,
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

    Raises
    ------
    ValueError
        Raised when the DataFusion backend is unavailable.
    """
    if not _requires_output(evidence_plan, "callsite_qname_candidates"):
        schema = dataset_schema_from_context(CALLSITE_QNAME_CANDIDATES_NAME)
        return empty_table(schema)
    backend = normalize_execution_context.ibis_backend if normalize_execution_context else None
    if backend is None:
        msg = "Callsite qualified-name candidates require a DataFusion backend."
        raise ValueError(msg)
    _require_datafusion_backend(backend)
    df_ctx = cast("_DatafusionContext", datafusion_context(backend))
    cst_callsites, cst_call_args = _resolve_callsite_sources(sources)
    register_nested_table(backend, name="libcst_files_v1", table=sources.libcst_files)
    if cst_callsites is None:
        schema = dataset_schema_from_context(CALLSITE_QNAME_CANDIDATES_NAME)
        return empty_table(schema)
    cst_callsites_table = _materialize_fragment(backend, cst_callsites)
    if cst_callsites_table.num_rows == 0:
        schema = dataset_schema_from_context(CALLSITE_QNAME_CANDIDATES_NAME)
        return empty_table(schema)
    tables = _callsite_candidate_tables(cst_callsites_table, ctx=ctx, df_ctx=df_ctx)
    if not tables:
        schema = dataset_schema_from_context(CALLSITE_QNAME_CANDIDATES_NAME)
        return empty_table(schema)
    joined = _concat_tables(tables)
    joined = _maybe_join_call_args(
        joined,
        backend=backend,
        cst_call_args=cst_call_args,
        df_ctx=df_ctx,
    )

    schema = dataset_schema_from_context(CALLSITE_QNAME_CANDIDATES_NAME)
    return align_table_to_schema(joined, schema=schema)


def _resolve_callsite_sources(
    sources: CallsiteQnameSources,
) -> tuple[TableLike | ViewReference | None, TableLike | ViewReference | None]:
    cst_callsites = sources.cst_callsites
    cst_call_args = sources.cst_call_args
    if cst_callsites is None and sources.libcst_files is not None:
        cst_callsites = ViewReference("cst_callsites")
    if cst_call_args is None and sources.libcst_files is not None:
        cst_call_args = ViewReference("cst_call_args")
    return cst_callsites, cst_call_args


def _callsite_candidate_tables(
    cst_callsites_table: TableLike,
    *,
    ctx: ExecutionContext,
    df_ctx: _DatafusionContext,
) -> list[TableLike]:
    if not isinstance(cst_callsites_table, pa.Table):
        return []
    table = cast("pa.Table", cst_callsites_table)
    has_qnames = "callee_qnames" in table.column_names
    has_fqns = "callee_fqns" in table.column_names
    if not has_qnames and not has_fqns:
        return []
    kernel = resolve_kernel("explode_list", ctx=ctx)
    tables: list[TableLike] = []
    if has_qnames:
        tables.extend(_callsite_qname_tables(kernel, table, df_ctx=df_ctx))
    if has_fqns:
        tables.extend(_callsite_fqn_tables(kernel, table, df_ctx=df_ctx))
    return tables


def _callsite_qname_tables(
    kernel: Callable[..., TableLike],
    cst_callsites_table: TableLike,
    *,
    df_ctx: _DatafusionContext,
) -> list[TableLike]:
    spec = ExplodeSpec(
        parent_keys=("call_id",),
        list_col="callee_qnames",
        value_col="qname_struct",
        idx_col=None,
        keep_empty=True,
    )
    exploded = kernel(cst_callsites_table, spec=spec, out_parent_col="call_id")
    base = _callsite_qname_base_table(exploded, ctx=df_ctx)
    joined = _join_callsite_qname_meta(base, cst_callsites_table, ctx=df_ctx)
    return [joined] if joined.num_rows > 0 else []


def _callsite_fqn_tables(
    kernel: Callable[..., TableLike],
    cst_callsites_table: TableLike,
    *,
    df_ctx: _DatafusionContext,
) -> list[TableLike]:
    spec = ExplodeSpec(
        parent_keys=("call_id",),
        list_col="callee_fqns",
        value_col="fqn_value",
        idx_col=None,
        keep_empty=True,
    )
    exploded = kernel(cst_callsites_table, spec=spec, out_parent_col="call_id")
    base = _callsite_fqn_base_table(exploded, ctx=df_ctx)
    joined = _join_callsite_qname_meta(base, cst_callsites_table, ctx=df_ctx)
    return [joined] if joined.num_rows > 0 else []


def _concat_tables(tables: Sequence[TableLike]) -> TableLike:
    return pa.concat_tables(tables) if len(tables) > 1 else tables[0]


def _maybe_join_call_args(
    joined: TableLike,
    *,
    backend: BaseBackend | None,
    cst_call_args: TableLike | ViewReference | None,
    df_ctx: _DatafusionContext,
) -> TableLike:
    if cst_call_args is None:
        return joined
    try:
        call_args_table = _materialize_fragment(backend, cst_call_args)
    except ValueError:
        return joined
    arg_summary = _callsite_arg_summary(call_args_table, ctx=df_ctx)
    return _join_callsite_arg_summary(joined, arg_summary, ctx=df_ctx)


@cache(format="delta")
@tag(layer="normalize", artifact="ast_nodes_norm", kind="table")
def ast_nodes_norm(
    span_normalize_context: SpanNormalizeContext,
    ast_nodes: TableLike | ViewReference | None = None,
    ast_files: TableLike | RecordBatchReaderLike | None = None,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Add span structs to AST nodes for join-ready alignment.

    Returns
    -------
    TableLike
        AST nodes with span structs appended.
    """
    backend = span_normalize_context.ibis_backend
    register_nested_table(backend, name="ast_files_v1", table=ast_files)
    if ast_nodes is None and ast_files is not None:
        ast_nodes = ViewReference("ast_nodes")
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
    expr = add_ast_span_struct_ibis(
        span_normalize_context.file_line_index,
        ast_nodes,
        backend=backend,
    )
    table = _materialize_ibis_table(expr, runtime=span_normalize_context.runtime)
    schema = dataset_schema_from_context("py_ast_nodes_v1")
    return align_table_to_schema(table, schema=schema, keep_extra_columns=True)


@cache(format="delta")
@tag(layer="normalize", artifact="py_bc_instructions_norm", kind="table")
def py_bc_instructions_norm(
    span_normalize_context: SpanNormalizeContext,
    py_bc_instructions: TableLike | ViewReference | None = None,
    bytecode_files: TableLike | RecordBatchReaderLike | None = None,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Anchor bytecode instructions to source span structs.

    Returns
    -------
    TableLike
        Bytecode instruction table with span structs.
    """
    backend = span_normalize_context.ibis_backend
    register_nested_table(backend, name="bytecode_files_v1", table=bytecode_files)
    if py_bc_instructions is None and bytecode_files is not None:
        py_bc_instructions = ViewReference("py_bc_instructions")
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
    expr = anchor_instructions_span_struct_ibis(
        span_normalize_context.file_line_index,
        py_bc_instructions,
        backend=backend,
    )
    table = _materialize_ibis_table(expr, runtime=span_normalize_context.runtime)
    schema = dataset_schema_from_context("py_bc_instructions_v1")
    return align_table_to_schema(table, schema=schema, keep_extra_columns=True)


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
        cst_parse_errors = ViewReference("cst_parse_errors")
    if ts_errors is None and diagnostics_table_inputs.tree_sitter_files is not None:
        ts_errors = ViewReference("ts_errors")
    if ts_missing is None and diagnostics_table_inputs.tree_sitter_files is not None:
        ts_missing = ViewReference("ts_missing")
    if scip_diagnostics is None and backend is not None:
        scip_diagnostics = ViewReference("scip_diagnostics")
    if scip_documents is None and backend is not None:
        scip_documents = ViewReference("scip_documents")
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
@extract_fields(
    {
        "scip_occurrences_norm": TableLike,
        "scip_span_errors": TableLike,
    }
)
@tag(layer="normalize", artifact="scip_occurrences_norm_bundle", kind="bundle")
def scip_occurrences_norm_bundle(
    span_normalize_context: SpanNormalizeContext,
    scip_documents: TableLike | ViewReference | None = None,
    scip_occurrences: TableLike | ViewReference | None = None,
    evidence_plan: EvidencePlan | None = None,
) -> dict[str, TableLike]:
    """Convert SCIP occurrences into span structs.

    Returns
    -------
    dict[str, TableLike]
        Bundle with normalized occurrences and span errors.
    """
    backend = span_normalize_context.ibis_backend
    if scip_documents is None and backend is not None:
        scip_documents = ViewReference("scip_documents")
    if scip_occurrences is None and backend is not None:
        scip_occurrences = ViewReference("scip_occurrences")
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
            "scip_span_errors": empty_table(SPAN_ERROR_SCHEMA),
        }
    if scip_documents is None or scip_occurrences is None:
        return {
            "scip_occurrences_norm": _empty_scip_occurrences_norm(fallback_schema),
            "scip_span_errors": empty_table(SPAN_ERROR_SCHEMA),
        }
    _require_datafusion_backend(backend)
    stats = _scip_position_encoding_stats(scip_documents, backend=backend)
    if stats is not None:
        _record_scip_position_encoding_stats(span_normalize_context.ctx, stats)
        missing = _coerce_int(stats.get("missing_position_encoding"))
        invalid = _coerce_int(stats.get("invalid_position_encoding"))
        encodings = _stats_list(stats, "valid_position_encodings")
        invalid_values = _stats_list(stats, "invalid_position_encoding_values")
        if missing or invalid or len(encodings) > 1:
            LOGGER.warning(
                "SCIP position encoding guard: missing=%d invalid=%d encodings=%s "
                "invalid_values=%s",
                missing,
                invalid,
                encodings,
                invalid_values,
            )
    occ_expr, err_expr = add_scip_occurrence_span_struct_ibis(
        span_normalize_context.file_line_index,
        scip_documents,
        scip_occurrences,
        backend=backend,
    )
    occ = _materialize_ibis_table(occ_expr, runtime=span_normalize_context.runtime)
    errs = _materialize_ibis_table(err_expr, runtime=span_normalize_context.runtime)
    schema = dataset_schema_from_context("scip_occurrences_v1")
    occ = align_table_to_schema(occ, schema=schema, keep_extra_columns=True)
    errs = align_table_to_schema(errs, schema=SPAN_ERROR_SCHEMA)
    return {"scip_occurrences_norm": occ, "scip_span_errors": errs}


def _stats_list(stats: Mapping[str, object], key: str) -> list[str]:
    value = stats.get(key)
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


@cache(format="delta")
@tag(layer="normalize", artifact="cst_imports_norm", kind="table")
def cst_imports_norm(
    cst_imports: TableLike | ViewReference | None = None,
    normalize_execution_context: NormalizeExecutionContext | None = None,
    libcst_files: TableLike | RecordBatchReaderLike | None = None,
    *,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Normalize CST import spans into span structs.

    Returns
    -------
    TableLike
        Normalized CST imports table with span structs.

    Raises
    ------
    ValueError
        Raised when no Ibis backend is available for normalization.
    """
    _ = ctx
    backend = normalize_execution_context.ibis_backend if normalize_execution_context else None
    runtime = normalize_execution_context.runtime if normalize_execution_context else None
    register_nested_table(backend, name="libcst_files_v1", table=libcst_files)
    if cst_imports is None and libcst_files is not None:
        cst_imports = ViewReference("cst_imports")
    if cst_imports is None:
        return empty_table(dataset_schema(dataset_name_from_alias("cst_imports_norm")))
    if not _requires_any(evidence_plan, ("cst_imports_norm", "cst_imports")):
        schema = _schema_from_source(
            cst_imports,
            backend=backend,
            fallback="py_cst_imports_v1",
        )
        return empty_table(schema)
    if backend is None or runtime is None:
        msg = "CST import normalization requires a normalize execution context."
        raise ValueError(msg)
    _require_datafusion_backend(backend)
    expr = normalize_cst_imports_span_struct_ibis(cst_imports, backend=backend)
    return _materialize_ibis_table(expr, runtime=runtime)


@cache(format="delta")
@tag(layer="normalize", artifact="cst_defs_norm", kind="table")
def cst_defs_norm(
    cst_defs: TableLike | ViewReference | None = None,
    normalize_execution_context: NormalizeExecutionContext | None = None,
    libcst_files: TableLike | RecordBatchReaderLike | None = None,
    *,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Normalize CST def spans into span structs.

    Returns
    -------
    TableLike
        Normalized CST definitions table with span structs.

    Raises
    ------
    ValueError
        Raised when no Ibis backend is available for normalization.
    """
    _ = ctx
    backend = normalize_execution_context.ibis_backend if normalize_execution_context else None
    runtime = normalize_execution_context.runtime if normalize_execution_context else None
    register_nested_table(backend, name="libcst_files_v1", table=libcst_files)
    if cst_defs is None and libcst_files is not None:
        cst_defs = ViewReference("cst_defs")
    if cst_defs is None:
        return empty_table(dataset_schema(dataset_name_from_alias("cst_defs_norm")))
    if not _requires_any(evidence_plan, ("cst_defs_norm", "cst_defs")):
        schema = _schema_from_source(
            cst_defs,
            backend=backend,
            fallback="py_cst_defs_v1",
        )
        return empty_table(schema)
    if backend is None or runtime is None:
        msg = "CST definition normalization requires a normalize execution context."
        raise ValueError(msg)
    _require_datafusion_backend(backend)
    expr = normalize_cst_defs_span_struct_ibis(cst_defs, backend=backend)
    return _materialize_ibis_table(expr, runtime=runtime)


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
