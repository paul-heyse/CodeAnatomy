"""Ibis-first public entrypoints for normalize outputs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, cast

from ibis.backends import BaseBackend
from ibis.expr.types import Table

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.core.ordering import Ordering
from arrowdsl.core.runtime_profiles import runtime_profile_factory
from arrowdsl.schema.build import empty_table
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan
from ibis_engine.plan import IbisPlan
from ibis_engine.sources import SourceToIbisOptions, register_ibis_table
from normalize.ibis_plan_builders import (
    CFG_BLOCKS_NAME,
    CFG_EDGES_NAME,
    DEF_USE_NAME,
    DIAG_NAME,
    REACHES_NAME,
    TYPE_EXPRS_NAME,
    TYPE_NODES_NAME,
    IbisPlanCatalog,
    IbisPlanSource,
    cfg_blocks_plan_ibis,
    cfg_edges_plan_ibis,
    def_use_events_plan_ibis,
    diagnostics_plan_ibis,
    reaching_defs_plan_ibis,
    type_exprs_plan_ibis,
    type_nodes_plan_ibis,
)
from normalize.ibis_spans import (
    SpanSource,
    add_ast_span_struct_ibis,
    add_scip_occurrence_span_struct_ibis,
    anchor_instructions_span_struct_ibis,
    normalize_cst_callsites_span_struct_ibis,
    normalize_cst_defs_span_struct_ibis,
    normalize_cst_imports_span_struct_ibis,
)
from normalize.registry_runtime import (
    dataset_contract,
    dataset_schema,
    dataset_schema_policy,
    dataset_spec,
)
from normalize.runner import (
    NormalizeFinalizeSpec,
    NormalizeRunOptions,
    ensure_execution_context,
    run_normalize,
)
from normalize.runtime import NormalizeRuntime
from normalize.schemas import SPAN_ERROR_SCHEMA

NormalizeSource = IbisPlanSource | None


@dataclass(frozen=True)
class DiagnosticsSources:
    """Source tables for diagnostics aggregation."""

    cst_parse_errors: NormalizeSource = None
    ts_errors: NormalizeSource = None
    ts_missing: NormalizeSource = None
    scip_diagnostics: NormalizeSource = None
    scip_documents: NormalizeSource = None


@dataclass(frozen=True)
class DiagnosticsInputs:
    """Inputs for diagnostics normalization."""

    file_line_index: NormalizeSource = None
    sources: DiagnosticsSources = field(default_factory=DiagnosticsSources)


def _require_runtime(runtime: NormalizeRuntime | None) -> NormalizeRuntime:
    if runtime is None:
        msg = "Normalize runtime is required for normalize Ibis APIs."
        raise ValueError(msg)
    return runtime


def _materialize_table_expr(expr: Table, *, runtime: NormalizeRuntime) -> TableLike:
    runtime_profile = runtime_profile_factory("default").with_datafusion(runtime.runtime_profile)
    exec_ctx = ExecutionContext(runtime=runtime_profile)
    execution = IbisExecutionContext(ctx=exec_ctx, ibis_backend=runtime.ibis_backend)
    plan = IbisPlan(expr=expr, ordering=Ordering.unordered())
    return materialize_ibis_plan(plan, execution=execution)


def _empty_plan(output: str, *, backend: BaseBackend) -> IbisPlan:
    schema = dataset_schema(output)
    return register_ibis_table(
        empty_table(schema),
        options=SourceToIbisOptions(
            backend=backend,
            name=None,
            ordering=Ordering.unordered(),
        ),
    )


def _catalog_from_tables(
    backend: BaseBackend,
    *,
    tables: dict[str, IbisPlanSource | None],
) -> IbisPlanCatalog:
    filtered: dict[str, IbisPlanSource] = {
        name: table for name, table in tables.items() if table is not None
    }
    return IbisPlanCatalog(backend=backend, tables=filtered)


def _span_source(name: str, source: NormalizeSource) -> SpanSource:
    if source is None:
        msg = f"{name} is required for span enrichment."
        raise ValueError(msg)
    if isinstance(source, IbisPlan):
        return source.expr
    return cast("SpanSource", source)


def _finalize_plan(
    plan: IbisPlan | None,
    *,
    output: str,
    ctx: ExecutionContext,
    runtime: NormalizeRuntime,
) -> TableLike:
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
        options=NormalizeRunOptions(
            finalize_spec=finalize_spec,
            runtime=runtime,
        ),
    ).good


def build_cfg_blocks(
    py_bc_blocks: NormalizeSource,
    py_bc_code_units: NormalizeSource,
    *,
    ctx: ExecutionContext | None = None,
    runtime: NormalizeRuntime | None = None,
    profile: str = "default",
) -> TableLike:
    """Normalize CFG block rows and enrich with file/path metadata.

    Returns
    -------
    TableLike
        Normalized CFG block table.
    """
    normalize_runtime = _require_runtime(runtime)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        normalize_runtime.ibis_backend,
        tables={
            "py_bc_blocks": py_bc_blocks,
            "py_bc_code_units": py_bc_code_units,
        },
    )
    plan = cfg_blocks_plan_ibis(catalog, exec_ctx, normalize_runtime.ibis_backend)
    return _finalize_plan(
        plan,
        output=CFG_BLOCKS_NAME,
        ctx=exec_ctx,
        runtime=normalize_runtime,
    )


def build_cfg_edges(
    py_bc_code_units: NormalizeSource,
    py_bc_cfg_edges: NormalizeSource,
    *,
    ctx: ExecutionContext | None = None,
    runtime: NormalizeRuntime | None = None,
    profile: str = "default",
) -> TableLike:
    """Normalize CFG edge rows and enrich with file/path metadata.

    Returns
    -------
    TableLike
        Normalized CFG edge table.
    """
    normalize_runtime = _require_runtime(runtime)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        normalize_runtime.ibis_backend,
        tables={
            "py_bc_cfg_edges": py_bc_cfg_edges,
            "py_bc_code_units": py_bc_code_units,
        },
    )
    plan = cfg_edges_plan_ibis(catalog, exec_ctx, normalize_runtime.ibis_backend)
    return _finalize_plan(
        plan,
        output=CFG_EDGES_NAME,
        ctx=exec_ctx,
        runtime=normalize_runtime,
    )


def build_def_use_events(
    py_bc_instructions: NormalizeSource,
    *,
    ctx: ExecutionContext | None = None,
    runtime: NormalizeRuntime | None = None,
    profile: str = "default",
) -> TableLike:
    """Build def/use events from bytecode instruction rows.

    Returns
    -------
    TableLike
        Def/use events table.
    """
    normalize_runtime = _require_runtime(runtime)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        normalize_runtime.ibis_backend,
        tables={"py_bc_instructions": py_bc_instructions},
    )
    plan = def_use_events_plan_ibis(catalog, exec_ctx, normalize_runtime.ibis_backend)
    return _finalize_plan(plan, output=DEF_USE_NAME, ctx=exec_ctx, runtime=normalize_runtime)


def run_reaching_defs(
    def_use_events: NormalizeSource,
    *,
    ctx: ExecutionContext | None = None,
    runtime: NormalizeRuntime | None = None,
    profile: str = "default",
) -> TableLike:
    """Compute a best-effort reaching-defs edge table.

    Returns
    -------
    TableLike
        Reaching-def edges table.
    """
    normalize_runtime = _require_runtime(runtime)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        normalize_runtime.ibis_backend,
        tables={"py_bc_def_use_events_v1": def_use_events},
    )
    plan = reaching_defs_plan_ibis(catalog, exec_ctx, normalize_runtime.ibis_backend)
    return _finalize_plan(plan, output=REACHES_NAME, ctx=exec_ctx, runtime=normalize_runtime)


def normalize_type_exprs(
    cst_type_exprs: NormalizeSource,
    *,
    ctx: ExecutionContext | None = None,
    runtime: NormalizeRuntime | None = None,
    profile: str = "default",
) -> TableLike:
    """Normalize type expressions into join-ready tables.

    Returns
    -------
    TableLike
        Normalized type expressions table.
    """
    normalize_runtime = _require_runtime(runtime)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        normalize_runtime.ibis_backend,
        tables={"cst_type_exprs": cst_type_exprs},
    )
    plan = type_exprs_plan_ibis(catalog, exec_ctx, normalize_runtime.ibis_backend)
    return _finalize_plan(plan, output=TYPE_EXPRS_NAME, ctx=exec_ctx, runtime=normalize_runtime)


def normalize_types(
    cst_type_exprs: NormalizeSource,
    scip_symbol_information: NormalizeSource = None,
    *,
    ctx: ExecutionContext | None = None,
    runtime: NormalizeRuntime | None = None,
    profile: str = "default",
) -> TableLike:
    """Normalize type expressions into type node rows.

    Returns
    -------
    TableLike
        Normalized type nodes table.
    """
    normalize_runtime = _require_runtime(runtime)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        normalize_runtime.ibis_backend,
        tables={
            "cst_type_exprs": cst_type_exprs,
            "scip_symbol_information": scip_symbol_information,
        },
    )
    exprs_plan = type_exprs_plan_ibis(catalog, exec_ctx, normalize_runtime.ibis_backend)
    if exprs_plan is None:
        exprs_plan = _empty_plan(TYPE_EXPRS_NAME, backend=normalize_runtime.ibis_backend)
    catalog.add(TYPE_EXPRS_NAME, exprs_plan)
    plan = type_nodes_plan_ibis(catalog, exec_ctx, normalize_runtime.ibis_backend)
    return _finalize_plan(plan, output=TYPE_NODES_NAME, ctx=exec_ctx, runtime=normalize_runtime)


def collect_diags(
    inputs: DiagnosticsInputs,
    *,
    ctx: ExecutionContext | None = None,
    runtime: NormalizeRuntime | None = None,
    profile: str = "default",
) -> TableLike:
    """Aggregate diagnostics into a single normalized table.

    Returns
    -------
    TableLike
        Normalized diagnostics table.
    """
    normalize_runtime = _require_runtime(runtime)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        normalize_runtime.ibis_backend,
        tables={
            "file_line_index": inputs.file_line_index,
            "cst_parse_errors": inputs.sources.cst_parse_errors,
            "ts_errors": inputs.sources.ts_errors,
            "ts_missing": inputs.sources.ts_missing,
            "scip_diagnostics": inputs.sources.scip_diagnostics,
            "scip_documents": inputs.sources.scip_documents,
        },
    )
    plan = diagnostics_plan_ibis(catalog, exec_ctx, normalize_runtime.ibis_backend)
    return _finalize_plan(plan, output=DIAG_NAME, ctx=exec_ctx, runtime=normalize_runtime)


def add_scip_occurrence_byte_spans(
    file_line_index: NormalizeSource,
    scip_documents: NormalizeSource,
    scip_occurrences: NormalizeSource,
    *,
    runtime: NormalizeRuntime | None = None,
) -> tuple[TableLike, TableLike]:
    """Add span structs to SCIP occurrences using line index joins.

    Parameters
    ----------
    file_line_index
        Line index table with per-line byte offsets.
    scip_documents
        SCIP documents input table or plan.
    scip_occurrences
        SCIP occurrences input table or plan.
    runtime
        Normalize runtime used for execution.

    Returns
    -------
    tuple[TableLike, TableLike]
        SCIP occurrences with span structs and span error rows.
    """
    normalize_runtime = _require_runtime(runtime)
    occ_expr, err_expr = add_scip_occurrence_span_struct_ibis(
        _span_source("file_line_index", file_line_index),
        _span_source("scip_documents", scip_documents),
        _span_source("scip_occurrences", scip_occurrences),
        backend=normalize_runtime.ibis_backend,
    )
    occ_table = _materialize_table_expr(occ_expr, runtime=normalize_runtime)
    err_table = _materialize_table_expr(err_expr, runtime=normalize_runtime)
    if err_table.num_rows == 0:
        return occ_table, empty_table(SPAN_ERROR_SCHEMA)
    return occ_table, err_table


def normalize_cst_callsites_spans(
    py_cst_callsites: NormalizeSource,
    *,
    runtime: NormalizeRuntime | None = None,
    primary: Literal["callee", "call"] = "callee",
) -> TableLike:
    """Ensure callsites expose canonical span structs.

    Parameters
    ----------
    py_cst_callsites
        CST callsite rows or a plan to materialize them.
    runtime
        Normalize runtime used for execution.
    primary
        Which span to treat as canonical.

    Returns
    -------
    TableLike
        Callsites table with canonical span structs.
    """
    normalize_runtime = _require_runtime(runtime)
    expr = normalize_cst_callsites_span_struct_ibis(
        _span_source("py_cst_callsites", py_cst_callsites),
        backend=normalize_runtime.ibis_backend,
        primary=primary,
    )
    return _materialize_table_expr(expr, runtime=normalize_runtime)


def normalize_cst_imports_spans(
    py_cst_imports: NormalizeSource,
    *,
    runtime: NormalizeRuntime | None = None,
    primary: Literal["alias", "stmt"] = "alias",
) -> TableLike:
    """Ensure imports expose canonical span structs.

    Parameters
    ----------
    py_cst_imports
        CST imports rows or a plan to materialize them.
    runtime
        Normalize runtime used for execution.
    primary
        Which span to treat as canonical.

    Returns
    -------
    TableLike
        Imports table with canonical span structs.
    """
    normalize_runtime = _require_runtime(runtime)
    expr = normalize_cst_imports_span_struct_ibis(
        _span_source("py_cst_imports", py_cst_imports),
        backend=normalize_runtime.ibis_backend,
        primary=primary,
    )
    return _materialize_table_expr(expr, runtime=normalize_runtime)


def normalize_cst_defs_spans(
    py_cst_defs: NormalizeSource,
    *,
    runtime: NormalizeRuntime | None = None,
    primary: Literal["name", "def"] = "name",
) -> TableLike:
    """Ensure defs expose canonical span structs.

    Parameters
    ----------
    py_cst_defs
        CST definition rows or a plan to materialize them.
    runtime
        Normalize runtime used for execution.
    primary
        Which span to treat as canonical.

    Returns
    -------
    TableLike
        Definitions table with canonical span structs.
    """
    normalize_runtime = _require_runtime(runtime)
    expr = normalize_cst_defs_span_struct_ibis(
        _span_source("py_cst_defs", py_cst_defs),
        backend=normalize_runtime.ibis_backend,
        primary=primary,
    )
    return _materialize_table_expr(expr, runtime=normalize_runtime)


def anchor_instructions(
    file_line_index: NormalizeSource,
    py_bc_instructions: NormalizeSource,
    *,
    runtime: NormalizeRuntime | None = None,
) -> TableLike:
    """Anchor bytecode instruction spans using line index joins.

    Returns
    -------
    TableLike
        Instruction table with span structs applied.
    """
    normalize_runtime = _require_runtime(runtime)
    expr = anchor_instructions_span_struct_ibis(
        _span_source("file_line_index", file_line_index),
        _span_source("py_bc_instructions", py_bc_instructions),
        backend=normalize_runtime.ibis_backend,
    )
    return _materialize_table_expr(expr, runtime=normalize_runtime)


def add_ast_byte_spans(
    file_line_index: NormalizeSource,
    py_ast_nodes: NormalizeSource,
    *,
    runtime: NormalizeRuntime | None = None,
) -> TableLike:
    """Append AST span structs using line index joins.

    Returns
    -------
    TableLike
        AST table with span structs applied.
    """
    normalize_runtime = _require_runtime(runtime)
    expr = add_ast_span_struct_ibis(
        _span_source("file_line_index", file_line_index),
        _span_source("py_ast_nodes", py_ast_nodes),
        backend=normalize_runtime.ibis_backend,
    )
    return _materialize_table_expr(expr, runtime=normalize_runtime)


__all__ = [
    "DiagnosticsInputs",
    "DiagnosticsSources",
    "add_ast_byte_spans",
    "add_scip_occurrence_byte_spans",
    "anchor_instructions",
    "build_cfg_blocks",
    "build_cfg_edges",
    "build_def_use_events",
    "collect_diags",
    "normalize_cst_callsites_spans",
    "normalize_cst_defs_spans",
    "normalize_cst_imports_spans",
    "normalize_type_exprs",
    "normalize_types",
    "run_reaching_defs",
]
