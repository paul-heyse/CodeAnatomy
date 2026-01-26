"""Ibis-first public entrypoints for normalize outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal, cast

from ibis.expr.types import Table

from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import TableLike
from arrowdsl.core.ordering import Ordering
from datafusion_engine.execution_facade import ExecutionResult
from datafusion_engine.view_registry import ensure_view_graph
from ibis_engine.catalog import IbisPlanSource
from ibis_engine.execution import execute_ibis_plan
from ibis_engine.execution_factory import ibis_execution_from_ctx
from ibis_engine.plan import IbisPlan
from normalize.ibis_spans import (
    SpanSource,
    add_ast_span_struct_ibis,
    add_scip_occurrence_span_struct_ibis,
    anchor_instructions_span_struct_ibis,
    normalize_cst_callsites_span_struct_ibis,
    normalize_cst_defs_span_struct_ibis,
    normalize_cst_imports_span_struct_ibis,
)
from normalize.runtime import NormalizeRuntime, build_normalize_runtime

NormalizeSource = IbisPlanSource | None

CFG_BLOCKS_NAME = "py_bc_blocks_norm_v1"
CFG_EDGES_NAME = "py_bc_cfg_edges_norm_v1"
DEF_USE_NAME = "py_bc_def_use_events_v1"
REACHES_NAME = "py_bc_reaches_v1"
TYPE_EXPRS_NAME = "type_exprs_norm_v1"
TYPE_NODES_NAME = "type_nodes_v1"
DIAG_NAME = "diagnostics_norm_v1"


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


def _resolve_runtime(
    runtime: NormalizeRuntime | None,
    ctx: ExecutionContext | None,
    *,
    profile: str,
) -> NormalizeRuntime:
    if runtime is not None:
        return runtime
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    return build_normalize_runtime(exec_ctx)


def _assert_no_inputs(output: str, inputs: Mapping[str, NormalizeSource]) -> None:
    provided = [name for name, value in inputs.items() if value is not None]
    if not provided:
        return
    msg = (
        f"Normalize output {output!r} is view-driven; "
        f"remove explicit inputs and register base tables via the DataFusion context. "
        f"Provided: {sorted(provided)}."
    )
    raise ValueError(msg)


def ensure_execution_context(
    ctx: ExecutionContext | None,
    *,
    profile: str,
) -> ExecutionContext:
    """Return an execution context, building one when missing.

    Returns
    -------
    ExecutionContext
        Execution context derived from inputs.
    """
    if ctx is not None:
        return ctx
    return execution_context_factory(profile)


def _view_output_table(
    output: str,
    *,
    ctx: ExecutionContext | None,
    runtime: NormalizeRuntime | None,
    profile: str,
) -> TableLike:
    normalize_runtime = _resolve_runtime(runtime, ctx, profile=profile)
    ensure_view_graph(
        normalize_runtime.ctx,
        runtime_profile=normalize_runtime.runtime_profile,
    )
    if not normalize_runtime.ctx.table_exist(output):
        msg = f"Normalize view {output!r} is not registered."
        raise ValueError(msg)
    return normalize_runtime.ctx.table(output).to_arrow_table()


def _materialize_table_expr(expr: Table, *, runtime: NormalizeRuntime) -> ExecutionResult:
    execution = ibis_execution_from_ctx(runtime.execution_ctx, backend=runtime.ibis_backend)
    plan = IbisPlan(expr=expr, ordering=Ordering.unordered())
    return execute_ibis_plan(
        plan,
        execution=execution,
        streaming=False,
    )


def _span_source(name: str, source: NormalizeSource) -> SpanSource:
    if source is None:
        msg = f"{name} is required for span enrichment."
        raise ValueError(msg)
    if isinstance(source, IbisPlan):
        return source.expr
    return cast("SpanSource", source)


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
    _assert_no_inputs(
        CFG_BLOCKS_NAME,
        {"py_bc_blocks": py_bc_blocks, "py_bc_code_units": py_bc_code_units},
    )
    return _view_output_table(CFG_BLOCKS_NAME, ctx=ctx, runtime=runtime, profile=profile)


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
    _assert_no_inputs(
        CFG_EDGES_NAME,
        {"py_bc_cfg_edges": py_bc_cfg_edges, "py_bc_code_units": py_bc_code_units},
    )
    return _view_output_table(CFG_EDGES_NAME, ctx=ctx, runtime=runtime, profile=profile)


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
    _assert_no_inputs(DEF_USE_NAME, {"py_bc_instructions": py_bc_instructions})
    return _view_output_table(DEF_USE_NAME, ctx=ctx, runtime=runtime, profile=profile)


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
    _assert_no_inputs(REACHES_NAME, {"py_bc_def_use_events_v1": def_use_events})
    return _view_output_table(REACHES_NAME, ctx=ctx, runtime=runtime, profile=profile)


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
    _assert_no_inputs(TYPE_EXPRS_NAME, {"cst_type_exprs": cst_type_exprs})
    return _view_output_table(TYPE_EXPRS_NAME, ctx=ctx, runtime=runtime, profile=profile)


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
    _assert_no_inputs(
        TYPE_NODES_NAME,
        {
            "cst_type_exprs": cst_type_exprs,
            "scip_symbol_information": scip_symbol_information,
        },
    )
    return _view_output_table(TYPE_NODES_NAME, ctx=ctx, runtime=runtime, profile=profile)


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
    _assert_no_inputs(
        DIAG_NAME,
        {
            "file_line_index": inputs.file_line_index,
            "cst_parse_errors": inputs.sources.cst_parse_errors,
            "ts_errors": inputs.sources.ts_errors,
            "ts_missing": inputs.sources.ts_missing,
            "scip_diagnostics": inputs.sources.scip_diagnostics,
            "scip_documents": inputs.sources.scip_documents,
        },
    )
    return _view_output_table(DIAG_NAME, ctx=ctx, runtime=runtime, profile=profile)


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
    occ_table = _materialize_table_expr(occ_expr, runtime=normalize_runtime).require_table()
    err_table = _materialize_table_expr(err_expr, runtime=normalize_runtime).require_table()
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
    return _materialize_table_expr(expr, runtime=normalize_runtime).require_table()


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
    return _materialize_table_expr(expr, runtime=normalize_runtime).require_table()


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
    return _materialize_table_expr(expr, runtime=normalize_runtime).require_table()


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
    return _materialize_table_expr(expr, runtime=normalize_runtime).require_table()


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
    return _materialize_table_expr(expr, runtime=normalize_runtime).require_table()


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
