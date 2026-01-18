"""Ibis-first public entrypoints for normalize outputs."""

from __future__ import annotations

from dataclasses import dataclass, field

import ibis
from ibis.backends import BaseBackend

from arrowdsl.core.context import ExecutionContext, Ordering
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import empty_table
from ibis_engine.plan import IbisPlan
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
    add_ast_byte_spans_ibis,
    add_scip_occurrence_byte_spans_ibis,
    anchor_instructions_ibis,
)
from normalize.registry_specs import (
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


@dataclass(frozen=True)
class DiagnosticsSources:
    """Source tables for diagnostics aggregation."""

    cst_parse_errors: TableLike | None = None
    ts_errors: TableLike | None = None
    ts_missing: TableLike | None = None
    scip_diagnostics: TableLike | None = None
    scip_documents: TableLike | None = None


@dataclass(frozen=True)
class DiagnosticsInputs:
    """Inputs for diagnostics normalization."""

    file_line_index: TableLike | None = None
    sources: DiagnosticsSources = field(default_factory=DiagnosticsSources)


def _require_backend(backend: BaseBackend | None) -> BaseBackend:
    if backend is None:
        msg = "Ibis backend is required for normalize Ibis APIs."
        raise ValueError(msg)
    return backend


def _empty_plan(output: str) -> IbisPlan:
    schema = dataset_schema(output)
    empty = ibis.memtable(empty_table(schema))
    return IbisPlan(expr=empty, ordering=Ordering.unordered())


def _catalog_from_tables(
    backend: BaseBackend,
    *,
    tables: dict[str, TableLike | None],
) -> IbisPlanCatalog:
    filtered: dict[str, IbisPlanSource] = {
        name: table for name, table in tables.items() if table is not None
    }
    return IbisPlanCatalog(backend=backend, tables=filtered)


def _finalize_plan(
    plan: IbisPlan | None,
    *,
    output: str,
    ctx: ExecutionContext,
    backend: BaseBackend,
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
            ibis_backend=backend,
        ),
    ).good


def build_cfg_blocks(
    py_bc_blocks: TableLike | None,
    py_bc_code_units: TableLike | None,
    *,
    ctx: ExecutionContext | None = None,
    backend: BaseBackend | None = None,
    profile: str = "default",
) -> TableLike:
    """Normalize CFG block rows and enrich with file/path metadata.

    Returns
    -------
    TableLike
        Normalized CFG block table.
    """
    ibis_backend = _require_backend(backend)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        ibis_backend,
        tables={
            "py_bc_blocks": py_bc_blocks,
            "py_bc_code_units": py_bc_code_units,
        },
    )
    plan = cfg_blocks_plan_ibis(catalog, exec_ctx, ibis_backend)
    return _finalize_plan(plan, output=CFG_BLOCKS_NAME, ctx=exec_ctx, backend=ibis_backend)


def build_cfg_edges(
    py_bc_code_units: TableLike | None,
    py_bc_cfg_edges: TableLike | None,
    *,
    ctx: ExecutionContext | None = None,
    backend: BaseBackend | None = None,
    profile: str = "default",
) -> TableLike:
    """Normalize CFG edge rows and enrich with file/path metadata.

    Returns
    -------
    TableLike
        Normalized CFG edge table.
    """
    ibis_backend = _require_backend(backend)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        ibis_backend,
        tables={
            "py_bc_cfg_edges": py_bc_cfg_edges,
            "py_bc_code_units": py_bc_code_units,
        },
    )
    plan = cfg_edges_plan_ibis(catalog, exec_ctx, ibis_backend)
    return _finalize_plan(plan, output=CFG_EDGES_NAME, ctx=exec_ctx, backend=ibis_backend)


def build_def_use_events(
    py_bc_instructions: TableLike | None,
    *,
    ctx: ExecutionContext | None = None,
    backend: BaseBackend | None = None,
    profile: str = "default",
) -> TableLike:
    """Build def/use events from bytecode instruction rows.

    Returns
    -------
    TableLike
        Def/use events table.
    """
    ibis_backend = _require_backend(backend)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        ibis_backend,
        tables={"py_bc_instructions": py_bc_instructions},
    )
    plan = def_use_events_plan_ibis(catalog, exec_ctx, ibis_backend)
    return _finalize_plan(plan, output=DEF_USE_NAME, ctx=exec_ctx, backend=ibis_backend)


def run_reaching_defs(
    def_use_events: TableLike | None,
    *,
    ctx: ExecutionContext | None = None,
    backend: BaseBackend | None = None,
    profile: str = "default",
) -> TableLike:
    """Compute a best-effort reaching-defs edge table.

    Returns
    -------
    TableLike
        Reaching-def edges table.
    """
    ibis_backend = _require_backend(backend)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        ibis_backend,
        tables={"py_bc_def_use_events_v1": def_use_events},
    )
    plan = reaching_defs_plan_ibis(catalog, exec_ctx, ibis_backend)
    return _finalize_plan(plan, output=REACHES_NAME, ctx=exec_ctx, backend=ibis_backend)


def normalize_type_exprs(
    cst_type_exprs: TableLike | None,
    *,
    ctx: ExecutionContext | None = None,
    backend: BaseBackend | None = None,
    profile: str = "default",
) -> TableLike:
    """Normalize type expressions into join-ready tables.

    Returns
    -------
    TableLike
        Normalized type expressions table.
    """
    ibis_backend = _require_backend(backend)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        ibis_backend,
        tables={"cst_type_exprs": cst_type_exprs},
    )
    plan = type_exprs_plan_ibis(catalog, exec_ctx, ibis_backend)
    return _finalize_plan(plan, output=TYPE_EXPRS_NAME, ctx=exec_ctx, backend=ibis_backend)


def normalize_types(
    cst_type_exprs: TableLike | None,
    scip_symbol_information: TableLike | None = None,
    *,
    ctx: ExecutionContext | None = None,
    backend: BaseBackend | None = None,
    profile: str = "default",
) -> TableLike:
    """Normalize type expressions into type node rows.

    Returns
    -------
    TableLike
        Normalized type nodes table.
    """
    ibis_backend = _require_backend(backend)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        ibis_backend,
        tables={
            "cst_type_exprs": cst_type_exprs,
            "scip_symbol_information": scip_symbol_information,
        },
    )
    exprs_plan = type_exprs_plan_ibis(catalog, exec_ctx, ibis_backend)
    if exprs_plan is None:
        exprs_plan = _empty_plan(TYPE_EXPRS_NAME)
    catalog.add(TYPE_EXPRS_NAME, exprs_plan)
    plan = type_nodes_plan_ibis(catalog, exec_ctx, ibis_backend)
    return _finalize_plan(plan, output=TYPE_NODES_NAME, ctx=exec_ctx, backend=ibis_backend)


def collect_diags(
    inputs: DiagnosticsInputs,
    *,
    ctx: ExecutionContext | None = None,
    backend: BaseBackend | None = None,
    profile: str = "default",
) -> TableLike:
    """Aggregate diagnostics into a single normalized table.

    Returns
    -------
    TableLike
        Normalized diagnostics table.
    """
    ibis_backend = _require_backend(backend)
    exec_ctx = ensure_execution_context(ctx, profile=profile)
    catalog = _catalog_from_tables(
        ibis_backend,
        tables={
            "file_line_index": inputs.file_line_index,
            "cst_parse_errors": inputs.sources.cst_parse_errors,
            "ts_errors": inputs.sources.ts_errors,
            "ts_missing": inputs.sources.ts_missing,
            "scip_diagnostics": inputs.sources.scip_diagnostics,
            "scip_documents": inputs.sources.scip_documents,
        },
    )
    plan = diagnostics_plan_ibis(catalog, exec_ctx, ibis_backend)
    return _finalize_plan(plan, output=DIAG_NAME, ctx=exec_ctx, backend=ibis_backend)


def add_scip_occurrence_byte_spans(
    file_line_index: TableLike,
    scip_documents: TableLike,
    scip_occurrences: TableLike,
    *,
    backend: BaseBackend | None = None,
) -> tuple[TableLike, TableLike]:
    """Add byte spans to SCIP occurrences using line index joins.

    Returns
    -------
    tuple[TableLike, TableLike]
        SCIP documents and occurrences with byte spans.
    """
    ibis_backend = _require_backend(backend)
    return add_scip_occurrence_byte_spans_ibis(
        file_line_index,
        scip_documents,
        scip_occurrences,
        backend=ibis_backend,
    )


def anchor_instructions(
    file_line_index: TableLike,
    py_bc_instructions: TableLike,
    *,
    backend: BaseBackend | None = None,
) -> TableLike:
    """Anchor bytecode instruction spans using line index joins.

    Returns
    -------
    TableLike
        Instruction table with byte spans applied.
    """
    ibis_backend = _require_backend(backend)
    return anchor_instructions_ibis(
        file_line_index,
        py_bc_instructions,
        backend=ibis_backend,
    )


def add_ast_byte_spans(
    file_line_index: TableLike,
    py_ast_nodes: TableLike,
    *,
    backend: BaseBackend | None = None,
) -> TableLike:
    """Append AST byte-span columns using line index joins.

    Returns
    -------
    TableLike
        AST table with byte spans applied.
    """
    ibis_backend = _require_backend(backend)
    return add_ast_byte_spans_ibis(
        file_line_index,
        py_ast_nodes,
        backend=ibis_backend,
    )


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
    "normalize_type_exprs",
    "normalize_types",
    "run_reaching_defs",
]
