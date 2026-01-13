"""Hamilton normalization stage functions."""

from __future__ import annotations

import pyarrow as pa
from hamilton.function_modifiers import cache, extract_fields, tag

from arrowdsl.compute.kernels import (
    distinct_sorted,
    explode_list_column,
    flatten_list_struct_field,
)
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import prefixed_hash_id
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, TableLike, pc
from arrowdsl.plan.joins import JoinConfig, left_join
from arrowdsl.schema.arrays import set_or_append_column
from arrowdsl.schema.factories import empty_table, table_from_arrays
from normalize.bytecode_anchor import anchor_instructions
from normalize.bytecode_cfg import build_cfg_blocks, build_cfg_edges
from normalize.bytecode_dfg import build_def_use_events, run_reaching_defs
from normalize.diagnostics import DiagnosticsSources, collect_diags
from normalize.schema_infer import align_table_to_schema, infer_schema_or_registry
from normalize.spans import (
    RepoTextIndex,
    add_ast_byte_spans,
    add_scip_occurrence_byte_spans,
    build_repo_text_index,
    normalize_cst_defs_spans,
    normalize_cst_imports_spans,
)
from normalize.types import normalize_type_exprs, normalize_types
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
    _ = ctx
    if (
        cst_callsites is None
        or cst_callsites.num_rows == 0
        or "callee_qnames" not in cst_callsites.column_names
    ):
        schema = infer_schema_or_registry(CALLSITE_QNAME_CANDIDATES_SPEC.table_spec.name, [])
        return empty_table(schema)

    exploded = explode_list_column(
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
    repo_text_index: RepoTextIndex,
    ast_nodes: TableLike,
    ctx: ExecutionContext,
) -> TableLike:
    """Add byte-span columns to AST nodes for join-ready alignment.

    Returns
    -------
    TableLike
        AST nodes with bstart/bend/span_ok columns appended.
    """
    _ = ctx
    return add_ast_byte_spans(repo_text_index, ast_nodes)


@cache(format="parquet")
@tag(layer="normalize", artifact="py_bc_instructions_norm", kind="table")
def py_bc_instructions_norm(
    repo_text_index: RepoTextIndex,
    py_bc_instructions: TableLike,
    ctx: ExecutionContext,
) -> TableLike:
    """Anchor bytecode instructions to source byte spans.

    Returns
    -------
    TableLike
        Bytecode instruction table with bstart/bend/span_ok columns.
    """
    _ = ctx
    return anchor_instructions(repo_text_index, py_bc_instructions)


@cache(format="parquet")
@tag(layer="normalize", artifact="py_bc_blocks_norm", kind="table")
def py_bc_blocks_norm(
    py_bc_blocks: TableLike,
    py_bc_code_units: TableLike,
    ctx: ExecutionContext,
) -> TableLike:
    """Normalize bytecode CFG blocks with file/path metadata.

    Returns
    -------
    TableLike
        CFG block table aligned to the normalization schema.
    """
    return build_cfg_blocks(py_bc_blocks, py_bc_code_units, ctx=ctx)


@cache(format="parquet")
@tag(layer="normalize", artifact="py_bc_cfg_edges_norm", kind="table")
def py_bc_cfg_edges_norm(
    py_bc_code_units: TableLike,
    py_bc_cfg_edges: TableLike,
    ctx: ExecutionContext,
) -> TableLike:
    """Normalize bytecode CFG edges with file/path metadata.

    Returns
    -------
    TableLike
        CFG edge table aligned to the normalization schema.
    """
    return build_cfg_edges(py_bc_code_units, py_bc_cfg_edges, ctx=ctx)


@cache(format="parquet")
@tag(layer="normalize", artifact="py_bc_def_use_events", kind="table")
def py_bc_def_use_events(py_bc_instructions: TableLike, ctx: ExecutionContext) -> TableLike:
    """Derive def/use events from bytecode instructions.

    Returns
    -------
    TableLike
        Def/use events table.
    """
    _ = ctx
    return build_def_use_events(py_bc_instructions)


@cache(format="parquet")
@tag(layer="normalize", artifact="py_bc_reaching_defs", kind="table")
def py_bc_reaching_defs(py_bc_def_use_events: TableLike, ctx: ExecutionContext) -> TableLike:
    """Compute reaching-def edges from def/use events.

    Returns
    -------
    TableLike
        Reaching-def edges table.
    """
    _ = ctx
    return run_reaching_defs(py_bc_def_use_events)


@cache(format="parquet")
@tag(layer="normalize", artifact="type_exprs_norm", kind="table")
def type_exprs_norm(cst_type_exprs: TableLike, ctx: ExecutionContext) -> TableLike:
    """Normalize CST type expressions into join-ready tables.

    Returns
    -------
    TableLike
        Normalized type expressions table.
    """
    _ = ctx
    return normalize_type_exprs(cst_type_exprs)


@cache(format="parquet")
@tag(layer="normalize", artifact="types_norm", kind="table")
def types_norm(
    type_exprs_norm: TableLike,
    scip_symbol_information: TableLike,
    ctx: ExecutionContext,
) -> TableLike:
    """Normalize type expressions into type nodes.

    Returns
    -------
    TableLike
        Normalized type node table.
    """
    _ = ctx
    return normalize_types(type_exprs_norm, scip_symbol_information)


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
    repo_text_index: RepoTextIndex,
    diagnostics_sources: DiagnosticsSources,
    ctx: ExecutionContext,
) -> TableLike:
    """Aggregate diagnostics into a normalized table.

    Returns
    -------
    TableLike
        Normalized diagnostics table.
    """
    _ = ctx
    return collect_diags(repo_text_index, sources=diagnostics_sources)


@cache()
@tag(layer="normalize", artifact="repo_text_index", kind="object")
def repo_text_index(repo_root: str, repo_files: TableLike, ctx: ExecutionContext) -> RepoTextIndex:
    """Build a repo text index for line/column to byte offsets.

    Returns
    -------
    RepoTextIndex
        Repository text index for span conversion.
    """
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
    repo_text_index: RepoTextIndex,
    ctx: ExecutionContext,
) -> dict[str, TableLike]:
    """Convert SCIP occurrences into byte offsets.

    Returns
    -------
    dict[str, TableLike]
        Bundle with normalized occurrences and span errors.
    """
    occ, errs = add_scip_occurrence_byte_spans(
        scip_documents=scip_documents,
        scip_occurrences=scip_occurrences,
        repo_text_index=repo_text_index,
        ctx=ctx,
    )
    return {"scip_occurrences_norm": occ, "scip_span_errors": errs}


@cache(format="parquet")
@tag(layer="normalize", artifact="cst_imports_norm", kind="table")
def cst_imports_norm(cst_imports: TableLike, ctx: ExecutionContext) -> TableLike:
    """Normalize CST import spans into bstart/bend.

    Returns
    -------
    TableLike
        Normalized CST imports table.
    """
    _ = ctx
    return normalize_cst_imports_spans(py_cst_imports=cst_imports)


@cache(format="parquet")
@tag(layer="normalize", artifact="cst_defs_norm", kind="table")
def cst_defs_norm(cst_defs: TableLike, ctx: ExecutionContext) -> TableLike:
    """Normalize CST def spans into bstart/bend.

    Returns
    -------
    TableLike
        Normalized CST definitions table.
    """
    _ = ctx
    return normalize_cst_defs_spans(py_cst_defs=cst_defs)
