"""Hamilton normalization stage functions."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
from hamilton.function_modifiers import cache, extract_fields, tag

from arrowdsl.ids import hash64_from_arrays
from arrowdsl.iter import iter_array_values, iter_arrays
from arrowdsl.kernels import explode_list_column
from arrowdsl.runtime import ExecutionContext
from normalize.bytecode_anchor import anchor_instructions
from normalize.bytecode_cfg import build_cfg_blocks, build_cfg_edges
from normalize.bytecode_dfg import build_def_use_events, run_reaching_defs
from normalize.diagnostics import DiagnosticsSources, collect_diags
from normalize.spans import (
    RepoTextIndex,
    add_ast_byte_spans,
    add_scip_occurrence_byte_spans,
    build_repo_text_index,
    normalize_cst_defs_spans,
    normalize_cst_imports_spans,
)
from normalize.types import normalize_type_exprs, normalize_types
from schema_spec.core import ArrowFieldSpec, TableSchemaSpec

QNAME_DIM_SPEC = TableSchemaSpec(
    name="dim_qualified_names_v1",
    fields=[
        ArrowFieldSpec(name="qname_id", dtype=pa.string()),
        ArrowFieldSpec(name="qname", dtype=pa.string()),
    ],
)

CALLSITE_QNAME_CANDIDATES_SPEC = TableSchemaSpec(
    name="callsite_qname_candidates_v1",
    fields=[
        ArrowFieldSpec(name="call_id", dtype=pa.string()),
        ArrowFieldSpec(name="qname", dtype=pa.string()),
        ArrowFieldSpec(name="path", dtype=pa.string()),
        ArrowFieldSpec(name="call_bstart", dtype=pa.int64()),
        ArrowFieldSpec(name="call_bend", dtype=pa.int64()),
        ArrowFieldSpec(name="qname_source", dtype=pa.string()),
    ],
)

QNAME_DIM_SCHEMA = QNAME_DIM_SPEC.to_arrow_schema()
CALLSITE_QNAME_CANDIDATES_SCHEMA = CALLSITE_QNAME_CANDIDATES_SPEC.to_arrow_schema()


def _qname_fields(value: object) -> tuple[str | None, str | None]:
    if isinstance(value, dict):
        name = value.get("name")
        source = value.get("source")
        return (str(name) if name else None, str(source) if source else None)
    if isinstance(value, str):
        return value, None
    return None, None


def _collect_string_lists(table: pa.Table | None, column: str) -> list[str]:
    """Flatten a list-valued column into a list of strings.

    Returns
    -------
    list[str]
        Flattened string values.
    """
    if table is None or column not in table.column_names:
        return []
    out: list[str] = []
    for items in iter_array_values(table[column]):
        if not isinstance(items, list):
            continue
        if not items:
            continue
        for item in items:
            name, _ = _qname_fields(item)
            if name:
                out.append(name)
    return out


@cache()
@tag(layer="normalize", artifact="dim_qualified_names", kind="table")
def dim_qualified_names(
    cst_callsites: pa.Table,
    cst_defs: pa.Table,
    ctx: ExecutionContext,
) -> pa.Table:
    """Build a dimension table of qualified names from CST extraction.

    Expected output columns:
      - qname_id (stable)
      - qname (string)

    Returns
    -------
    pa.Table
        Qualified name dimension table.
    """
    _ = ctx
    qnames = _collect_string_lists(cst_callsites, "callee_qnames")
    qnames.extend(_collect_string_lists(cst_defs, "qnames"))

    if not qnames:
        return pa.Table.from_pylist([], schema=QNAME_DIM_SCHEMA)

    # distinct + deterministic
    uniq = sorted(set(qnames))
    qname_array = pa.array(uniq, type=pa.string())
    qname_hash = hash64_from_arrays([qname_array], prefix="qname")
    qname_ids = pc.binary_join_element_wise(
        pa.scalar("qname"), pc.cast(qname_hash, pa.string()), ":"
    )

    return pa.Table.from_arrays(
        [qname_ids, qname_array],
        names=["qname_id", "qname"],
    )


@cache()
@tag(layer="normalize", artifact="callsite_qname_candidates", kind="table")
def callsite_qname_candidates(
    cst_callsites: pa.Table,
    ctx: ExecutionContext,
) -> pa.Table:
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
    pa.Table
        Table of callsite qualified name candidates.
    """
    _ = ctx
    if (
        cst_callsites is None
        or cst_callsites.num_rows == 0
        or "callee_qnames" not in cst_callsites.column_names
    ):
        return pa.Table.from_pylist([], schema=CALLSITE_QNAME_CANDIDATES_SCHEMA)

    # explode list column -> (call_id, qname)
    exploded = explode_list_column(
        cst_callsites,
        parent_id_col="call_id",
        list_col="callee_qnames",
        out_parent_col="call_id",
        out_value_col="qname",
    )

    # bring along evidence span columns by joining back on call_id (cheap)
    # For simplicity we do python-side index; for large scale convert to hash join later.
    call_meta: dict[str, dict[str, object]] = {}
    meta_cols = [
        c
        for c in ["call_id", "path", "call_bstart", "call_bend", "qname_source"]
        if c in cst_callsites.column_names
    ]
    if meta_cols:
        meta_table = cst_callsites.select(meta_cols)
        meta_arrays = [meta_table[col] for col in meta_cols]
        for values in iter_arrays(meta_arrays):
            row = dict(zip(meta_cols, values, strict=True))
            cid = row.get("call_id")
            if cid:
                call_meta[str(cid)] = row

    rows: list[dict[str, object]] = []
    if "call_id" in exploded.column_names and "qname" in exploded.column_names:
        exploded_arrays = [exploded["call_id"], exploded["qname"]]
        for cid, qn in iter_arrays(exploded_arrays):
            if not cid or not qn:
                continue
            qname, qname_source = _qname_fields(qn)
            if qname is None:
                continue
            meta = call_meta.get(str(cid), {})
            rows.append(
                {
                    "call_id": str(cid),
                    "qname": qname,
                    "path": meta.get("path"),
                    "call_bstart": meta.get("call_bstart"),
                    "call_bend": meta.get("call_bend"),
                    "qname_source": qname_source or meta.get("qname_source"),
                }
            )

    return pa.Table.from_pylist(rows)


@cache()
@tag(layer="normalize", artifact="ast_nodes_norm", kind="table")
def ast_nodes_norm(
    repo_text_index: RepoTextIndex,
    ast_nodes: pa.Table,
    ctx: ExecutionContext,
) -> pa.Table:
    """Add byte-span columns to AST nodes for join-ready alignment.

    Returns
    -------
    pa.Table
        AST nodes with bstart/bend/span_ok columns appended.
    """
    _ = ctx
    return add_ast_byte_spans(repo_text_index, ast_nodes)


@cache()
@tag(layer="normalize", artifact="py_bc_instructions_norm", kind="table")
def py_bc_instructions_norm(
    repo_text_index: RepoTextIndex,
    py_bc_instructions: pa.Table,
    ctx: ExecutionContext,
) -> pa.Table:
    """Anchor bytecode instructions to source byte spans.

    Returns
    -------
    pa.Table
        Bytecode instruction table with bstart/bend/span_ok columns.
    """
    _ = ctx
    return anchor_instructions(repo_text_index, py_bc_instructions)


@cache()
@tag(layer="normalize", artifact="py_bc_blocks_norm", kind="table")
def py_bc_blocks_norm(
    py_bc_blocks: pa.Table,
    py_bc_code_units: pa.Table,
    ctx: ExecutionContext,
) -> pa.Table:
    """Normalize bytecode CFG blocks with file/path metadata.

    Returns
    -------
    pa.Table
        CFG block table aligned to the normalization schema.
    """
    return build_cfg_blocks(py_bc_blocks, py_bc_code_units, ctx=ctx)


@cache()
@tag(layer="normalize", artifact="py_bc_cfg_edges_norm", kind="table")
def py_bc_cfg_edges_norm(
    py_bc_code_units: pa.Table,
    py_bc_cfg_edges: pa.Table,
    ctx: ExecutionContext,
) -> pa.Table:
    """Normalize bytecode CFG edges with file/path metadata.

    Returns
    -------
    pa.Table
        CFG edge table aligned to the normalization schema.
    """
    return build_cfg_edges(py_bc_code_units, py_bc_cfg_edges, ctx=ctx)


@cache()
@tag(layer="normalize", artifact="py_bc_def_use_events", kind="table")
def py_bc_def_use_events(py_bc_instructions: pa.Table, ctx: ExecutionContext) -> pa.Table:
    """Derive def/use events from bytecode instructions.

    Returns
    -------
    pa.Table
        Def/use events table.
    """
    _ = ctx
    return build_def_use_events(py_bc_instructions)


@cache()
@tag(layer="normalize", artifact="py_bc_reaching_defs", kind="table")
def py_bc_reaching_defs(py_bc_def_use_events: pa.Table, ctx: ExecutionContext) -> pa.Table:
    """Compute reaching-def edges from def/use events.

    Returns
    -------
    pa.Table
        Reaching-def edges table.
    """
    _ = ctx
    return run_reaching_defs(py_bc_def_use_events)


@cache()
@tag(layer="normalize", artifact="type_exprs_norm", kind="table")
def type_exprs_norm(cst_type_exprs: pa.Table, ctx: ExecutionContext) -> pa.Table:
    """Normalize CST type expressions into join-ready tables.

    Returns
    -------
    pa.Table
        Normalized type expressions table.
    """
    _ = ctx
    return normalize_type_exprs(cst_type_exprs)


@cache()
@tag(layer="normalize", artifact="types_norm", kind="table")
def types_norm(
    type_exprs_norm: pa.Table,
    scip_symbol_information: pa.Table,
    ctx: ExecutionContext,
) -> pa.Table:
    """Normalize type expressions into type nodes.

    Returns
    -------
    pa.Table
        Normalized type node table.
    """
    _ = ctx
    return normalize_types(type_exprs_norm, scip_symbol_information)


@cache()
@tag(layer="normalize", artifact="diagnostics_sources", kind="object")
def diagnostics_sources(
    cst_parse_errors: pa.Table,
    ts_errors: pa.Table,
    ts_missing: pa.Table,
    scip_diagnostics: pa.Table,
    scip_documents: pa.Table,
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


@cache()
@tag(layer="normalize", artifact="diagnostics_norm", kind="table")
def diagnostics_norm(
    repo_text_index: RepoTextIndex,
    diagnostics_sources: DiagnosticsSources,
    ctx: ExecutionContext,
) -> pa.Table:
    """Aggregate diagnostics into a normalized table.

    Returns
    -------
    pa.Table
        Normalized diagnostics table.
    """
    _ = ctx
    return collect_diags(repo_text_index, sources=diagnostics_sources)


@cache()
@tag(layer="normalize", artifact="repo_text_index", kind="object")
def repo_text_index(repo_root: str, repo_files: pa.Table, ctx: ExecutionContext) -> RepoTextIndex:
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
        "scip_occurrences_norm": pa.Table,
        "scip_span_errors": pa.Table,
    }
)
@tag(layer="normalize", artifact="scip_occurrences_norm_bundle", kind="bundle")
def scip_occurrences_norm_bundle(
    scip_documents: pa.Table,
    scip_occurrences: pa.Table,
    repo_text_index: RepoTextIndex,
    ctx: ExecutionContext,
) -> dict[str, pa.Table]:
    """Convert SCIP occurrences into byte offsets.

    Returns
    -------
    dict[str, pa.Table]
        Bundle with normalized occurrences and span errors.
    """
    occ, errs = add_scip_occurrence_byte_spans(
        scip_documents=scip_documents,
        scip_occurrences=scip_occurrences,
        repo_text_index=repo_text_index,
        ctx=ctx,
    )
    return {"scip_occurrences_norm": occ, "scip_span_errors": errs}


@cache()
@tag(layer="normalize", artifact="cst_imports_norm", kind="table")
def cst_imports_norm(cst_imports: pa.Table, ctx: ExecutionContext) -> pa.Table:
    """Normalize CST import spans into bstart/bend.

    Returns
    -------
    pa.Table
        Normalized CST imports table.
    """
    _ = ctx
    return normalize_cst_imports_spans(py_cst_imports=cst_imports)


@cache()
@tag(layer="normalize", artifact="cst_defs_norm", kind="table")
def cst_defs_norm(cst_defs: pa.Table, ctx: ExecutionContext) -> pa.Table:
    """Normalize CST def spans into bstart/bend.

    Returns
    -------
    pa.Table
        Normalized CST definitions table.
    """
    _ = ctx
    return normalize_cst_defs_spans(py_cst_defs=cst_defs)
