"""Hamilton normalization stage functions."""

from __future__ import annotations

from hamilton.function_modifiers import cache, extract_fields, tag

import arrowdsl.pyarrow_core as pa
from arrowdsl.ids import prefixed_hash_id
from arrowdsl.iter import iter_array_values, iter_arrays
from arrowdsl.kernels import explode_list_column
from arrowdsl.pyarrow_protocols import TableLike
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
from schema_spec.core import ArrowFieldSpec
from schema_spec.factories import make_table_spec
from schema_spec.fields import call_span_bundle
from schema_spec.registry import GLOBAL_SCHEMA_REGISTRY

SCHEMA_VERSION = 1

QNAME_DIM_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
        name="dim_qualified_names_v1",
        version=SCHEMA_VERSION,
        bundles=(),
        fields=[
            ArrowFieldSpec(name="qname_id", dtype=pa.string()),
            ArrowFieldSpec(name="qname", dtype=pa.string()),
        ],
    )
)

CALLSITE_QNAME_CANDIDATES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
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


def _collect_string_lists(table: TableLike | None, column: str) -> list[str]:
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
    qnames = _collect_string_lists(cst_callsites, "callee_qnames")
    qnames.extend(_collect_string_lists(cst_defs, "qnames"))

    if not qnames:
        return pa.Table.from_pylist([], schema=QNAME_DIM_SCHEMA)

    # distinct + deterministic
    uniq = sorted(set(qnames))
    qname_array = pa.array(uniq, type=pa.string())
    qname_ids = prefixed_hash_id([qname_array], prefix="qname")

    return pa.Table.from_arrays(
        [qname_ids, qname_array],
        names=["qname_id", "qname"],
    )


@cache()
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


@cache()
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


@cache()
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


@cache()
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


@cache()
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


@cache()
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


@cache()
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


@cache()
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


@cache()
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


@cache()
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


@cache()
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
