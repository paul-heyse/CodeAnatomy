from __future__ import annotations

from typing import Dict, List, Optional

import pyarrow as pa
from hamilton.function_modifiers import cache, extract_fields, tag

from ...arrowdsl.runtime import ExecutionContext
from ...normalize.spans import (
    RepoTextIndex,
    build_repo_text_index,
    add_scip_occurrence_byte_spans,
    normalize_cst_imports_spans,
    normalize_cst_defs_spans,
)

@cache()
@tag(layer="normalize", artifact="dim_qualified_names", kind="table")
def dim_qualified_names(
    cst_callsites: pa.Table,
    cst_defs: pa.Table,
    ctx: ExecutionContext,
) -> pa.Table:
    """
    Build a dimension table of qualified names encountered in CST extraction.

    Expected output columns:
      - qname_id (stable)
      - qname (string)
    """
    qnames: List[str] = []

    # From callsites: callee_qnames may be a list of strings per row
    if cst_callsites is not None and "callee_qnames" in cst_callsites.column_names:
        for lst in cst_callsites["callee_qnames"].to_pylist():
            if not lst:
                continue
            for s in lst:
                if s:
                    qnames.append(str(s))

    # From defs: qnames may be list[str]
    if cst_defs is not None and "qnames" in cst_defs.column_names:
        for lst in cst_defs["qnames"].to_pylist():
            if not lst:
                continue
            for s in lst:
                if s:
                    qnames.append(str(s))

    if not qnames:
        return pa.Table.from_pylist([], schema=pa.schema([("qname_id", pa.string()), ("qname", pa.string())]))

    # distinct + deterministic
    uniq = sorted(set(qnames))
    qname_ids = [stable_id("qname", s) for s in uniq]

    return pa.Table.from_arrays(
        [pa.array(qname_ids, type=pa.string()), pa.array(uniq, type=pa.string())],
        names=["qname_id", "qname"],
    )


@cache()
@tag(layer="normalize", artifact="callsite_qname_candidates", kind="table")
def callsite_qname_candidates(
    cst_callsites: pa.Table,
    ctx: ExecutionContext,
) -> pa.Table:
    """
    Explode cst_callsites.callee_qnames into a row-per-candidate table.

    Output columns (minimum):
      - call_id
      - qname
      - path
      - call_bstart
      - call_bend
      - qname_source (optional)
    """
    if cst_callsites is None or cst_callsites.num_rows == 0 or "callee_qnames" not in cst_callsites.column_names:
        return pa.Table.from_pylist(
            [],
            schema=pa.schema(
                [
                    ("call_id", pa.string()),
                    ("qname", pa.string()),
                    ("path", pa.string()),
                    ("call_bstart", pa.int64()),
                    ("call_bend", pa.int64()),
                    ("qname_source", pa.string()),
                ]
            ),
        )

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
    call_meta: Dict[str, Dict[str, object]] = {}
    for r in cst_callsites.select(
        [c for c in ["call_id", "path", "call_bstart", "call_bend", "qname_source"] if c in cst_callsites.column_names]
    ).to_pylist():
        cid = r.get("call_id")
        if cid:
            call_meta[str(cid)] = r

    rows: List[Dict[str, object]] = []
    for r in exploded.to_pylist():
        cid = r.get("call_id")
        qn = r.get("qname")
        if not cid or not qn:
            continue
        meta = call_meta.get(str(cid), {})
        rows.append(
            {
                "call_id": str(cid),
                "qname": str(qn),
                "path": meta.get("path"),
                "call_bstart": meta.get("call_bstart"),
                "call_bend": meta.get("call_bend"),
                "qname_source": meta.get("qname_source"),
            }
        )

    return pa.Table.from_pylist(rows)

@cache()
@tag(layer="normalize", artifact="repo_text_index", kind="object")
def repo_text_index(repo_root: str, repo_files: pa.Table, ctx: ExecutionContext) -> RepoTextIndex:
    """
    Builds a repo text index used to convert (line,col) → byte offsets.
    """
    return build_repo_text_index(repo_root=repo_root, repo_files=repo_files, ctx=ctx)


@cache()
@extract_fields(
    scip_occurrences_norm=pa.Table,
    scip_span_errors=pa.Table,
)
@tag(layer="normalize", artifact="scip_occurrences_norm_bundle", kind="bundle")
def scip_occurrences_norm_bundle(
    scip_occurrences: pa.Table,
    repo_text_index: RepoTextIndex,
    ctx: ExecutionContext,
) -> Dict[str, pa.Table]:
    """
    Converts SCIP occurrence (line/col spans) into byte offsets bstart/bend.
    """
    occ, errs = add_scip_occurrence_byte_spans(
        scip_occurrences=scip_occurrences,
        repo_text_index=repo_text_index,
        ctx=ctx,
    )
    return {"scip_occurrences_norm": occ, "scip_span_errors": errs}


@cache()
@tag(layer="normalize", artifact="cst_imports_norm", kind="table")
def cst_imports_norm(cst_imports: pa.Table, ctx: ExecutionContext) -> pa.Table:
    """
    Normalizes CST import spans into bstart/bend (alias span preferred).
    """
    return normalize_cst_imports_spans(cst_imports=cst_imports, ctx=ctx)


@cache()
@tag(layer="normalize", artifact="cst_defs_norm", kind="table")
def cst_defs_norm(cst_defs: pa.Table, ctx: ExecutionContext) -> pa.Table:
    """
    Normalizes CST def spans into bstart/bend.
    """
    return normalize_cst_defs_spans(cst_defs=cst_defs, ctx=ctx)