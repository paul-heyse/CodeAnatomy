"""Hamilton normalization stage functions."""

from __future__ import annotations

import pyarrow as pa
from hamilton.function_modifiers import cache, extract_fields, tag

from arrowdsl.kernels import explode_list_column
from arrowdsl.runtime import ExecutionContext
from extract.repo_scan import stable_id
from normalize.spans import (
    RepoTextIndex,
    add_scip_occurrence_byte_spans,
    build_repo_text_index,
    normalize_cst_defs_spans,
    normalize_cst_imports_spans,
)


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
    for items in table[column].to_pylist():
        if not isinstance(items, list):
            continue
        if not items:
            continue
        out.extend(str(item) for item in items if item)
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
        return pa.Table.from_pylist(
            [], schema=pa.schema([("qname_id", pa.string()), ("qname", pa.string())])
        )

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
    call_meta: dict[str, dict[str, object]] = {}
    for r in cst_callsites.select(
        [
            c
            for c in ["call_id", "path", "call_bstart", "call_bend", "qname_source"]
            if c in cst_callsites.column_names
        ]
    ).to_pylist():
        cid = r.get("call_id")
        if cid:
            call_meta[str(cid)] = r

    rows: list[dict[str, object]] = []
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
