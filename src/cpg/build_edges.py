from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

import pyarrow as pa

from ..arrowdsl.finalize import FinalizeResult, finalize
from ..arrowdsl.runtime import ExecutionContext
from ..normalize.ids import stable_id
from .kinds import (
    EdgeKind,
    SCIP_ROLE_DEFINITION,
    SCIP_ROLE_IMPORT,
    SCIP_ROLE_READ,
    SCIP_ROLE_WRITE,
)
from .schemas import SCHEMA_VERSION, CPG_EDGES_CONTRACT, CPG_EDGES_SCHEMA, empty_edges


def _const_str(n: int, value: str) -> pa.Array:
    return pa.array([value] * n, type=pa.string())


def _const_i32(n: int, value: int) -> pa.Array:
    return pa.array([int(value)] * n, type=pa.int32())


def _const_f32(n: int, value: float) -> pa.Array:
    return pa.array([float(value)] * n, type=pa.float32())


def _get(t: pa.Table, col: str, *, default_type: pa.DataType) -> pa.Array:
    if col in t.column_names:
        return t[col]
    return pa.array([None] * t.num_rows, type=default_type)


def _role_mask_filter(symbol_roles: Sequence[Optional[int]], mask: int, *, must_set: bool) -> list[bool]:
    out: list[bool] = []
    for r in symbol_roles:
        if r is None:
            out.append(False if must_set else True)
            continue
        hit = (int(r) & int(mask)) != 0
        out.append(hit if must_set else (not hit))
    return out


def _compute_edge_ids(
    edge_kind: str,
    src: list,
    dst: list,
    path: list,
    bstart: list,
    bend: list,
) -> list[Optional[str]]:
    ids: list[Optional[str]] = []
    for s, d, p, bs, be in zip(src, dst, path, bstart, bend):
        if s is None or d is None:
            ids.append(None)
            continue
        # path/span may be None (allowed) but included if present
        if p is None or bs is None or be is None:
            ids.append(stable_id("edge", edge_kind, str(s), str(d)))
        else:
            ids.append(stable_id("edge", edge_kind, str(s), str(d), str(p), str(int(bs)), str(int(be))))
    return ids


def _emit_edges_from_relation(
    rel: pa.Table,
    *,
    edge_kind: EdgeKind,
    src_col: str,
    dst_col: str,
    path_col: str,
    bstart_col: str,
    bend_col: str,
    origin: str,
    default_resolution_method: str,
) -> pa.Table:
    if rel is None or rel.num_rows == 0:
        return empty_edges()

    n = rel.num_rows
    src = _get(rel, src_col, default_type=pa.string()).to_pylist()
    dst = _get(rel, dst_col, default_type=pa.string()).to_pylist()
    path = _get(rel, path_col, default_type=pa.string()).to_pylist()
    bstart = _get(rel, bstart_col, default_type=pa.int64()).to_pylist()
    bend = _get(rel, bend_col, default_type=pa.int64()).to_pylist()

    edge_ids = _compute_edge_ids(edge_kind.value, src, dst, path, bstart, bend)

    # Pull metadata fields if present, else fill defaults
    origin_arr = _const_str(n, origin)
    res_method = _get(rel, "resolution_method", default_type=pa.string())
    if res_method.null_count == n:
        res_method = _const_str(n, default_resolution_method)

    confidence = _get(rel, "confidence", default_type=pa.float32())
    if confidence.null_count == n:
        confidence = _const_f32(n, 1.0 if origin == "scip" else 0.5)

    score = _get(rel, "score", default_type=pa.float32())
    if score.null_count == n:
        score = _const_f32(n, 1.0 if origin == "scip" else 0.5)

    symbol_roles = _get(rel, "symbol_roles", default_type=pa.int32())
    qname_source = _get(rel, "qname_source", default_type=pa.string())
    ambiguity_group_id = _get(rel, "ambiguity_group_id", default_type=pa.string())

    rule_name = _get(rel, "rule_name", default_type=pa.string())
    rule_priority = _get(rel, "rule_priority", default_type=pa.int32())

    out = pa.Table.from_arrays(
        [
            _const_i32(n, SCHEMA_VERSION),
            pa.array(edge_ids, type=pa.string()),
            _const_str(n, edge_kind.value),
            pa.array(src, type=pa.string()),
            pa.array(dst, type=pa.string()),
            pa.array(path, type=pa.string()),
            pa.array(bstart, type=pa.int64()),
            pa.array(bend, type=pa.int64()),
            origin_arr,
            res_method,
            confidence,
            score,
            symbol_roles,
            qname_source,
            ambiguity_group_id,
            rule_name,
            rule_priority,
        ],
        schema=CPG_EDGES_SCHEMA,
    )
    # Drop rows with missing edge_id/src/dst (finalize will also catch these, but better here)
    mask = [eid is not None and s is not None and d is not None for eid, s, d in zip(edge_ids, src, dst)]
    if not any(mask):
        return empty_edges()
    return out.filter(pa.array(mask, type=pa.bool_()))


@dataclass(frozen=True)
class EdgeBuildOptions:
    """
    Edge emission toggles.

    This builder expects finalized relationship datasets (relspec outputs), but it is tolerant:
    missing inputs => that edge family is skipped.
    """
    emit_symbol_role_edges: bool = True
    emit_import_edges: bool = True
    emit_call_edges: bool = True
    emit_qname_fallback_call_edges: bool = True


def build_cpg_edges_raw(
    *,
    rel_name_symbol: Optional[pa.Table] = None,
    rel_import_symbol: Optional[pa.Table] = None,
    rel_callsite_symbol: Optional[pa.Table] = None,
    rel_callsite_qname: Optional[pa.Table] = None,
    options: Optional[EdgeBuildOptions] = None,
) -> pa.Table:
    """
    Emit CPG edges (raw, not finalized).
    """
    options = options or EdgeBuildOptions()
    parts: list[pa.Table] = []

    # --- Role-driven name->symbol edges ---
    if options.emit_symbol_role_edges and rel_name_symbol is not None and rel_name_symbol.num_rows:
        roles = _get(rel_name_symbol, "symbol_roles", default_type=pa.int32()).to_pylist()

        # defines: Definition bit set
        mask_def = pa.array(_role_mask_filter(roles, SCIP_ROLE_DEFINITION, must_set=True), type=pa.bool_())
        parts.append(
            _emit_edges_from_relation(
                rel_name_symbol.filter(mask_def),
                edge_kind=EdgeKind.PY_DEFINES_SYMBOL,
                src_col="name_ref_id",
                dst_col="symbol",
                path_col="path",
                bstart_col="bstart",
                bend_col="bend",
                origin="scip",
                default_resolution_method="SPAN_EXACT",
            )
        )

        # references: Definition bit NOT set
        mask_ref = pa.array(_role_mask_filter(roles, SCIP_ROLE_DEFINITION, must_set=False), type=pa.bool_())
        parts.append(
            _emit_edges_from_relation(
                rel_name_symbol.filter(mask_ref),
                edge_kind=EdgeKind.PY_REFERENCES_SYMBOL,
                src_col="name_ref_id",
                dst_col="symbol",
                path_col="path",
                bstart_col="bstart",
                bend_col="bend",
                origin="scip",
                default_resolution_method="SPAN_EXACT",
            )
        )

        # reads: ReadAccess bit set
        mask_read = pa.array(_role_mask_filter(roles, SCIP_ROLE_READ, must_set=True), type=pa.bool_())
        parts.append(
            _emit_edges_from_relation(
                rel_name_symbol.filter(mask_read),
                edge_kind=EdgeKind.PY_READS_SYMBOL,
                src_col="name_ref_id",
                dst_col="symbol",
                path_col="path",
                bstart_col="bstart",
                bend_col="bend",
                origin="scip",
                default_resolution_method="SPAN_EXACT",
            )
        )

        # writes: WriteAccess bit set
        mask_write = pa.array(_role_mask_filter(roles, SCIP_ROLE_WRITE, must_set=True), type=pa.bool_())
        parts.append(
            _emit_edges_from_relation(
                rel_name_symbol.filter(mask_write),
                edge_kind=EdgeKind.PY_WRITES_SYMBOL,
                src_col="name_ref_id",
                dst_col="symbol",
                path_col="path",
                bstart_col="bstart",
                bend_col="bend",
                origin="scip",
                default_resolution_method="SPAN_EXACT",
            )
        )

    # --- Import edges (import alias -> symbol) ---
    if options.emit_import_edges and rel_import_symbol is not None and rel_import_symbol.num_rows:
        roles = _get(rel_import_symbol, "symbol_roles", default_type=pa.int32()).to_pylist()
        mask_imp = pa.array(_role_mask_filter(roles, SCIP_ROLE_IMPORT, must_set=True), type=pa.bool_())

        src_col = "import_alias_id" if "import_alias_id" in rel_import_symbol.column_names else "import_id"
        # evidence: alias span is preferred; normalized imports should have bstart/bend
        bstart_col = "bstart" if "bstart" in rel_import_symbol.column_names else "alias_bstart"
        bend_col = "bend" if "bend" in rel_import_symbol.column_names else "alias_bend"

        parts.append(
            _emit_edges_from_relation(
                rel_import_symbol.filter(mask_imp),
                edge_kind=EdgeKind.PY_IMPORTS_SYMBOL,
                src_col=src_col,
                dst_col="symbol",
                path_col="path",
                bstart_col=bstart_col,
                bend_col=bend_col,
                origin="scip",
                default_resolution_method="SPAN_EXACT",
            )
        )

    # --- Call edges: preferred callsite -> symbol ---
    if options.emit_call_edges and rel_callsite_symbol is not None and rel_callsite_symbol.num_rows:
        parts.append(
            _emit_edges_from_relation(
                rel_callsite_symbol,
                edge_kind=EdgeKind.PY_CALLS_SYMBOL,
                src_col="call_id",
                dst_col="symbol",
                path_col="path",
                bstart_col="call_bstart" if "call_bstart" in rel_callsite_symbol.column_names else "bstart",
                bend_col="call_bend" if "call_bend" in rel_callsite_symbol.column_names else "bend",
                origin="scip",
                default_resolution_method="CALLEE_SPAN_EXACT",
            )
        )

    # --- Call edges fallback: callsite -> qualified_name candidates (only if no SCIP callee symbol) ---
    if (
        options.emit_qname_fallback_call_edges
        and rel_callsite_qname is not None
        and rel_callsite_qname.num_rows
    ):
        # Determine which call_ids are already resolved by scip
        resolved: set[str] = set()
        if rel_callsite_symbol is not None and "call_id" in rel_callsite_symbol.column_names:
            for cid in rel_callsite_symbol["call_id"].to_pylist():
                if cid:
                    resolved.add(str(cid))

        # Filter rel_callsite_qname to unresolved call_ids
        if "call_id" in rel_callsite_qname.column_names:
            mask = pa.array(
                [False if (cid is None or str(cid) in resolved) else True for cid in rel_callsite_qname["call_id"].to_pylist()],
                type=pa.bool_(),
            )
            qnp = rel_callsite_qname.filter(mask)
        else:
            qnp = rel_callsite_qname

        # Ensure ambiguity_group_id exists (call_id)
        if qnp.num_rows:
            if "ambiguity_group_id" not in qnp.column_names and "call_id" in qnp.column_names:
                qnp = qnp.append_column("ambiguity_group_id", qnp["call_id"])

            parts.append(
                _emit_edges_from_relation(
                    qnp,
                    edge_kind=EdgeKind.PY_CALLS_QUALIFIED_NAME,
                    src_col="call_id",
                    dst_col="qname_id",
                    path_col="path",
                    bstart_col="call_bstart" if "call_bstart" in qnp.column_names else "bstart",
                    bend_col="call_bend" if "call_bend" in qnp.column_names else "bend",
                    origin="qnp",
                    default_resolution_method="QNP_CALLEE_FALLBACK",
                )
            )

    if not parts:
        return empty_edges()

    return pa.concat_tables(parts, promote=True)


def build_cpg_edges(
    *,
    ctx: ExecutionContext,
    rel_name_symbol: Optional[pa.Table] = None,
    rel_import_symbol: Optional[pa.Table] = None,
    rel_callsite_symbol: Optional[pa.Table] = None,
    rel_callsite_qname: Optional[pa.Table] = None,
    options: Optional[EdgeBuildOptions] = None,
) -> FinalizeResult:
    """
    Build + finalize cpg_edges (schema alignment, dedupe, canonical sort).
    """
    raw = build_cpg_edges_raw(
        rel_name_symbol=rel_name_symbol,
        rel_import_symbol=rel_import_symbol,
        rel_callsite_symbol=rel_callsite_symbol,
        rel_callsite_qname=rel_callsite_qname,
        options=options,
    )
    return finalize(raw, contract=CPG_EDGES_CONTRACT, ctx=ctx)
