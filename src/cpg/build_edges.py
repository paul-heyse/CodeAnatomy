"""Build CPG edge tables from relationship outputs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

import pyarrow as pa

from arrowdsl.finalize import FinalizeResult, finalize
from arrowdsl.runtime import ExecutionContext
from cpg.kinds import (
    SCIP_ROLE_DEFINITION,
    SCIP_ROLE_IMPORT,
    SCIP_ROLE_READ,
    SCIP_ROLE_WRITE,
    EdgeKind,
)
from cpg.schemas import CPG_EDGES_CONTRACT, CPG_EDGES_SCHEMA, SCHEMA_VERSION, empty_edges
from normalize.ids import stable_id


def _const_str(n: int, value: str) -> pa.Array:
    return pa.array([value] * n, type=pa.string())


def _const_i32(n: int, value: int) -> pa.Array:
    return pa.array([int(value)] * n, type=pa.int32())


def _const_f32(n: int, value: float) -> pa.Array:
    return pa.array([float(value)] * n, type=pa.float32())


def _get(table: pa.Table, col: str, *, default_type: pa.DataType) -> pa.Array:
    if col in table.column_names:
        return table[col]
    return pa.array([None] * table.num_rows, type=default_type)


@dataclass(frozen=True)
class EdgeIdInputs:
    """Inputs required to compute edge IDs."""

    src: Sequence[str | None]
    dst: Sequence[str | None]
    path: Sequence[str | None]
    bstart: Sequence[int | None]
    bend: Sequence[int | None]


@dataclass(frozen=True)
class EdgeEmitSpec:
    """Specification for emitting edge rows from a relation table."""

    edge_kind: EdgeKind
    src_col: str
    dst_col: str
    path_col: str
    bstart_col: str
    bend_col: str
    origin: str
    default_resolution_method: str


def _resolve_string_col(rel: pa.Table, col: str, *, default_value: str) -> pa.Array:
    arr = _get(rel, col, default_type=pa.string())
    if arr.null_count == rel.num_rows:
        return _const_str(rel.num_rows, default_value)
    return arr


def _resolve_float_col(rel: pa.Table, col: str, *, default_value: float) -> pa.Array:
    arr = _get(rel, col, default_type=pa.float32())
    if arr.null_count == rel.num_rows:
        return _const_f32(rel.num_rows, default_value)
    return arr


def _callsite_span_columns(table: pa.Table) -> tuple[str, str]:
    bstart_col = "call_bstart" if "call_bstart" in table.column_names else "bstart"
    bend_col = "call_bend" if "call_bend" in table.column_names else "bend"
    return bstart_col, bend_col


def _import_edge_columns(table: pa.Table) -> tuple[str, str, str]:
    src_col = "import_alias_id" if "import_alias_id" in table.column_names else "import_id"
    bstart_col = "bstart" if "bstart" in table.column_names else "alias_bstart"
    bend_col = "bend" if "bend" in table.column_names else "alias_bend"
    return src_col, bstart_col, bend_col


def _resolved_call_ids(rel_callsite_symbol: pa.Table | None) -> set[str]:
    if rel_callsite_symbol is None or "call_id" not in rel_callsite_symbol.column_names:
        return set()
    return {str(call_id) for call_id in rel_callsite_symbol["call_id"].to_pylist() if call_id}


def _filter_unresolved_qname_calls(
    rel_callsite_qname: pa.Table,
    resolved: set[str],
) -> pa.Table:
    if "call_id" not in rel_callsite_qname.column_names:
        return rel_callsite_qname
    call_ids = rel_callsite_qname["call_id"].to_pylist()
    mask = pa.array(
        [cid is not None and str(cid) not in resolved for cid in call_ids],
        type=pa.bool_(),
    )
    return rel_callsite_qname.filter(mask)


def _ensure_ambiguity_group_id(table: pa.Table) -> pa.Table:
    if "ambiguity_group_id" not in table.column_names and "call_id" in table.column_names:
        return table.append_column("ambiguity_group_id", table["call_id"])
    return table


def _role_mask_filter(
    symbol_roles: Sequence[int | None],
    mask: int,
    *,
    must_set: bool,
) -> list[bool]:
    out: list[bool] = []
    for role in symbol_roles:
        if role is None:
            out.append(not must_set)
            continue
        hit = (int(role) & int(mask)) != 0
        out.append(hit if must_set else (not hit))
    return out


def _compute_edge_ids(
    edge_kind: str,
    inputs: EdgeIdInputs,
) -> list[str | None]:
    ids: list[str | None] = []
    for s, d, p, bs, be in zip(
        inputs.src, inputs.dst, inputs.path, inputs.bstart, inputs.bend, strict=True
    ):
        if s is None or d is None:
            ids.append(None)
            continue
        if p is None or bs is None or be is None:
            ids.append(stable_id("edge", edge_kind, str(s), str(d)))
        else:
            ids.append(
                stable_id("edge", edge_kind, str(s), str(d), str(p), str(int(bs)), str(int(be)))
            )
    return ids


def _emit_edges_from_relation(
    rel: pa.Table | None,
    *,
    spec: EdgeEmitSpec,
) -> pa.Table:
    if rel is None or rel.num_rows == 0:
        return empty_edges()

    n = rel.num_rows
    src = cast("list[str | None]", _get(rel, spec.src_col, default_type=pa.string()).to_pylist())
    dst = cast("list[str | None]", _get(rel, spec.dst_col, default_type=pa.string()).to_pylist())
    path = cast("list[str | None]", _get(rel, spec.path_col, default_type=pa.string()).to_pylist())
    bstart = cast(
        "list[int | None]", _get(rel, spec.bstart_col, default_type=pa.int64()).to_pylist()
    )
    bend = cast("list[int | None]", _get(rel, spec.bend_col, default_type=pa.int64()).to_pylist())

    edge_ids = _compute_edge_ids(
        spec.edge_kind.value,
        EdgeIdInputs(src=src, dst=dst, path=path, bstart=bstart, bend=bend),
    )

    default_score = 1.0 if spec.origin == "scip" else 0.5

    out = pa.Table.from_arrays(
        [
            _const_i32(n, SCHEMA_VERSION),
            pa.array(edge_ids, type=pa.string()),
            _const_str(n, spec.edge_kind.value),
            pa.array(src, type=pa.string()),
            pa.array(dst, type=pa.string()),
            pa.array(path, type=pa.string()),
            pa.array(bstart, type=pa.int64()),
            pa.array(bend, type=pa.int64()),
            _const_str(n, spec.origin),
            _resolve_string_col(
                rel, "resolution_method", default_value=spec.default_resolution_method
            ),
            _resolve_float_col(rel, "confidence", default_value=default_score),
            _resolve_float_col(rel, "score", default_value=default_score),
            _get(rel, "symbol_roles", default_type=pa.int32()),
            _get(rel, "qname_source", default_type=pa.string()),
            _get(rel, "ambiguity_group_id", default_type=pa.string()),
            _get(rel, "rule_name", default_type=pa.string()),
            _get(rel, "rule_priority", default_type=pa.int32()),
        ],
        schema=CPG_EDGES_SCHEMA,
    )
    mask = [
        eid is not None and s is not None and d is not None
        for eid, s, d in zip(edge_ids, src, dst, strict=True)
    ]
    if not any(mask):
        return empty_edges()
    return out.filter(pa.array(mask, type=pa.bool_()))


def _symbol_role_edges(rel_name_symbol: pa.Table | None) -> list[pa.Table]:
    if rel_name_symbol is None or rel_name_symbol.num_rows == 0:
        return []

    roles = cast(
        "list[int | None]",
        _get(rel_name_symbol, "symbol_roles", default_type=pa.int32()).to_pylist(),
    )
    parts: list[pa.Table] = []

    for edge_kind, role_mask, must_set in (
        (EdgeKind.PY_DEFINES_SYMBOL, SCIP_ROLE_DEFINITION, True),
        (EdgeKind.PY_REFERENCES_SYMBOL, SCIP_ROLE_DEFINITION, False),
        (EdgeKind.PY_READS_SYMBOL, SCIP_ROLE_READ, True),
        (EdgeKind.PY_WRITES_SYMBOL, SCIP_ROLE_WRITE, True),
    ):
        mask = pa.array(_role_mask_filter(roles, role_mask, must_set=must_set), type=pa.bool_())
        parts.append(
            _emit_edges_from_relation(
                rel_name_symbol.filter(mask),
                spec=EdgeEmitSpec(
                    edge_kind=edge_kind,
                    src_col="name_ref_id",
                    dst_col="symbol",
                    path_col="path",
                    bstart_col="bstart",
                    bend_col="bend",
                    origin="scip",
                    default_resolution_method="SPAN_EXACT",
                ),
            )
        )
    return parts


def _import_symbol_edges(rel_import_symbol: pa.Table | None) -> list[pa.Table]:
    if rel_import_symbol is None or rel_import_symbol.num_rows == 0:
        return []

    roles = cast(
        "list[int | None]",
        _get(rel_import_symbol, "symbol_roles", default_type=pa.int32()).to_pylist(),
    )
    mask = pa.array(_role_mask_filter(roles, SCIP_ROLE_IMPORT, must_set=True), type=pa.bool_())
    src_col, bstart_col, bend_col = _import_edge_columns(rel_import_symbol)
    return [
        _emit_edges_from_relation(
            rel_import_symbol.filter(mask),
            spec=EdgeEmitSpec(
                edge_kind=EdgeKind.PY_IMPORTS_SYMBOL,
                src_col=src_col,
                dst_col="symbol",
                path_col="path",
                bstart_col=bstart_col,
                bend_col=bend_col,
                origin="scip",
                default_resolution_method="SPAN_EXACT",
            ),
        )
    ]


def _call_symbol_edges(rel_callsite_symbol: pa.Table | None) -> list[pa.Table]:
    if rel_callsite_symbol is None or rel_callsite_symbol.num_rows == 0:
        return []

    bstart_col, bend_col = _callsite_span_columns(rel_callsite_symbol)
    return [
        _emit_edges_from_relation(
            rel_callsite_symbol,
            spec=EdgeEmitSpec(
                edge_kind=EdgeKind.PY_CALLS_SYMBOL,
                src_col="call_id",
                dst_col="symbol",
                path_col="path",
                bstart_col=bstart_col,
                bend_col=bend_col,
                origin="scip",
                default_resolution_method="CALLEE_SPAN_EXACT",
            ),
        )
    ]


def _qname_fallback_edges(
    rel_callsite_qname: pa.Table | None,
    rel_callsite_symbol: pa.Table | None,
) -> list[pa.Table]:
    if rel_callsite_qname is None or rel_callsite_qname.num_rows == 0:
        return []

    resolved = _resolved_call_ids(rel_callsite_symbol)
    qnp = _filter_unresolved_qname_calls(rel_callsite_qname, resolved)
    if qnp.num_rows == 0:
        return []

    qnp = _ensure_ambiguity_group_id(qnp)
    bstart_col, bend_col = _callsite_span_columns(qnp)
    return [
        _emit_edges_from_relation(
            qnp,
            spec=EdgeEmitSpec(
                edge_kind=EdgeKind.PY_CALLS_QUALIFIED_NAME,
                src_col="call_id",
                dst_col="qname_id",
                path_col="path",
                bstart_col=bstart_col,
                bend_col=bend_col,
                origin="qnp",
                default_resolution_method="QNP_CALLEE_FALLBACK",
            ),
        )
    ]


@dataclass(frozen=True)
class EdgeBuildOptions:
    """Configure which edge families are emitted."""

    emit_symbol_role_edges: bool = True
    emit_import_edges: bool = True
    emit_call_edges: bool = True
    emit_qname_fallback_call_edges: bool = True


def build_cpg_edges_raw(
    *,
    relationship_outputs: Mapping[str, pa.Table] | None = None,
    options: EdgeBuildOptions | None = None,
) -> pa.Table:
    """Emit raw CPG edges without finalization.

    Returns
    -------
    pyarrow.Table
        Raw edges table.
    """
    options = options or EdgeBuildOptions()
    outputs = relationship_outputs or {}
    rel_name_symbol = outputs.get("rel_name_symbol")
    rel_import_symbol = outputs.get("rel_import_symbol")
    rel_callsite_symbol = outputs.get("rel_callsite_symbol")
    rel_callsite_qname = outputs.get("rel_callsite_qname")
    parts: list[pa.Table] = []

    if options.emit_symbol_role_edges:
        parts.extend(_symbol_role_edges(rel_name_symbol))
    if options.emit_import_edges:
        parts.extend(_import_symbol_edges(rel_import_symbol))
    if options.emit_call_edges:
        parts.extend(_call_symbol_edges(rel_callsite_symbol))
    if options.emit_qname_fallback_call_edges:
        parts.extend(_qname_fallback_edges(rel_callsite_qname, rel_callsite_symbol))

    if not parts:
        return empty_edges()

    return pa.concat_tables(parts, promote=True)


def build_cpg_edges(
    *,
    ctx: ExecutionContext,
    relationship_outputs: Mapping[str, pa.Table] | None = None,
    options: EdgeBuildOptions | None = None,
) -> FinalizeResult:
    """Build and finalize CPG edges.

    Returns
    -------
    FinalizeResult
        Finalized edges tables and stats.
    """
    raw = build_cpg_edges_raw(
        relationship_outputs=relationship_outputs,
        options=options,
    )
    return finalize(raw, contract=CPG_EDGES_CONTRACT, ctx=ctx)
