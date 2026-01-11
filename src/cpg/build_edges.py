"""Build CPG edge tables from relationship outputs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

from arrowdsl.finalize import FinalizeResult, finalize
from arrowdsl.ids import hash64_from_arrays
from arrowdsl.runtime import ExecutionContext
from cpg.kinds import (
    SCIP_ROLE_DEFINITION,
    SCIP_ROLE_IMPORT,
    SCIP_ROLE_READ,
    SCIP_ROLE_WRITE,
    EdgeKind,
)
from cpg.schemas import CPG_EDGES_CONTRACT, CPG_EDGES_SCHEMA, SCHEMA_VERSION, empty_edges


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


def _set_or_append_column(
    table: pa.Table, name: str, values: pa.Array | pa.ChunkedArray
) -> pa.Table:
    if name in table.column_names:
        idx = table.schema.get_field_index(name)
        return table.set_column(idx, name, values)
    return table.append_column(name, values)


def _callsite_span_columns(table: pa.Table) -> tuple[str, str]:
    bstart_col = "call_bstart" if "call_bstart" in table.column_names else "bstart"
    bend_col = "call_bend" if "call_bend" in table.column_names else "bend"
    return bstart_col, bend_col


def _import_edge_columns(table: pa.Table) -> tuple[str, str, str]:
    src_col = "import_alias_id" if "import_alias_id" in table.column_names else "import_id"
    bstart_col = "bstart" if "bstart" in table.column_names else "alias_bstart"
    bend_col = "bend" if "bend" in table.column_names else "alias_bend"
    return src_col, bstart_col, bend_col


def _filter_unresolved_qname_calls(
    rel_callsite_qname: pa.Table,
    rel_callsite_symbol: pa.Table | None,
) -> pa.Table:
    if "call_id" not in rel_callsite_qname.column_names:
        return rel_callsite_qname
    if rel_callsite_symbol is None or "call_id" not in rel_callsite_symbol.column_names:
        return rel_callsite_qname
    resolved = pc.drop_null(rel_callsite_symbol["call_id"])
    mask = pc.is_in(rel_callsite_qname["call_id"], value_set=resolved)
    mask = pc.fill_null(mask, replacement=False)
    return rel_callsite_qname.filter(pc.invert(mask))


def _ensure_ambiguity_group_id(table: pa.Table) -> pa.Table:
    if "ambiguity_group_id" not in table.column_names and "call_id" in table.column_names:
        return table.append_column("ambiguity_group_id", table["call_id"])
    return table


def _with_repo_file_ids(diag_table: pa.Table, repo_files: pa.Table | None) -> pa.Table:
    if (
        repo_files is None
        or repo_files.num_rows == 0
        or "path" not in diag_table.column_names
        or "path" not in repo_files.column_names
        or "file_id" not in repo_files.column_names
    ):
        return diag_table
    repo_subset = repo_files.select(["path", "file_id"])
    joined = diag_table.join(
        repo_subset, keys=["path"], join_type="left_outer", right_suffix="_repo"
    )
    if "file_id_repo" in joined.column_names and "file_id" in joined.column_names:
        resolved = pc.coalesce(joined["file_id"], joined["file_id_repo"])
        joined = _set_or_append_column(joined, "file_id", resolved)
        joined = joined.drop(["file_id_repo"])
    return joined


def _severity_score_array(
    severity: pa.Array | pa.ChunkedArray,
) -> pa.Array | pa.ChunkedArray:
    severity_str = pc.cast(severity, pa.string())
    severity_str = pc.fill_null(severity_str, replacement="ERROR")
    is_error = pc.equal(severity_str, pa.scalar("ERROR"))
    is_warning = pc.equal(severity_str, pa.scalar("WARNING"))
    score = pc.if_else(
        is_error, pa.scalar(1.0), pc.if_else(is_warning, pa.scalar(0.7), pa.scalar(0.5))
    )
    return pc.cast(score, pa.float32())


def _role_mask(
    symbol_roles: pa.Array | pa.ChunkedArray,
    mask: int,
    *,
    must_set: bool,
) -> pa.Array | pa.ChunkedArray:
    roles = pc.cast(symbol_roles, pa.int64())
    hit = pc.not_equal(pc.bit_wise_and(roles, pa.scalar(int(mask))), pa.scalar(0))
    hit = pc.fill_null(hit, replacement=False)
    return hit if must_set else pc.invert(hit)


@dataclass(frozen=True)
class EdgeIdArrays:
    """Column arrays required to compute edge IDs."""

    src: pa.Array | pa.ChunkedArray
    dst: pa.Array | pa.ChunkedArray
    path: pa.Array | pa.ChunkedArray
    bstart: pa.Array | pa.ChunkedArray
    bend: pa.Array | pa.ChunkedArray


def _edge_id_array(
    *,
    edge_kind: str,
    inputs: EdgeIdArrays,
) -> pa.Array | pa.ChunkedArray:
    n = len(inputs.src)
    kind_arr = _const_str(n, edge_kind)
    base_hash = hash64_from_arrays([kind_arr, inputs.src, inputs.dst], prefix="edge")
    full_hash = hash64_from_arrays(
        [kind_arr, inputs.src, inputs.dst, inputs.path, inputs.bstart, inputs.bend],
        prefix="edge",
    )
    has_span = pc.and_(
        pc.is_valid(inputs.path),
        pc.and_(pc.is_valid(inputs.bstart), pc.is_valid(inputs.bend)),
    )
    chosen = pc.if_else(has_span, full_hash, base_hash)
    chosen_str = pc.cast(chosen, pa.string())
    edge_id = pc.binary_join_element_wise(pa.scalar("edge"), chosen_str, ":")
    valid = pc.and_(pc.is_valid(inputs.src), pc.is_valid(inputs.dst))
    return pc.if_else(valid, edge_id, pa.scalar(None, type=pa.string()))


def _emit_edges_from_relation(
    rel: pa.Table | None,
    *,
    spec: EdgeEmitSpec,
) -> pa.Table:
    if rel is None or rel.num_rows == 0:
        return empty_edges()

    n = rel.num_rows
    src = _get(rel, spec.src_col, default_type=pa.string())
    dst = _get(rel, spec.dst_col, default_type=pa.string())
    path = _get(rel, spec.path_col, default_type=pa.string())
    bstart = _get(rel, spec.bstart_col, default_type=pa.int64())
    bend = _get(rel, spec.bend_col, default_type=pa.int64())
    edge_ids = _edge_id_array(
        edge_kind=spec.edge_kind.value,
        inputs=EdgeIdArrays(src=src, dst=dst, path=path, bstart=bstart, bend=bend),
    )

    default_score = 1.0 if spec.origin == "scip" else 0.5
    origin = _resolve_string_col(rel, "origin", default_value=spec.origin)
    origin = pc.fill_null(origin, replacement=spec.origin)

    out = pa.Table.from_arrays(
        [
            _const_i32(n, SCHEMA_VERSION),
            edge_ids,
            _const_str(n, spec.edge_kind.value),
            src,
            dst,
            path,
            bstart,
            bend,
            origin,
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
    mask = pc.fill_null(pc.is_valid(edge_ids), replacement=False)
    out = out.filter(mask)
    if out.num_rows == 0:
        return empty_edges()
    return out


def _symbol_role_edges(rel_name_symbol: pa.Table | None) -> list[pa.Table]:
    if rel_name_symbol is None or rel_name_symbol.num_rows == 0:
        return []

    roles = _get(rel_name_symbol, "symbol_roles", default_type=pa.int32())
    parts: list[pa.Table] = []

    for edge_kind, role_mask, must_set in (
        (EdgeKind.PY_DEFINES_SYMBOL, SCIP_ROLE_DEFINITION, True),
        (EdgeKind.PY_REFERENCES_SYMBOL, SCIP_ROLE_DEFINITION, False),
        (EdgeKind.PY_READS_SYMBOL, SCIP_ROLE_READ, True),
        (EdgeKind.PY_WRITES_SYMBOL, SCIP_ROLE_WRITE, True),
    ):
        mask = _role_mask(roles, role_mask, must_set=must_set)
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


def _scip_symbol_relationship_edges(scip_symbol_relationships: pa.Table | None) -> list[pa.Table]:
    if scip_symbol_relationships is None or scip_symbol_relationships.num_rows == 0:
        return []
    if "symbol" not in scip_symbol_relationships.column_names:
        return []
    if "related_symbol" not in scip_symbol_relationships.column_names:
        return []

    parts: list[pa.Table] = []
    for edge_kind, flag_col, resolution_method in (
        (EdgeKind.SCIP_SYMBOL_REFERENCE, "is_reference", "SCIP_SYMBOL_REFERENCE"),
        (EdgeKind.SCIP_SYMBOL_IMPLEMENTATION, "is_implementation", "SCIP_SYMBOL_IMPLEMENTATION"),
        (EdgeKind.SCIP_SYMBOL_TYPE_DEFINITION, "is_type_definition", "SCIP_SYMBOL_TYPE_DEFINITION"),
        (EdgeKind.SCIP_SYMBOL_DEFINITION, "is_definition", "SCIP_SYMBOL_DEFINITION"),
    ):
        if flag_col not in scip_symbol_relationships.column_names:
            continue
        mask = pc.fill_null(
            pc.cast(scip_symbol_relationships[flag_col], pa.bool_()), replacement=False
        )
        rel = scip_symbol_relationships.filter(mask)
        if rel.num_rows == 0:
            continue
        parts.append(
            _emit_edges_from_relation(
                rel,
                spec=EdgeEmitSpec(
                    edge_kind=edge_kind,
                    src_col="symbol",
                    dst_col="related_symbol",
                    path_col="path",
                    bstart_col="bstart",
                    bend_col="bend",
                    origin="scip",
                    default_resolution_method=resolution_method,
                ),
            )
        )
    return parts


def _import_symbol_edges(rel_import_symbol: pa.Table | None) -> list[pa.Table]:
    if rel_import_symbol is None or rel_import_symbol.num_rows == 0:
        return []

    roles = _get(rel_import_symbol, "symbol_roles", default_type=pa.int32())
    mask = _role_mask(roles, SCIP_ROLE_IMPORT, must_set=True)
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

    qnp = _filter_unresolved_qname_calls(rel_callsite_qname, rel_callsite_symbol)
    if qnp.num_rows == 0:
        return []

    qnp = _ensure_ambiguity_group_id(qnp)
    bstart_col, bend_col = _callsite_span_columns(qnp)
    return [
        _emit_edges_from_relation(
            qnp,
            spec=EdgeEmitSpec(
                edge_kind=EdgeKind.PY_CALLS_QNAME,
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


def _diagnostic_edges(
    diagnostics_norm: pa.Table | None,
    repo_files: pa.Table | None,
) -> list[pa.Table]:
    if diagnostics_norm is None or diagnostics_norm.num_rows == 0:
        return []
    diag = _with_repo_file_ids(diagnostics_norm, repo_files)
    if "diag_id" not in diag.column_names:
        return []

    n = diag.num_rows
    severity = _get(diag, "severity", default_type=pa.string())
    score = _severity_score_array(severity)
    origin = _get(diag, "diag_source", default_type=pa.string())
    origin = pc.coalesce(origin, pa.scalar("diagnostic"))

    diag = _set_or_append_column(diag, "origin", origin)
    diag = _set_or_append_column(diag, "confidence", score)
    diag = _set_or_append_column(diag, "score", score)
    diag = _set_or_append_column(diag, "resolution_method", _const_str(n, "DIAGNOSTIC"))

    return [
        _emit_edges_from_relation(
            diag,
            spec=EdgeEmitSpec(
                edge_kind=EdgeKind.HAS_DIAGNOSTIC,
                src_col="file_id",
                dst_col="diag_id",
                path_col="path",
                bstart_col="bstart",
                bend_col="bend",
                origin="diagnostic",
                default_resolution_method="DIAGNOSTIC",
            ),
        )
    ]


def _type_annotation_edges(type_exprs_norm: pa.Table | None) -> list[pa.Table]:
    if type_exprs_norm is None or type_exprs_norm.num_rows == 0:
        return []
    n = type_exprs_norm.num_rows
    rel = _set_or_append_column(type_exprs_norm, "confidence", _const_f32(n, 1.0))
    rel = _set_or_append_column(rel, "score", _const_f32(n, 1.0))
    return [
        _emit_edges_from_relation(
            rel,
            spec=EdgeEmitSpec(
                edge_kind=EdgeKind.HAS_ANNOTATION,
                src_col="owner_def_id",
                dst_col="type_expr_id",
                path_col="path",
                bstart_col="bstart",
                bend_col="bend",
                origin="annotation",
                default_resolution_method="TYPE_ANNOTATION",
            ),
        )
    ]


def _inferred_type_edges(type_exprs_norm: pa.Table | None) -> list[pa.Table]:
    if type_exprs_norm is None or type_exprs_norm.num_rows == 0:
        return []

    n = type_exprs_norm.num_rows
    rel = pa.Table.from_arrays(
        [
            _get(type_exprs_norm, "owner_def_id", default_type=pa.string()),
            _get(type_exprs_norm, "type_id", default_type=pa.string()),
            pa.nulls(n, type=pa.string()),
            pa.nulls(n, type=pa.int64()),
            pa.nulls(n, type=pa.int64()),
            _const_f32(n, 1.0),
            _const_f32(n, 1.0),
        ],
        names=["owner_def_id", "type_id", "path", "bstart", "bend", "confidence", "score"],
    )
    return [
        _emit_edges_from_relation(
            rel,
            spec=EdgeEmitSpec(
                edge_kind=EdgeKind.INFERRED_TYPE,
                src_col="owner_def_id",
                dst_col="type_id",
                path_col="path",
                bstart_col="bstart",
                bend_col="bend",
                origin="inferred",
                default_resolution_method="ANNOTATION_INFER",
            ),
        )
    ]


def _runtime_edges(
    rt_signatures: pa.Table | None,
    rt_signature_params: pa.Table | None,
    rt_members: pa.Table | None,
) -> list[pa.Table]:
    parts: list[pa.Table] = []
    signature_edges = _runtime_edge_pairs(
        rt_signatures,
        src_col="rt_id",
        dst_col="sig_id",
        edge_kind=EdgeKind.RT_HAS_SIGNATURE,
    )
    if signature_edges is not None:
        parts.append(signature_edges)
    param_edges = _runtime_edge_pairs(
        rt_signature_params,
        src_col="sig_id",
        dst_col="param_id",
        edge_kind=EdgeKind.RT_HAS_PARAM,
    )
    if param_edges is not None:
        parts.append(param_edges)
    member_edges = _runtime_edge_pairs(
        rt_members,
        src_col="rt_id",
        dst_col="member_id",
        edge_kind=EdgeKind.RT_HAS_MEMBER,
    )
    if member_edges is not None:
        parts.append(member_edges)
    return parts


def _runtime_edge_pairs(
    table: pa.Table | None,
    *,
    src_col: str,
    dst_col: str,
    edge_kind: EdgeKind,
) -> pa.Table | None:
    if table is None or table.num_rows == 0:
        return None
    n = table.num_rows
    rel = pa.Table.from_arrays(
        [
            _get(table, src_col, default_type=pa.string()),
            _get(table, dst_col, default_type=pa.string()),
            pa.nulls(n, type=pa.string()),
            pa.nulls(n, type=pa.int64()),
            pa.nulls(n, type=pa.int64()),
            _const_f32(n, 1.0),
            _const_f32(n, 1.0),
        ],
        names=[src_col, dst_col, "path", "bstart", "bend", "confidence", "score"],
    )
    return _emit_edges_from_relation(
        rel,
        spec=EdgeEmitSpec(
            edge_kind=edge_kind,
            src_col=src_col,
            dst_col=dst_col,
            path_col="path",
            bstart_col="bstart",
            bend_col="bend",
            origin="inspect",
            default_resolution_method="RUNTIME_INSPECT",
        ),
    )


@dataclass(frozen=True)
class EdgeBuildOptions:
    """Configure which edge families are emitted."""

    emit_symbol_role_edges: bool = True
    emit_scip_symbol_relationship_edges: bool = True
    emit_import_edges: bool = True
    emit_call_edges: bool = True
    emit_qname_fallback_call_edges: bool = True
    emit_diagnostic_edges: bool = True
    emit_type_edges: bool = True
    emit_runtime_edges: bool = True


@dataclass(frozen=True)
class EdgeBuildInputs:
    """Input tables for edge construction."""

    relationship_outputs: Mapping[str, pa.Table] | None = None
    scip_symbol_relationships: pa.Table | None = None
    diagnostics_norm: pa.Table | None = None
    repo_files: pa.Table | None = None
    type_exprs_norm: pa.Table | None = None
    rt_signatures: pa.Table | None = None
    rt_signature_params: pa.Table | None = None
    rt_members: pa.Table | None = None


def _edge_inputs_from_legacy(legacy: Mapping[str, object]) -> EdgeBuildInputs:
    relationship_outputs = legacy.get("relationship_outputs")
    scip_symbol_relationships = legacy.get("scip_symbol_relationships")
    diagnostics_norm = legacy.get("diagnostics_norm")
    repo_files = legacy.get("repo_files")
    type_exprs_norm = legacy.get("type_exprs_norm")
    rt_signatures = legacy.get("rt_signatures")
    rt_signature_params = legacy.get("rt_signature_params")
    rt_members = legacy.get("rt_members")
    return EdgeBuildInputs(
        relationship_outputs=relationship_outputs
        if isinstance(relationship_outputs, Mapping)
        else None,
        scip_symbol_relationships=scip_symbol_relationships
        if isinstance(scip_symbol_relationships, pa.Table)
        else None,
        diagnostics_norm=diagnostics_norm if isinstance(diagnostics_norm, pa.Table) else None,
        repo_files=repo_files if isinstance(repo_files, pa.Table) else None,
        type_exprs_norm=type_exprs_norm if isinstance(type_exprs_norm, pa.Table) else None,
        rt_signatures=rt_signatures if isinstance(rt_signatures, pa.Table) else None,
        rt_signature_params=rt_signature_params
        if isinstance(rt_signature_params, pa.Table)
        else None,
        rt_members=rt_members if isinstance(rt_members, pa.Table) else None,
    )


def _collect_edge_parts(
    *,
    tables: EdgeBuildInputs,
    options: EdgeBuildOptions,
) -> list[pa.Table]:
    outputs = tables.relationship_outputs or {}
    rel_name_symbol = outputs.get("rel_name_symbol")
    rel_import_symbol = outputs.get("rel_import_symbol")
    rel_callsite_symbol = outputs.get("rel_callsite_symbol")
    rel_callsite_qname = outputs.get("rel_callsite_qname")

    def emit_type_edges() -> list[pa.Table]:
        return [
            *_type_annotation_edges(tables.type_exprs_norm),
            *_inferred_type_edges(tables.type_exprs_norm),
        ]

    emitters: list[tuple[bool, Callable[[], list[pa.Table]]]] = [
        (options.emit_symbol_role_edges, lambda: _symbol_role_edges(rel_name_symbol)),
        (
            options.emit_scip_symbol_relationship_edges,
            lambda: _scip_symbol_relationship_edges(tables.scip_symbol_relationships),
        ),
        (options.emit_import_edges, lambda: _import_symbol_edges(rel_import_symbol)),
        (options.emit_call_edges, lambda: _call_symbol_edges(rel_callsite_symbol)),
        (
            options.emit_qname_fallback_call_edges,
            lambda: _qname_fallback_edges(rel_callsite_qname, rel_callsite_symbol),
        ),
        (
            options.emit_diagnostic_edges,
            lambda: _diagnostic_edges(tables.diagnostics_norm, tables.repo_files),
        ),
        (options.emit_type_edges, emit_type_edges),
        (
            options.emit_runtime_edges,
            lambda: _runtime_edges(
                tables.rt_signatures,
                tables.rt_signature_params,
                tables.rt_members,
            ),
        ),
    ]

    parts: list[pa.Table] = []
    for enabled, emitter in emitters:
        if enabled:
            parts.extend(emitter())
    return parts


def build_cpg_edges_raw(
    *,
    inputs: EdgeBuildInputs | None = None,
    options: EdgeBuildOptions | None = None,
    **legacy: object,
) -> pa.Table:
    """Emit raw CPG edges without finalization.

    Returns
    -------
    pyarrow.Table
        Raw edges table.
    """
    options = options or EdgeBuildOptions()
    if inputs is None and legacy:
        inputs = _edge_inputs_from_legacy(legacy)
    tables = inputs or EdgeBuildInputs()
    parts = _collect_edge_parts(tables=tables, options=options)

    if not parts:
        return empty_edges()

    return pa.concat_tables(parts, promote=True)


def build_cpg_edges(
    *,
    ctx: ExecutionContext,
    inputs: EdgeBuildInputs | None = None,
    options: EdgeBuildOptions | None = None,
) -> FinalizeResult:
    """Build and finalize CPG edges.

    Returns
    -------
    FinalizeResult
        Finalized edges tables and stats.
    """
    raw = build_cpg_edges_raw(inputs=inputs, options=options)
    return finalize(raw, contract=CPG_EDGES_CONTRACT, ctx=ctx)
