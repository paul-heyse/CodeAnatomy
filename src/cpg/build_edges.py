"""Build CPG edge tables from relationship outputs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

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


def _row_str(row: Mapping[str, object], key: str) -> str | None:
    value = row.get(key)
    return value if isinstance(value, str) else None


def _row_int(row: Mapping[str, object], key: str) -> int | None:
    value = row.get(key)
    if isinstance(value, bool):
        return None
    return value if isinstance(value, int) else None


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


@dataclass(frozen=True)
class EdgeRow:
    """Single edge row payload."""

    src: str | None
    dst: str | None
    path: str | None
    bstart: int | None
    bend: int | None
    origin: str
    resolution_method: str
    confidence: float
    score: float


@dataclass(frozen=True)
class EdgeListColumns:
    """Edge table columns assembled from lists."""

    src: list[str | None]
    dst: list[str | None]
    path: list[str | None]
    bstart: list[int | None]
    bend: list[int | None]
    origin: list[str]
    resolution_method: list[str]
    confidence: list[float]
    score: list[float]


@dataclass
class EdgeListBuilder:
    """Collect edge rows into list-based columns."""

    src: list[str | None]
    dst: list[str | None]
    path: list[str | None]
    bstart: list[int | None]
    bend: list[int | None]
    origin: list[str]
    resolution_method: list[str]
    confidence: list[float]
    score: list[float]

    @classmethod
    def empty(cls) -> EdgeListBuilder:
        """Create an empty edge list builder.

        Returns
        -------
        EdgeListBuilder
            Empty edge list builder instance.
        """
        return cls(
            src=[],
            dst=[],
            path=[],
            bstart=[],
            bend=[],
            origin=[],
            resolution_method=[],
            confidence=[],
            score=[],
        )

    def append(self, row: EdgeRow) -> None:
        """Append a row of edge data."""
        self.src.append(row.src)
        self.dst.append(row.dst)
        self.path.append(row.path)
        self.bstart.append(row.bstart)
        self.bend.append(row.bend)
        self.origin.append(row.origin)
        self.resolution_method.append(row.resolution_method)
        self.confidence.append(row.confidence)
        self.score.append(row.score)

    def to_columns(self) -> EdgeListColumns:
        """Return collected rows as column lists.

        Returns
        -------
        EdgeListColumns
            Column lists for edge construction.
        """
        return EdgeListColumns(
            src=self.src,
            dst=self.dst,
            path=self.path,
            bstart=self.bstart,
            bend=self.bend,
            origin=self.origin,
            resolution_method=self.resolution_method,
            confidence=self.confidence,
            score=self.score,
        )


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


def _file_id_lookup(repo_files: pa.Table | None) -> dict[str, str]:
    file_id_by_path: dict[str, str] = {}
    if repo_files is None or repo_files.num_rows == 0:
        return file_id_by_path
    for row in repo_files.to_pylist():
        path = row.get("path")
        file_id = row.get("file_id")
        if isinstance(path, str) and isinstance(file_id, str):
            file_id_by_path[path] = file_id
    return file_id_by_path


def _severity_score(severity: str) -> float:
    if severity == "ERROR":
        return 1.0
    if severity == "WARNING":
        return 0.7
    return 0.5


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
    mask = pc.fill_null(pc.is_valid(edge_ids), replacement=False)
    if not pc.any(mask).as_py():
        return empty_edges()
    return out.filter(mask)


def _edge_table_from_columns(
    *,
    edge_kind: EdgeKind,
    columns: EdgeListColumns,
) -> pa.Table:
    n = len(columns.src)
    if n == 0:
        return empty_edges()

    src = pa.array(columns.src, type=pa.string())
    dst = pa.array(columns.dst, type=pa.string())
    path = pa.array(columns.path, type=pa.string())
    bstart = pa.array(columns.bstart, type=pa.int64())
    bend = pa.array(columns.bend, type=pa.int64())
    edge_ids = _edge_id_array(
        edge_kind=edge_kind.value,
        inputs=EdgeIdArrays(src=src, dst=dst, path=path, bstart=bstart, bend=bend),
    )
    out = pa.Table.from_arrays(
        [
            _const_i32(n, SCHEMA_VERSION),
            edge_ids,
            _const_str(n, edge_kind.value),
            src,
            dst,
            path,
            bstart,
            bend,
            pa.array(columns.origin, type=pa.string()),
            pa.array(columns.resolution_method, type=pa.string()),
            pa.array(columns.confidence, type=pa.float32()),
            pa.array(columns.score, type=pa.float32()),
            pa.array([None] * n, type=pa.int32()),
            pa.array([None] * n, type=pa.string()),
            pa.array([None] * n, type=pa.string()),
            pa.array([None] * n, type=pa.string()),
            pa.array([None] * n, type=pa.int32()),
        ],
        schema=CPG_EDGES_SCHEMA,
    )
    mask = pc.fill_null(pc.is_valid(edge_ids), replacement=False)
    if not pc.any(mask).as_py():
        return empty_edges()
    return out.filter(mask)


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

    file_id_by_path = _file_id_lookup(repo_files)
    builder = EdgeListBuilder.empty()

    for row in diagnostics_norm.to_pylist():
        diag_id = row.get("diag_id")
        if not isinstance(diag_id, str):
            continue
        path = _row_str(row, "path")
        file_id = row.get("file_id")
        if not isinstance(file_id, str) and path is not None:
            file_id = file_id_by_path.get(path)
        if not isinstance(file_id, str):
            continue
        severity = row.get("severity")
        severity_str = severity if isinstance(severity, str) else "ERROR"
        score = _severity_score(severity_str)
        builder.append(
            EdgeRow(
                src=file_id,
                dst=diag_id,
                path=path,
                bstart=_row_int(row, "bstart"),
                bend=_row_int(row, "bend"),
                origin=_row_str(row, "diag_source") or "diagnostic",
                resolution_method="DIAGNOSTIC",
                confidence=score,
                score=score,
            )
        )

    if not builder.src:
        return []
    return [
        _edge_table_from_columns(
            edge_kind=EdgeKind.HAS_DIAGNOSTIC,
            columns=builder.to_columns(),
        )
    ]


def _type_annotation_edges(type_exprs_norm: pa.Table | None) -> list[pa.Table]:
    if type_exprs_norm is None or type_exprs_norm.num_rows == 0:
        return []

    builder = EdgeListBuilder.empty()
    for row in type_exprs_norm.to_pylist():
        owner_id = row.get("owner_def_id")
        type_expr_id = row.get("type_expr_id")
        if not isinstance(owner_id, str) or not isinstance(type_expr_id, str):
            continue
        builder.append(
            EdgeRow(
                src=owner_id,
                dst=type_expr_id,
                path=_row_str(row, "path"),
                bstart=_row_int(row, "bstart"),
                bend=_row_int(row, "bend"),
                origin="annotation",
                resolution_method="TYPE_ANNOTATION",
                confidence=1.0,
                score=1.0,
            )
        )

    if not builder.src:
        return []
    return [
        _edge_table_from_columns(
            edge_kind=EdgeKind.HAS_ANNOTATION,
            columns=builder.to_columns(),
        )
    ]


def _inferred_type_edges(type_exprs_norm: pa.Table | None) -> list[pa.Table]:
    if type_exprs_norm is None or type_exprs_norm.num_rows == 0:
        return []

    builder = EdgeListBuilder.empty()
    for row in type_exprs_norm.to_pylist():
        owner_id = row.get("owner_def_id")
        type_id = row.get("type_id")
        if not isinstance(owner_id, str) or not isinstance(type_id, str):
            continue
        builder.append(
            EdgeRow(
                src=owner_id,
                dst=type_id,
                path=None,
                bstart=None,
                bend=None,
                origin="inferred",
                resolution_method="ANNOTATION_INFER",
                confidence=1.0,
                score=1.0,
            )
        )

    if not builder.src:
        return []
    return [
        _edge_table_from_columns(
            edge_kind=EdgeKind.INFERRED_TYPE,
            columns=builder.to_columns(),
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
    builder = EdgeListBuilder.empty()
    for row in table.to_pylist():
        src_id = row.get(src_col)
        dst_id = row.get(dst_col)
        if not isinstance(src_id, str) or not isinstance(dst_id, str):
            continue
        builder.append(
            EdgeRow(
                src=src_id,
                dst=dst_id,
                path=None,
                bstart=None,
                bend=None,
                origin="inspect",
                resolution_method="RUNTIME_INSPECT",
                confidence=1.0,
                score=1.0,
            )
        )
    if not builder.src:
        return None
    return _edge_table_from_columns(edge_kind=edge_kind, columns=builder.to_columns())


@dataclass(frozen=True)
class EdgeBuildOptions:
    """Configure which edge families are emitted."""

    emit_symbol_role_edges: bool = True
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
    diagnostics_norm: pa.Table | None = None
    repo_files: pa.Table | None = None
    type_exprs_norm: pa.Table | None = None
    rt_signatures: pa.Table | None = None
    rt_signature_params: pa.Table | None = None
    rt_members: pa.Table | None = None


def _edge_inputs_from_legacy(legacy: Mapping[str, object]) -> EdgeBuildInputs:
    relationship_outputs = legacy.get("relationship_outputs")
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
        diagnostics_norm=diagnostics_norm if isinstance(diagnostics_norm, pa.Table) else None,
        repo_files=repo_files if isinstance(repo_files, pa.Table) else None,
        type_exprs_norm=type_exprs_norm if isinstance(type_exprs_norm, pa.Table) else None,
        rt_signatures=rt_signatures if isinstance(rt_signatures, pa.Table) else None,
        rt_signature_params=rt_signature_params
        if isinstance(rt_signature_params, pa.Table)
        else None,
        rt_members=rt_members if isinstance(rt_members, pa.Table) else None,
    )


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
    outputs = tables.relationship_outputs or {}
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
    if options.emit_diagnostic_edges:
        parts.extend(_diagnostic_edges(tables.diagnostics_norm, tables.repo_files))
    if options.emit_type_edges:
        parts.extend(_type_annotation_edges(tables.type_exprs_norm))
        parts.extend(_inferred_type_edges(tables.type_exprs_norm))
    if options.emit_runtime_edges:
        parts.extend(
            _runtime_edges(tables.rt_signatures, tables.rt_signature_params, tables.rt_members)
        )

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
