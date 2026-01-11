"""Build CPG edge tables from relationship outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.compute import pc
from arrowdsl.finalize import FinalizeResult, finalize
from arrowdsl.runtime import ExecutionContext
from cpg.builders import EdgeBuilder
from cpg.kinds import (
    SCIP_ROLE_DEFINITION,
    SCIP_ROLE_IMPORT,
    SCIP_ROLE_READ,
    SCIP_ROLE_WRITE,
    EdgeKind,
)
from cpg.schemas import CPG_EDGES_CONTRACT, CPG_EDGES_SCHEMA, SCHEMA_VERSION, empty_edges
from cpg.specs import EdgeEmitSpec, EdgePlanSpec, TableFilter, TableGetter


def _const_str(n: int, value: str) -> pa.Array:
    return pa.array([value] * n, type=pa.string())


def _const_f32(n: int, value: float) -> pa.Array:
    return pa.array([float(value)] * n, type=pa.float32())


def _get(table: pa.Table, col: str, *, default_type: pa.DataType) -> pa.Array:
    if col in table.column_names:
        return table[col]
    return pa.nulls(table.num_rows, type=default_type)


def _set_or_append_column(
    table: pa.Table,
    name: str,
    values: pa.Array | pa.ChunkedArray,
) -> pa.Table:
    if name in table.column_names:
        idx = table.schema.get_field_index(name)
        return table.set_column(idx, name, values)
    return table.append_column(name, values)


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


def _table_getter(name: str) -> TableGetter:
    def _get_table(tables: Mapping[str, pa.Table]) -> pa.Table | None:
        return tables.get(name)

    return _get_table


def _symbol_role_filter(mask: int, *, must_set: bool) -> TableFilter:
    def _filter(table: pa.Table) -> pa.Array:
        roles = _get(table, "symbol_roles", default_type=pa.int64())
        return _role_mask(roles, mask, must_set=must_set)

    return _filter


def _flag_filter(flag_col: str) -> TableFilter:
    def _filter(table: pa.Table) -> pa.Array:
        if flag_col not in table.column_names:
            return pa.array([False] * table.num_rows, type=pa.bool_())
        return pc.fill_null(pc.cast(table[flag_col], pa.bool_()), replacement=False)

    return _filter


def _qname_fallback_relation(tables: Mapping[str, pa.Table]) -> pa.Table | None:
    rel_callsite_qname = tables.get("rel_callsite_qname")
    if rel_callsite_qname is None or rel_callsite_qname.num_rows == 0:
        return None
    rel_callsite_symbol = tables.get("rel_callsite_symbol")
    filtered = _filter_unresolved_qname_calls(rel_callsite_qname, rel_callsite_symbol)
    if filtered.num_rows == 0:
        return None
    return _ensure_ambiguity_group_id(filtered)


def _diagnostic_relation(tables: Mapping[str, pa.Table]) -> pa.Table | None:
    diagnostics_norm = tables.get("diagnostics_norm")
    if diagnostics_norm is None or diagnostics_norm.num_rows == 0:
        return None
    diag = _with_repo_file_ids(diagnostics_norm, tables.get("repo_files"))
    if "diag_id" not in diag.column_names:
        return None
    n = diag.num_rows
    severity = _get(diag, "severity", default_type=pa.string())
    score = _severity_score_array(severity)
    origin = _get(diag, "diag_source", default_type=pa.string())
    origin = pc.coalesce(origin, pa.scalar("diagnostic"))
    diag = _set_or_append_column(diag, "origin", origin)
    diag = _set_or_append_column(diag, "confidence", score)
    diag = _set_or_append_column(diag, "score", score)
    return _set_or_append_column(diag, "resolution_method", _const_str(n, "DIAGNOSTIC"))


def _type_annotation_relation(tables: Mapping[str, pa.Table]) -> pa.Table | None:
    type_exprs_norm = tables.get("type_exprs_norm")
    if type_exprs_norm is None or type_exprs_norm.num_rows == 0:
        return None
    n = type_exprs_norm.num_rows
    rel = _set_or_append_column(type_exprs_norm, "confidence", _const_f32(n, 1.0))
    return _set_or_append_column(rel, "score", _const_f32(n, 1.0))


def _inferred_type_relation(tables: Mapping[str, pa.Table]) -> pa.Table | None:
    type_exprs_norm = tables.get("type_exprs_norm")
    if type_exprs_norm is None or type_exprs_norm.num_rows == 0:
        return None
    n = type_exprs_norm.num_rows
    return pa.Table.from_arrays(
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


def _runtime_relation(
    table: pa.Table | None,
    *,
    src_col: str,
    dst_col: str,
) -> pa.Table | None:
    if table is None or table.num_rows == 0:
        return None
    n = table.num_rows
    return pa.Table.from_arrays(
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


def _runtime_relation_getter(key: str, *, src_col: str, dst_col: str) -> TableGetter:
    def _get_rel(tables: Mapping[str, pa.Table]) -> pa.Table | None:
        table = tables.get(key)
        return _runtime_relation(table, src_col=src_col, dst_col=dst_col)

    return _get_rel


EDGE_PLAN_SPECS: tuple[EdgePlanSpec, ...] = (
    EdgePlanSpec(
        name="symbol_role_defines",
        option_flag="emit_symbol_role_edges",
        relation_getter=_table_getter("rel_name_symbol"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_DEFINES_SYMBOL,
            src_cols=("name_ref_id",),
            dst_cols=("symbol",),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
        filter_fn=_symbol_role_filter(SCIP_ROLE_DEFINITION, must_set=True),
    ),
    EdgePlanSpec(
        name="symbol_role_references",
        option_flag="emit_symbol_role_edges",
        relation_getter=_table_getter("rel_name_symbol"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_REFERENCES_SYMBOL,
            src_cols=("name_ref_id",),
            dst_cols=("symbol",),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
        filter_fn=_symbol_role_filter(SCIP_ROLE_DEFINITION, must_set=False),
    ),
    EdgePlanSpec(
        name="symbol_role_reads",
        option_flag="emit_symbol_role_edges",
        relation_getter=_table_getter("rel_name_symbol"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_READS_SYMBOL,
            src_cols=("name_ref_id",),
            dst_cols=("symbol",),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
        filter_fn=_symbol_role_filter(SCIP_ROLE_READ, must_set=True),
    ),
    EdgePlanSpec(
        name="symbol_role_writes",
        option_flag="emit_symbol_role_edges",
        relation_getter=_table_getter("rel_name_symbol"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_WRITES_SYMBOL,
            src_cols=("name_ref_id",),
            dst_cols=("symbol",),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
        filter_fn=_symbol_role_filter(SCIP_ROLE_WRITE, must_set=True),
    ),
    EdgePlanSpec(
        name="scip_symbol_reference",
        option_flag="emit_scip_symbol_relationship_edges",
        relation_getter=_table_getter("scip_symbol_relationships"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.SCIP_SYMBOL_REFERENCE,
            src_cols=("symbol",),
            dst_cols=("related_symbol",),
            origin="scip",
            default_resolution_method="SCIP_SYMBOL_REFERENCE",
        ),
        filter_fn=_flag_filter("is_reference"),
    ),
    EdgePlanSpec(
        name="scip_symbol_implementation",
        option_flag="emit_scip_symbol_relationship_edges",
        relation_getter=_table_getter("scip_symbol_relationships"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.SCIP_SYMBOL_IMPLEMENTATION,
            src_cols=("symbol",),
            dst_cols=("related_symbol",),
            origin="scip",
            default_resolution_method="SCIP_SYMBOL_IMPLEMENTATION",
        ),
        filter_fn=_flag_filter("is_implementation"),
    ),
    EdgePlanSpec(
        name="scip_symbol_type_definition",
        option_flag="emit_scip_symbol_relationship_edges",
        relation_getter=_table_getter("scip_symbol_relationships"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.SCIP_SYMBOL_TYPE_DEFINITION,
            src_cols=("symbol",),
            dst_cols=("related_symbol",),
            origin="scip",
            default_resolution_method="SCIP_SYMBOL_TYPE_DEFINITION",
        ),
        filter_fn=_flag_filter("is_type_definition"),
    ),
    EdgePlanSpec(
        name="scip_symbol_definition",
        option_flag="emit_scip_symbol_relationship_edges",
        relation_getter=_table_getter("scip_symbol_relationships"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.SCIP_SYMBOL_DEFINITION,
            src_cols=("symbol",),
            dst_cols=("related_symbol",),
            origin="scip",
            default_resolution_method="SCIP_SYMBOL_DEFINITION",
        ),
        filter_fn=_flag_filter("is_definition"),
    ),
    EdgePlanSpec(
        name="import_edges",
        option_flag="emit_import_edges",
        relation_getter=_table_getter("rel_import_symbol"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_IMPORTS_SYMBOL,
            src_cols=("import_alias_id", "import_id"),
            dst_cols=("symbol",),
            path_cols=("path",),
            bstart_cols=("bstart", "alias_bstart"),
            bend_cols=("bend", "alias_bend"),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
        filter_fn=_symbol_role_filter(SCIP_ROLE_IMPORT, must_set=True),
    ),
    EdgePlanSpec(
        name="call_edges",
        option_flag="emit_call_edges",
        relation_getter=_table_getter("rel_callsite_symbol"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_CALLS_SYMBOL,
            src_cols=("call_id",),
            dst_cols=("symbol",),
            path_cols=("path",),
            bstart_cols=("call_bstart", "bstart"),
            bend_cols=("call_bend", "bend"),
            origin="scip",
            default_resolution_method="CALLEE_SPAN_EXACT",
        ),
    ),
    EdgePlanSpec(
        name="qname_fallback_calls",
        option_flag="emit_qname_fallback_call_edges",
        relation_getter=_qname_fallback_relation,
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_CALLS_QNAME,
            src_cols=("call_id",),
            dst_cols=("qname_id",),
            path_cols=("path",),
            bstart_cols=("call_bstart", "bstart"),
            bend_cols=("call_bend", "bend"),
            origin="qnp",
            default_resolution_method="QNP_CALLEE_FALLBACK",
        ),
    ),
    EdgePlanSpec(
        name="diagnostic_edges",
        option_flag="emit_diagnostic_edges",
        relation_getter=_diagnostic_relation,
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.HAS_DIAGNOSTIC,
            src_cols=("file_id",),
            dst_cols=("diag_id",),
            origin="diagnostic",
            default_resolution_method="DIAGNOSTIC",
        ),
    ),
    EdgePlanSpec(
        name="type_annotation_edges",
        option_flag="emit_type_edges",
        relation_getter=_type_annotation_relation,
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.HAS_ANNOTATION,
            src_cols=("owner_def_id",),
            dst_cols=("type_expr_id",),
            origin="annotation",
            default_resolution_method="TYPE_ANNOTATION",
        ),
    ),
    EdgePlanSpec(
        name="inferred_type_edges",
        option_flag="emit_type_edges",
        relation_getter=_inferred_type_relation,
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.INFERRED_TYPE,
            src_cols=("owner_def_id",),
            dst_cols=("type_id",),
            origin="inferred",
            default_resolution_method="ANNOTATION_INFER",
        ),
    ),
    EdgePlanSpec(
        name="runtime_signatures",
        option_flag="emit_runtime_edges",
        relation_getter=_runtime_relation_getter(
            "rt_signatures", src_col="rt_id", dst_col="sig_id"
        ),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.RT_HAS_SIGNATURE,
            src_cols=("rt_id",),
            dst_cols=("sig_id",),
            origin="inspect",
            default_resolution_method="RUNTIME_INSPECT",
        ),
    ),
    EdgePlanSpec(
        name="runtime_params",
        option_flag="emit_runtime_edges",
        relation_getter=_runtime_relation_getter(
            "rt_signature_params", src_col="sig_id", dst_col="param_id"
        ),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.RT_HAS_PARAM,
            src_cols=("sig_id",),
            dst_cols=("param_id",),
            origin="inspect",
            default_resolution_method="RUNTIME_INSPECT",
        ),
    ),
    EdgePlanSpec(
        name="runtime_members",
        option_flag="emit_runtime_edges",
        relation_getter=_runtime_relation_getter(
            "rt_members", src_col="rt_id", dst_col="member_id"
        ),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.RT_HAS_MEMBER,
            src_cols=("rt_id",),
            dst_cols=("member_id",),
            origin="inspect",
            default_resolution_method="RUNTIME_INSPECT",
        ),
    ),
)

EDGE_BUILDER = EdgeBuilder(
    emitters=EDGE_PLAN_SPECS,
    schema_version=SCHEMA_VERSION,
    edge_schema=CPG_EDGES_SCHEMA,
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


def _edge_tables(inputs: EdgeBuildInputs) -> dict[str, pa.Table]:
    tables: dict[str, pa.Table] = {}
    if inputs.relationship_outputs:
        tables.update(
            {
                name: table
                for name, table in inputs.relationship_outputs.items()
                if isinstance(table, pa.Table)
            }
        )
    tables.update(
        {
            name: table
            for name, table in {
                "scip_symbol_relationships": inputs.scip_symbol_relationships,
                "diagnostics_norm": inputs.diagnostics_norm,
                "repo_files": inputs.repo_files,
                "type_exprs_norm": inputs.type_exprs_norm,
                "rt_signatures": inputs.rt_signatures,
                "rt_signature_params": inputs.rt_signature_params,
                "rt_members": inputs.rt_members,
            }.items()
            if table is not None
        }
    )
    return tables


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
    tables = _edge_tables(inputs or EdgeBuildInputs())
    parts = EDGE_BUILDER.build(tables=tables, options=options)
    parts = [part for part in parts if part.num_rows]

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
