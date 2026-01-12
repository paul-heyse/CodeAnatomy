"""Edge relation registry and relation builders for CPG edges."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.compute.kernels import coalesce_arrays, drop_nulls, severity_score_array
from arrowdsl.compute.predicates import BitmaskMatch, BoolColumn, FilterSpec, InValues, Not
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ArrayLike, DataTypeLike, TableLike
from arrowdsl.plan.ops import JoinSpec
from arrowdsl.plan.plan import Plan, hash_join
from arrowdsl.schema.arrays import (
    ColumnDefaultsSpec,
    ConstExpr,
    FieldExpr,
    const_array,
    set_or_append_column,
)
from cpg.catalog import TableCatalog
from cpg.kinds import (
    SCIP_ROLE_DEFINITION,
    SCIP_ROLE_IMPORT,
    SCIP_ROLE_READ,
    SCIP_ROLE_WRITE,
    EdgeKind,
)
from cpg.specs import EdgeEmitSpec, TableFilter

type RelationBuilder = Callable[[TableCatalog, ExecutionContext], TableLike | None]


@dataclass(frozen=True)
class EdgeRelationSpec:
    """Spec for building an edge relation table."""

    name: str
    option_flag: str
    emit: EdgeEmitSpec
    relation_builder: RelationBuilder
    filter_fn: TableFilter | None = None

    def build(self, catalog: TableCatalog, *, ctx: ExecutionContext) -> TableLike | None:
        """Return the relation table for this spec, after filtering.

        Returns
        -------
        TableLike | None
            Relation table or ``None`` when empty.
        """
        rel = self.relation_builder(catalog, ctx)
        if rel is None or rel.num_rows == 0:
            return None
        if self.filter_fn is not None:
            rel = self.filter_fn(rel)
            if rel.num_rows == 0:
                return None
        return rel


def _get(table: TableLike, col: str, *, default_type: DataTypeLike) -> ArrayLike:
    if col in table.column_names:
        return table[col]
    return pa.nulls(table.num_rows, type=default_type)


def _table_relation(name: str) -> RelationBuilder:
    def _build(catalog: TableCatalog, _ctx: ExecutionContext) -> TableLike | None:
        return catalog.tables.get(name)

    return _build


def _filter_unresolved_qname_calls(
    rel_callsite_qname: TableLike,
    rel_callsite_symbol: TableLike | None,
) -> TableLike:
    if "call_id" not in rel_callsite_qname.column_names:
        return rel_callsite_qname
    if rel_callsite_symbol is None or "call_id" not in rel_callsite_symbol.column_names:
        return rel_callsite_qname
    resolved = drop_nulls(rel_callsite_symbol["call_id"])
    predicate = Not(InValues(col="call_id", values=resolved, fill_null=False))
    return FilterSpec(predicate).apply_kernel(rel_callsite_qname)


def _ensure_ambiguity_group_id(table: TableLike) -> TableLike:
    if "call_id" not in table.column_names:
        return table
    defaults = ColumnDefaultsSpec(
        defaults=(("ambiguity_group_id", FieldExpr("call_id")),),
    )
    return defaults.apply(table)


def _with_repo_file_ids(
    diag_table: TableLike,
    repo_files: TableLike | None,
    *,
    ctx: ExecutionContext,
) -> TableLike:
    if (
        repo_files is None
        or repo_files.num_rows == 0
        or "path" not in diag_table.column_names
        or "path" not in repo_files.column_names
        or "file_id" not in repo_files.column_names
    ):
        return diag_table
    repo_subset = repo_files.select(["path", "file_id"])
    left = Plan.table_source(diag_table, label="diagnostic")
    right = Plan.table_source(repo_subset, label="repo_files")
    joined = hash_join(
        left=left,
        right=right,
        spec=JoinSpec(
            join_type="left outer",
            left_keys=("path",),
            right_keys=("path",),
            left_output=tuple(diag_table.column_names),
            right_output=("file_id",),
            output_suffix_for_right="_repo",
        ),
    ).to_table(ctx=ctx)
    if "file_id_repo" in joined.column_names and "file_id" in joined.column_names:
        resolved = coalesce_arrays([joined["file_id"], joined["file_id_repo"]])
        joined = set_or_append_column(joined, "file_id", resolved)
        joined = joined.drop(["file_id_repo"])
    return joined


def _symbol_role_filter(mask: int, *, must_set: bool) -> TableFilter:
    def _filter(table: TableLike) -> TableLike:
        defaults = ColumnDefaultsSpec(
            defaults=(("symbol_roles", ConstExpr(0, dtype=pa.int64())),),
        )
        table = defaults.apply(table)
        predicate = BitmaskMatch(col="symbol_roles", bitmask=mask, must_set=must_set)
        return FilterSpec(predicate).apply_kernel(table)

    return _filter


def _flag_filter(flag_col: str) -> TableFilter:
    def _filter(table: TableLike) -> TableLike:
        defaults = ColumnDefaultsSpec(
            defaults=((flag_col, ConstExpr(value=False, dtype=pa.bool_())),),
        )
        table = defaults.apply(table)
        predicate = BoolColumn(col=flag_col, fill_null=False)
        return FilterSpec(predicate).apply_kernel(table)

    return _filter


def qname_fallback_relation(catalog: TableCatalog, _ctx: ExecutionContext) -> TableLike | None:
    rel_callsite_qname = catalog.tables.get("rel_callsite_qname")
    if rel_callsite_qname is None or rel_callsite_qname.num_rows == 0:
        return None
    rel_callsite_symbol = catalog.tables.get("rel_callsite_symbol")
    filtered = _filter_unresolved_qname_calls(rel_callsite_qname, rel_callsite_symbol)
    if filtered.num_rows == 0:
        return None
    return _ensure_ambiguity_group_id(filtered)


def diagnostic_relation(catalog: TableCatalog, ctx: ExecutionContext) -> TableLike | None:
    diagnostics_norm = catalog.tables.get("diagnostics_norm")
    if diagnostics_norm is None or diagnostics_norm.num_rows == 0:
        return None
    diag = _with_repo_file_ids(diagnostics_norm, catalog.tables.get("repo_files"), ctx=ctx)
    if "diag_id" not in diag.column_names:
        return None
    n = diag.num_rows
    severity = _get(diag, "severity", default_type=pa.string())
    score = severity_score_array(severity)
    origin = _get(diag, "diag_source", default_type=pa.string())
    origin = coalesce_arrays([origin, pa.scalar("diagnostic")])
    diag = set_or_append_column(diag, "origin", origin)
    diag = set_or_append_column(diag, "confidence", score)
    diag = set_or_append_column(diag, "score", score)
    return set_or_append_column(
        diag, "resolution_method", const_array(n, "DIAGNOSTIC", dtype=pa.string())
    )


def type_annotation_relation(catalog: TableCatalog, _ctx: ExecutionContext) -> TableLike | None:
    type_exprs_norm = catalog.tables.get("type_exprs_norm")
    if type_exprs_norm is None or type_exprs_norm.num_rows == 0:
        return None
    n = type_exprs_norm.num_rows
    rel = set_or_append_column(
        type_exprs_norm, "confidence", const_array(n, 1.0, dtype=pa.float32())
    )
    return set_or_append_column(rel, "score", const_array(n, 1.0, dtype=pa.float32()))


def inferred_type_relation(catalog: TableCatalog, _ctx: ExecutionContext) -> TableLike | None:
    type_exprs_norm = catalog.tables.get("type_exprs_norm")
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
            const_array(n, 1.0, dtype=pa.float32()),
            const_array(n, 1.0, dtype=pa.float32()),
        ],
        names=["owner_def_id", "type_id", "path", "bstart", "bend", "confidence", "score"],
    )


def runtime_relation(
    catalog: TableCatalog,
    _ctx: ExecutionContext,
    *,
    key: str,
    src_col: str,
    dst_col: str,
) -> TableLike | None:
    table = catalog.tables.get(key)
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
            const_array(n, 1.0, dtype=pa.float32()),
            const_array(n, 1.0, dtype=pa.float32()),
        ],
        names=[src_col, dst_col, "path", "bstart", "bend", "confidence", "score"],
    )


EDGE_RELATION_SPECS: tuple[EdgeRelationSpec, ...] = (
    EdgeRelationSpec(
        name="symbol_role_defines",
        option_flag="emit_symbol_role_edges",
        relation_builder=_table_relation("rel_name_symbol"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_DEFINES_SYMBOL,
            src_cols=("name_ref_id",),
            dst_cols=("symbol",),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
        filter_fn=_symbol_role_filter(SCIP_ROLE_DEFINITION, must_set=True),
    ),
    EdgeRelationSpec(
        name="symbol_role_references",
        option_flag="emit_symbol_role_edges",
        relation_builder=_table_relation("rel_name_symbol"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_REFERENCES_SYMBOL,
            src_cols=("name_ref_id",),
            dst_cols=("symbol",),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
        filter_fn=_symbol_role_filter(SCIP_ROLE_DEFINITION, must_set=False),
    ),
    EdgeRelationSpec(
        name="symbol_role_reads",
        option_flag="emit_symbol_role_edges",
        relation_builder=_table_relation("rel_name_symbol"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_READS_SYMBOL,
            src_cols=("name_ref_id",),
            dst_cols=("symbol",),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
        filter_fn=_symbol_role_filter(SCIP_ROLE_READ, must_set=True),
    ),
    EdgeRelationSpec(
        name="symbol_role_writes",
        option_flag="emit_symbol_role_edges",
        relation_builder=_table_relation("rel_name_symbol"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_WRITES_SYMBOL,
            src_cols=("name_ref_id",),
            dst_cols=("symbol",),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
        filter_fn=_symbol_role_filter(SCIP_ROLE_WRITE, must_set=True),
    ),
    EdgeRelationSpec(
        name="scip_symbol_reference",
        option_flag="emit_scip_symbol_relationship_edges",
        relation_builder=_table_relation("scip_symbol_relationships"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.SCIP_SYMBOL_REFERENCE,
            src_cols=("symbol",),
            dst_cols=("related_symbol",),
            origin="scip",
            default_resolution_method="SCIP_SYMBOL_REFERENCE",
        ),
        filter_fn=_flag_filter("is_reference"),
    ),
    EdgeRelationSpec(
        name="scip_symbol_implementation",
        option_flag="emit_scip_symbol_relationship_edges",
        relation_builder=_table_relation("scip_symbol_relationships"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.SCIP_SYMBOL_IMPLEMENTATION,
            src_cols=("symbol",),
            dst_cols=("related_symbol",),
            origin="scip",
            default_resolution_method="SCIP_SYMBOL_IMPLEMENTATION",
        ),
        filter_fn=_flag_filter("is_implementation"),
    ),
    EdgeRelationSpec(
        name="scip_symbol_type_definition",
        option_flag="emit_scip_symbol_relationship_edges",
        relation_builder=_table_relation("scip_symbol_relationships"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.SCIP_SYMBOL_TYPE_DEFINITION,
            src_cols=("symbol",),
            dst_cols=("related_symbol",),
            origin="scip",
            default_resolution_method="SCIP_SYMBOL_TYPE_DEFINITION",
        ),
        filter_fn=_flag_filter("is_type_definition"),
    ),
    EdgeRelationSpec(
        name="scip_symbol_definition",
        option_flag="emit_scip_symbol_relationship_edges",
        relation_builder=_table_relation("scip_symbol_relationships"),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.SCIP_SYMBOL_DEFINITION,
            src_cols=("symbol",),
            dst_cols=("related_symbol",),
            origin="scip",
            default_resolution_method="SCIP_SYMBOL_DEFINITION",
        ),
        filter_fn=_flag_filter("is_definition"),
    ),
    EdgeRelationSpec(
        name="import_edges",
        option_flag="emit_import_edges",
        relation_builder=_table_relation("rel_import_symbol"),
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
    EdgeRelationSpec(
        name="call_edges",
        option_flag="emit_call_edges",
        relation_builder=_table_relation("rel_callsite_symbol"),
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
    EdgeRelationSpec(
        name="qname_fallback_calls",
        option_flag="emit_qname_fallback_call_edges",
        relation_builder=qname_fallback_relation,
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
    EdgeRelationSpec(
        name="diagnostic_edges",
        option_flag="emit_diagnostic_edges",
        relation_builder=diagnostic_relation,
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.HAS_DIAGNOSTIC,
            src_cols=("file_id",),
            dst_cols=("diag_id",),
            origin="diagnostic",
            default_resolution_method="DIAGNOSTIC",
        ),
    ),
    EdgeRelationSpec(
        name="type_annotation_edges",
        option_flag="emit_type_edges",
        relation_builder=type_annotation_relation,
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.HAS_ANNOTATION,
            src_cols=("owner_def_id",),
            dst_cols=("type_expr_id",),
            origin="annotation",
            default_resolution_method="TYPE_ANNOTATION",
        ),
    ),
    EdgeRelationSpec(
        name="inferred_type_edges",
        option_flag="emit_type_edges",
        relation_builder=inferred_type_relation,
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.INFERRED_TYPE,
            src_cols=("owner_def_id",),
            dst_cols=("type_id",),
            origin="inferred",
            default_resolution_method="ANNOTATION_INFER",
        ),
    ),
    EdgeRelationSpec(
        name="runtime_signatures",
        option_flag="emit_runtime_edges",
        relation_builder=lambda catalog, ctx: runtime_relation(
            catalog, ctx, key="rt_signatures", src_col="rt_id", dst_col="sig_id"
        ),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.RT_HAS_SIGNATURE,
            src_cols=("rt_id",),
            dst_cols=("sig_id",),
            origin="inspect",
            default_resolution_method="RUNTIME_INSPECT",
        ),
    ),
    EdgeRelationSpec(
        name="runtime_params",
        option_flag="emit_runtime_edges",
        relation_builder=lambda catalog, ctx: runtime_relation(
            catalog, ctx, key="rt_signature_params", src_col="sig_id", dst_col="param_id"
        ),
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.RT_HAS_PARAM,
            src_cols=("sig_id",),
            dst_cols=("param_id",),
            origin="inspect",
            default_resolution_method="RUNTIME_INSPECT",
        ),
    ),
    EdgeRelationSpec(
        name="runtime_members",
        option_flag="emit_runtime_edges",
        relation_builder=lambda catalog, ctx: runtime_relation(
            catalog, ctx, key="rt_members", src_col="rt_id", dst_col="member_id"
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


__all__ = ["EDGE_RELATION_SPECS", "EdgeRelationSpec"]
