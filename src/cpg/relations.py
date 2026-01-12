"""Edge relation registry and relation builders for CPG edges."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, DataTypeLike, ensure_expression, pc
from arrowdsl.plan.ops import JoinSpec
from arrowdsl.plan.plan import Plan, hash_join
from cpg.catalog import PlanCatalog, PlanRef
from cpg.kinds import (
    SCIP_ROLE_DEFINITION,
    SCIP_ROLE_IMPORT,
    SCIP_ROLE_READ,
    SCIP_ROLE_WRITE,
    EdgeKind,
)
from cpg.plan_helpers import set_or_append_column
from cpg.specs import EdgeEmitSpec

type RelationBuilder = Callable[[PlanCatalog, ExecutionContext], Plan | None]
type PlanFilter = Callable[[Plan, ExecutionContext], Plan]


@dataclass(frozen=True)
class EdgeRelationSpec:
    """Spec for building an edge relation plan."""

    name: str
    option_flag: str
    emit: EdgeEmitSpec
    relation_builder: RelationBuilder
    filter_fn: PlanFilter | None = None

    def build(self, catalog: PlanCatalog, *, ctx: ExecutionContext) -> Plan | None:
        """Return the relation plan for this spec, after filtering.

        Returns
        -------
        Plan | None
            Relation plan or ``None`` when unavailable.
        """
        rel = self.relation_builder(catalog, ctx)
        if rel is None:
            return None
        if self.filter_fn is not None:
            rel = self.filter_fn(rel, ctx)
        return rel


def _field_expr(
    name: str,
    *,
    available: set[str],
    dtype: DataTypeLike,
) -> ComputeExpression:
    if name in available:
        return ensure_expression(pc.cast(pc.field(name), dtype, safe=False))
    return ensure_expression(pc.cast(pc.scalar(None), dtype, safe=False))


def _plan_relation(name: str) -> RelationBuilder:
    ref = PlanRef(name)

    def _build(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
        return catalog.resolve(ref, ctx=ctx)

    return _build


def _filter_unresolved_qname_calls(
    rel_callsite_qname: Plan,
    rel_callsite_symbol: Plan | None,
    *,
    ctx: ExecutionContext,
) -> Plan:
    if rel_callsite_symbol is None:
        return rel_callsite_qname
    left_cols = rel_callsite_qname.schema(ctx=ctx).names
    if "call_id" not in left_cols:
        return rel_callsite_qname
    right_cols = rel_callsite_symbol.schema(ctx=ctx).names
    if "call_id" not in right_cols:
        return rel_callsite_qname
    return hash_join(
        left=rel_callsite_qname,
        right=rel_callsite_symbol,
        spec=JoinSpec(
            join_type="left anti",
            left_keys=("call_id",),
            right_keys=("call_id",),
            left_output=tuple(left_cols),
            right_output=(),
        ),
    )


def _ensure_ambiguity_group_id(plan: Plan, *, ctx: ExecutionContext) -> Plan:
    available = set(plan.schema(ctx=ctx).names)
    if "call_id" not in available:
        return plan
    return set_or_append_column(plan, name="ambiguity_group_id", expr=pc.field("call_id"), ctx=ctx)


def _with_repo_file_ids(
    diag_plan: Plan,
    repo_files: Plan | None,
    *,
    ctx: ExecutionContext,
) -> Plan:
    if repo_files is None:
        return diag_plan
    diag_cols = diag_plan.schema(ctx=ctx).names
    if "path" not in diag_cols:
        return diag_plan
    repo_cols = repo_files.schema(ctx=ctx).names
    if "path" not in repo_cols or "file_id" not in repo_cols:
        return diag_plan

    repo_proj = repo_files.project(
        [pc.field("path"), pc.field("file_id")],
        ["path", "file_id"],
        ctx=ctx,
    )
    joined = hash_join(
        left=diag_plan,
        right=repo_proj,
        spec=JoinSpec(
            join_type="left outer",
            left_keys=("path",),
            right_keys=("path",),
            left_output=tuple(diag_cols),
            right_output=("file_id",),
            output_suffix_for_right="_repo",
        ),
    )
    joined_cols = joined.schema(ctx=ctx).names
    if "file_id_repo" not in joined_cols:
        return joined

    available = set(joined_cols)
    if "file_id" in available:
        file_id = ensure_expression(pc.coalesce(pc.field("file_id"), pc.field("file_id_repo")))
    else:
        file_id = pc.field("file_id_repo")
    joined = set_or_append_column(joined, name="file_id", expr=file_id, ctx=ctx)
    keep = [col for col in joined.schema(ctx=ctx).names if col != "file_id_repo"]
    return joined.project([pc.field(col) for col in keep], keep, ctx=ctx)


def _severity_score_expr(expr: ComputeExpression) -> ComputeExpression:
    severity = ensure_expression(pc.cast(expr, pa.string(), safe=False))
    severity = ensure_expression(pc.fill_null(severity, pc.scalar("ERROR")))
    is_error = ensure_expression(pc.equal(severity, pc.scalar("ERROR")))
    is_warning = ensure_expression(pc.equal(severity, pc.scalar("WARNING")))
    score = ensure_expression(
        pc.if_else(
            is_error,
            pc.scalar(1.0),
            pc.if_else(is_warning, pc.scalar(0.7), pc.scalar(0.5)),
        )
    )
    return ensure_expression(pc.cast(score, pa.float32(), safe=False))


def _symbol_role_filter(mask: int, *, must_set: bool) -> PlanFilter:
    def _filter(plan: Plan, ctx: ExecutionContext) -> Plan:
        available = set(plan.schema(ctx=ctx).names)
        if "symbol_roles" in available:
            roles = pc.cast(pc.field("symbol_roles"), pa.int64(), safe=False)
        else:
            roles = pc.cast(pc.scalar(0), pa.int64(), safe=False)
        hit = pc.not_equal(pc.bit_wise_and(roles, pa.scalar(mask)), pa.scalar(0))
        hit = pc.fill_null(hit, fill_value=False)
        if not must_set:
            hit = pc.invert(hit)
        return plan.filter(ensure_expression(hit), ctx=ctx)

    return _filter


def _flag_filter(flag_col: str) -> PlanFilter:
    def _filter(plan: Plan, ctx: ExecutionContext) -> Plan:
        available = set(plan.schema(ctx=ctx).names)
        if flag_col in available:
            flag = pc.cast(pc.field(flag_col), pa.bool_(), safe=False)
        else:
            flag = pc.cast(pc.scalar(0), pa.bool_(), safe=False)
        flag = pc.fill_null(flag, fill_value=False)
        return plan.filter(ensure_expression(flag), ctx=ctx)

    return _filter


def qname_fallback_relation(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    rel_callsite_qname = catalog.resolve(PlanRef("rel_callsite_qname"), ctx=ctx)
    if rel_callsite_qname is None:
        return None
    rel_callsite_symbol = catalog.resolve(PlanRef("rel_callsite_symbol"), ctx=ctx)
    filtered = _filter_unresolved_qname_calls(rel_callsite_qname, rel_callsite_symbol, ctx=ctx)
    return _ensure_ambiguity_group_id(filtered, ctx=ctx)


def diagnostic_relation(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    diagnostics_norm = catalog.resolve(PlanRef("diagnostics_norm"), ctx=ctx)
    if diagnostics_norm is None:
        return None
    repo_files = catalog.resolve(PlanRef("repo_files"), ctx=ctx)
    diag = _with_repo_file_ids(diagnostics_norm, repo_files, ctx=ctx)
    available = set(diag.schema(ctx=ctx).names)
    if "diag_id" not in available:
        return None

    severity = _field_expr("severity", available=available, dtype=pa.string())
    score = _severity_score_expr(severity)
    origin = _field_expr("diag_source", available=available, dtype=pa.string())
    origin = ensure_expression(pc.coalesce(origin, pc.scalar("diagnostic")))

    diag = set_or_append_column(diag, name="origin", expr=origin, ctx=ctx)
    diag = set_or_append_column(diag, name="confidence", expr=score, ctx=ctx)
    diag = set_or_append_column(diag, name="score", expr=score, ctx=ctx)
    resolution = ensure_expression(pc.cast(pc.scalar("DIAGNOSTIC"), pa.string(), safe=False))
    return set_or_append_column(diag, name="resolution_method", expr=resolution, ctx=ctx)


def type_annotation_relation(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    type_exprs_norm = catalog.resolve(PlanRef("type_exprs_norm"), ctx=ctx)
    if type_exprs_norm is None:
        return None
    score = ensure_expression(pc.cast(pc.scalar(1.0), pa.float32(), safe=False))
    plan = set_or_append_column(type_exprs_norm, name="confidence", expr=score, ctx=ctx)
    return set_or_append_column(plan, name="score", expr=score, ctx=ctx)


def inferred_type_relation(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    type_exprs_norm = catalog.resolve(PlanRef("type_exprs_norm"), ctx=ctx)
    if type_exprs_norm is None:
        return None
    available = set(type_exprs_norm.schema(ctx=ctx).names)
    exprs = [
        _field_expr("owner_def_id", available=available, dtype=pa.string()),
        _field_expr("type_id", available=available, dtype=pa.string()),
        ensure_expression(pc.cast(pc.scalar(None), pa.string(), safe=False)),
        ensure_expression(pc.cast(pc.scalar(None), pa.int64(), safe=False)),
        ensure_expression(pc.cast(pc.scalar(None), pa.int64(), safe=False)),
        ensure_expression(pc.cast(pc.scalar(1.0), pa.float32(), safe=False)),
        ensure_expression(pc.cast(pc.scalar(1.0), pa.float32(), safe=False)),
    ]
    names = ["owner_def_id", "type_id", "path", "bstart", "bend", "confidence", "score"]
    return type_exprs_norm.project(exprs, names, ctx=ctx)


def runtime_relation(
    catalog: PlanCatalog,
    ctx: ExecutionContext,
    *,
    key: str,
    src_col: str,
    dst_col: str,
) -> Plan | None:
    table = catalog.resolve(PlanRef(key), ctx=ctx)
    if table is None:
        return None
    available = set(table.schema(ctx=ctx).names)
    exprs = [
        _field_expr(src_col, available=available, dtype=pa.string()),
        _field_expr(dst_col, available=available, dtype=pa.string()),
        ensure_expression(pc.cast(pc.scalar(None), pa.string(), safe=False)),
        ensure_expression(pc.cast(pc.scalar(None), pa.int64(), safe=False)),
        ensure_expression(pc.cast(pc.scalar(None), pa.int64(), safe=False)),
        ensure_expression(pc.cast(pc.scalar(1.0), pa.float32(), safe=False)),
        ensure_expression(pc.cast(pc.scalar(1.0), pa.float32(), safe=False)),
    ]
    names = [src_col, dst_col, "path", "bstart", "bend", "confidence", "score"]
    return table.project(exprs, names, ctx=ctx)


EDGE_RELATION_SPECS: tuple[EdgeRelationSpec, ...] = (
    EdgeRelationSpec(
        name="symbol_role_defines",
        option_flag="emit_symbol_role_edges",
        relation_builder=_plan_relation("rel_name_symbol"),
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
        relation_builder=_plan_relation("rel_name_symbol"),
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
        relation_builder=_plan_relation("rel_name_symbol"),
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
        relation_builder=_plan_relation("rel_name_symbol"),
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
        relation_builder=_plan_relation("scip_symbol_relationships"),
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
        relation_builder=_plan_relation("scip_symbol_relationships"),
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
        relation_builder=_plan_relation("scip_symbol_relationships"),
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
        relation_builder=_plan_relation("scip_symbol_relationships"),
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
        relation_builder=_plan_relation("rel_import_symbol"),
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
        relation_builder=_plan_relation("rel_callsite_symbol"),
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
