"""Relationship rule registry for CPG edge construction."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cache

import pyarrow as pa

from arrowdsl.compute.expr_core import ScalarValue
from arrowdsl.core.context import ExecutionContext
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import QuerySpec
from arrowdsl.schema.build import ConstExpr, FieldExpr
from arrowdsl.spec.expr_ir import ExprIR
from arrowdsl.spec.tables.relspec import relationship_rule_table, relationship_rules_from_table
from cpg.catalog import PlanCatalog, resolve_plan_source
from cpg.kinds_ultimate import (
    SCIP_ROLE_DEFINITION,
    SCIP_ROLE_IMPORT,
    SCIP_ROLE_READ,
    SCIP_ROLE_WRITE,
    EdgeKind,
)
from cpg.plan_specs import ensure_plan
from cpg.specs import EdgeEmitSpec, EdgePlanSpec
from relspec.compiler import RelationshipRuleCompiler
from relspec.model import (
    AddLiteralSpec,
    DatasetRef,
    FilterKernelSpec,
    HashJoinConfig,
    ProjectConfig,
    RelationshipRule,
    RuleKind,
)

FALSE_VALUE: ScalarValue = False


def _field(name: str) -> ExprIR:
    return ExprIR(op="field", name=name)


def _literal(value: ScalarValue) -> ExprIR:
    return ExprIR(op="literal", value=value)


def _call(name: str, *args: ExprIR) -> ExprIR:
    return ExprIR(op="call", name=name, args=tuple(args))


def _fill_null(expr: ExprIR, value: ScalarValue) -> ExprIR:
    return _call("fill_null", expr, _literal(value))


def _bitmask_predicate(field_name: str, mask: int, *, invert: bool = False) -> ExprIR:
    masked = _call("bit_wise_and", _field(field_name), _literal(mask))
    hit = _call("not_equal", masked, _literal(0))
    hit = _fill_null(hit, value=FALSE_VALUE)
    if invert:
        return _call("invert", hit)
    return hit


def _flag_predicate(field_name: str) -> ExprIR:
    return _fill_null(_field(field_name), value=FALSE_VALUE)


def _severity_score_expr() -> ExprIR:
    severity = _fill_null(_field("severity"), "ERROR")
    is_error = _call("equal", severity, _literal("ERROR"))
    is_warning = _call("equal", severity, _literal("WARNING"))
    return _call(
        "if_else",
        is_error,
        _literal(1.0),
        _call("if_else", is_warning, _literal(0.7), _literal(0.5)),
    )


def _project(
    *,
    select: Sequence[str] = (),
    exprs: dict[str, ExprIR] | None = None,
) -> ProjectConfig:
    return ProjectConfig(select=tuple(select), exprs=exprs or {})


def _filter_rule(
    *,
    name: str,
    input_name: str,
    predicate: ExprIR | None = None,
    project: ProjectConfig | None = None,
    post_kernels: Sequence[AddLiteralSpec] = (),
) -> RelationshipRule:
    kernels: list[FilterKernelSpec | AddLiteralSpec] = []
    if predicate is not None:
        kernels.append(FilterKernelSpec(predicate=predicate))
    kernels.extend(post_kernels)
    return RelationshipRule(
        name=name,
        kind=RuleKind.FILTER_PROJECT,
        output_dataset=name,
        inputs=(DatasetRef(name=input_name),),
        project=project or _project(),
        post_kernels=tuple(kernels),
    )


def _edge_spec(
    *,
    name: str,
    option_flag: str,
    emit: EdgeEmitSpec,
) -> EdgePlanSpec:
    return EdgePlanSpec(
        name=name,
        option_flag=option_flag,
        relation_ref=name,
        emit=emit,
    )


RELATION_RULES: tuple[RelationshipRule, ...] = (
    _filter_rule(
        name="symbol_role_defines",
        input_name="rel_name_symbol",
        predicate=_bitmask_predicate("symbol_roles", SCIP_ROLE_DEFINITION),
        project=_project(
            select=(
                "name_ref_id",
                "symbol",
                "path",
                "bstart",
                "bend",
                "symbol_roles",
            )
        ),
    ),
    _filter_rule(
        name="symbol_role_references",
        input_name="rel_name_symbol",
        predicate=_bitmask_predicate("symbol_roles", SCIP_ROLE_DEFINITION, invert=True),
        project=_project(
            select=(
                "name_ref_id",
                "symbol",
                "path",
                "bstart",
                "bend",
                "symbol_roles",
            )
        ),
    ),
    _filter_rule(
        name="symbol_role_reads",
        input_name="rel_name_symbol",
        predicate=_bitmask_predicate("symbol_roles", SCIP_ROLE_READ),
        project=_project(
            select=(
                "name_ref_id",
                "symbol",
                "path",
                "bstart",
                "bend",
                "symbol_roles",
            )
        ),
    ),
    _filter_rule(
        name="symbol_role_writes",
        input_name="rel_name_symbol",
        predicate=_bitmask_predicate("symbol_roles", SCIP_ROLE_WRITE),
        project=_project(
            select=(
                "name_ref_id",
                "symbol",
                "path",
                "bstart",
                "bend",
                "symbol_roles",
            )
        ),
    ),
    _filter_rule(
        name="scip_symbol_reference",
        input_name="scip_symbol_relationships",
        predicate=_flag_predicate("is_reference"),
        project=_project(select=("symbol", "related_symbol", "is_reference")),
    ),
    _filter_rule(
        name="scip_symbol_implementation",
        input_name="scip_symbol_relationships",
        predicate=_flag_predicate("is_implementation"),
        project=_project(select=("symbol", "related_symbol", "is_implementation")),
    ),
    _filter_rule(
        name="scip_symbol_type_definition",
        input_name="scip_symbol_relationships",
        predicate=_flag_predicate("is_type_definition"),
        project=_project(select=("symbol", "related_symbol", "is_type_definition")),
    ),
    _filter_rule(
        name="scip_symbol_definition",
        input_name="scip_symbol_relationships",
        predicate=_flag_predicate("is_definition"),
        project=_project(select=("symbol", "related_symbol", "is_definition")),
    ),
    _filter_rule(
        name="import_edges",
        input_name="rel_import_symbol",
        predicate=_bitmask_predicate("symbol_roles", SCIP_ROLE_IMPORT),
        project=_project(
            select=(
                "import_alias_id",
                "import_id",
                "symbol",
                "path",
                "bstart",
                "bend",
                "alias_bstart",
                "alias_bend",
                "symbol_roles",
            )
        ),
    ),
    _filter_rule(
        name="call_edges",
        input_name="rel_callsite_symbol",
        project=_project(
            select=("call_id", "symbol", "path", "call_bstart", "call_bend", "bstart", "bend")
        ),
    ),
    RelationshipRule(
        name="qname_fallback_calls",
        kind=RuleKind.HASH_JOIN,
        output_dataset="qname_fallback_calls",
        inputs=(DatasetRef(name="rel_callsite_qname"), DatasetRef(name="rel_callsite_symbol")),
        hash_join=HashJoinConfig(
            join_type="left anti",
            left_keys=("call_id",),
            right_keys=("call_id",),
            left_output=(
                "call_id",
                "qname_id",
                "path",
                "call_bstart",
                "call_bend",
                "bstart",
                "bend",
            ),
            right_output=(),
        ),
        project=ProjectConfig(
            select=(
                "call_id",
                "qname_id",
                "path",
                "call_bstart",
                "call_bend",
                "bstart",
                "bend",
            ),
            exprs={"ambiguity_group_id": _field("call_id")},
        ),
    ),
    _filter_rule(
        name="diagnostic_edges",
        input_name="diagnostics_norm",
        project=_project(
            select=("diag_id", "file_id", "path", "bstart", "bend", "severity"),
            exprs={
                "origin": _fill_null(_field("diag_source"), "diagnostic"),
                "confidence": _severity_score_expr(),
                "score": _severity_score_expr(),
            },
        ),
    ),
    _filter_rule(
        name="type_annotation_edges",
        input_name="type_exprs_norm",
        project=_project(select=("owner_def_id", "type_expr_id", "path", "bstart", "bend")),
        post_kernels=(
            AddLiteralSpec(name="confidence", value=pa.scalar(1.0, type=pa.float32())),
            AddLiteralSpec(name="score", value=pa.scalar(1.0, type=pa.float32())),
        ),
    ),
    _filter_rule(
        name="inferred_type_edges",
        input_name="type_exprs_norm",
        project=_project(select=("owner_def_id", "type_id")),
        post_kernels=(
            AddLiteralSpec(name="path", value=pa.scalar(None, type=pa.string())),
            AddLiteralSpec(name="bstart", value=pa.scalar(None, type=pa.int64())),
            AddLiteralSpec(name="bend", value=pa.scalar(None, type=pa.int64())),
            AddLiteralSpec(name="confidence", value=pa.scalar(1.0, type=pa.float32())),
            AddLiteralSpec(name="score", value=pa.scalar(1.0, type=pa.float32())),
        ),
    ),
    _filter_rule(
        name="runtime_signatures",
        input_name="rt_signatures",
        project=_project(select=("rt_id", "sig_id")),
        post_kernels=(
            AddLiteralSpec(name="path", value=pa.scalar(None, type=pa.string())),
            AddLiteralSpec(name="bstart", value=pa.scalar(None, type=pa.int64())),
            AddLiteralSpec(name="bend", value=pa.scalar(None, type=pa.int64())),
            AddLiteralSpec(name="confidence", value=pa.scalar(1.0, type=pa.float32())),
            AddLiteralSpec(name="score", value=pa.scalar(1.0, type=pa.float32())),
        ),
    ),
    _filter_rule(
        name="runtime_params",
        input_name="rt_signature_params",
        project=_project(select=("sig_id", "param_id")),
        post_kernels=(
            AddLiteralSpec(name="path", value=pa.scalar(None, type=pa.string())),
            AddLiteralSpec(name="bstart", value=pa.scalar(None, type=pa.int64())),
            AddLiteralSpec(name="bend", value=pa.scalar(None, type=pa.int64())),
            AddLiteralSpec(name="confidence", value=pa.scalar(1.0, type=pa.float32())),
            AddLiteralSpec(name="score", value=pa.scalar(1.0, type=pa.float32())),
        ),
    ),
    _filter_rule(
        name="runtime_members",
        input_name="rt_members",
        project=_project(select=("rt_id", "member_id")),
        post_kernels=(
            AddLiteralSpec(name="path", value=pa.scalar(None, type=pa.string())),
            AddLiteralSpec(name="bstart", value=pa.scalar(None, type=pa.int64())),
            AddLiteralSpec(name="bend", value=pa.scalar(None, type=pa.int64())),
            AddLiteralSpec(name="confidence", value=pa.scalar(1.0, type=pa.float32())),
            AddLiteralSpec(name="score", value=pa.scalar(1.0, type=pa.float32())),
        ),
    ),
)


EDGE_PLAN_SPECS: tuple[EdgePlanSpec, ...] = (
    _edge_spec(
        name="symbol_role_defines",
        option_flag="emit_symbol_role_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_DEFINES_SYMBOL,
            src_cols=("name_ref_id",),
            dst_cols=("symbol",),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
    ),
    _edge_spec(
        name="symbol_role_references",
        option_flag="emit_symbol_role_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_REFERENCES_SYMBOL,
            src_cols=("name_ref_id",),
            dst_cols=("symbol",),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
    ),
    _edge_spec(
        name="symbol_role_reads",
        option_flag="emit_symbol_role_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_READS_SYMBOL,
            src_cols=("name_ref_id",),
            dst_cols=("symbol",),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
    ),
    _edge_spec(
        name="symbol_role_writes",
        option_flag="emit_symbol_role_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.PY_WRITES_SYMBOL,
            src_cols=("name_ref_id",),
            dst_cols=("symbol",),
            origin="scip",
            default_resolution_method="SPAN_EXACT",
        ),
    ),
    _edge_spec(
        name="scip_symbol_reference",
        option_flag="emit_scip_symbol_relationship_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.SCIP_SYMBOL_REFERENCE,
            src_cols=("symbol",),
            dst_cols=("related_symbol",),
            origin="scip",
            default_resolution_method="SCIP_SYMBOL_REFERENCE",
        ),
    ),
    _edge_spec(
        name="scip_symbol_implementation",
        option_flag="emit_scip_symbol_relationship_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.SCIP_SYMBOL_IMPLEMENTATION,
            src_cols=("symbol",),
            dst_cols=("related_symbol",),
            origin="scip",
            default_resolution_method="SCIP_SYMBOL_IMPLEMENTATION",
        ),
    ),
    _edge_spec(
        name="scip_symbol_type_definition",
        option_flag="emit_scip_symbol_relationship_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.SCIP_SYMBOL_TYPE_DEFINITION,
            src_cols=("symbol",),
            dst_cols=("related_symbol",),
            origin="scip",
            default_resolution_method="SCIP_SYMBOL_TYPE_DEFINITION",
        ),
    ),
    _edge_spec(
        name="scip_symbol_definition",
        option_flag="emit_scip_symbol_relationship_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.SCIP_SYMBOL_DEFINITION,
            src_cols=("symbol",),
            dst_cols=("related_symbol",),
            origin="scip",
            default_resolution_method="SCIP_SYMBOL_DEFINITION",
        ),
    ),
    _edge_spec(
        name="import_edges",
        option_flag="emit_import_edges",
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
    ),
    _edge_spec(
        name="call_edges",
        option_flag="emit_call_edges",
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
    _edge_spec(
        name="qname_fallback_calls",
        option_flag="emit_qname_fallback_call_edges",
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
    _edge_spec(
        name="diagnostic_edges",
        option_flag="emit_diagnostic_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.HAS_DIAGNOSTIC,
            src_cols=("file_id",),
            dst_cols=("diag_id",),
            origin="diagnostic",
            default_resolution_method="DIAGNOSTIC",
        ),
    ),
    _edge_spec(
        name="type_annotation_edges",
        option_flag="emit_type_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.HAS_ANNOTATION,
            src_cols=("owner_def_id",),
            dst_cols=("type_expr_id",),
            origin="annotation",
            default_resolution_method="TYPE_ANNOTATION",
        ),
    ),
    _edge_spec(
        name="inferred_type_edges",
        option_flag="emit_type_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.INFERRED_TYPE,
            src_cols=("owner_def_id",),
            dst_cols=("type_id",),
            origin="inferred",
            default_resolution_method="ANNOTATION_INFER",
        ),
    ),
    _edge_spec(
        name="runtime_signatures",
        option_flag="emit_runtime_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.RT_HAS_SIGNATURE,
            src_cols=("rt_id",),
            dst_cols=("sig_id",),
            origin="inspect",
            default_resolution_method="RUNTIME_INSPECT",
        ),
    ),
    _edge_spec(
        name="runtime_params",
        option_flag="emit_runtime_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.RT_HAS_PARAM,
            src_cols=("sig_id",),
            dst_cols=("param_id",),
            origin="inspect",
            default_resolution_method="RUNTIME_INSPECT",
        ),
    ),
    _edge_spec(
        name="runtime_members",
        option_flag="emit_runtime_edges",
        emit=EdgeEmitSpec(
            edge_kind=EdgeKind.RT_HAS_MEMBER,
            src_cols=("rt_id",),
            dst_cols=("member_id",),
            origin="inspect",
            default_resolution_method="RUNTIME_INSPECT",
        ),
    ),
)


@cache
def relation_rule_table_cached() -> pa.Table:
    """Return the relationship rule table for CPG edges.

    Returns
    -------
    pa.Table
        Arrow table of relationship rules.
    """
    return relationship_rule_table(RELATION_RULES)


def relation_rules() -> tuple[RelationshipRule, ...]:
    """Return relationship rules parsed from the rule table.

    Returns
    -------
    tuple[RelationshipRule, ...]
        Relationship rule definitions.
    """
    return relationship_rules_from_table(relation_rule_table_cached())


def edge_plan_specs() -> tuple[EdgePlanSpec, ...]:
    """Return edge plan specs for CPG edge emission.

    Returns
    -------
    tuple[EdgePlanSpec, ...]
        Edge plan specs for edge emission.
    """
    return EDGE_PLAN_SPECS


def _apply_query_spec(plan: Plan, spec: QuerySpec, *, ctx: ExecutionContext) -> Plan:
    exprs = [FieldExpr(name=col).to_expression() for col in spec.projection.base]
    names = list(spec.projection.base)
    for name, expr_spec in spec.projection.derived.items():
        exprs.append(expr_spec.to_expression())
        names.append(name)
    if exprs:
        plan = plan.project(exprs, names, label=plan.label or "")
    predicate = spec.predicate_expression()
    if predicate is not None:
        plan = plan.filter(predicate, ctx=ctx)
    pushdown = spec.pushdown_expression()
    if pushdown is not None:
        plan = plan.filter(pushdown, ctx=ctx)
    return plan


class CatalogPlanResolver:
    """Resolve dataset refs using the CPG plan catalog."""

    def __init__(self, catalog: PlanCatalog) -> None:
        self._catalog = catalog

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> Plan:
        """Resolve a DatasetRef into a Plan.

        Returns
        -------
        Plan
            Plan for the dataset reference.

        Raises
        ------
        KeyError
            Raised when the dataset reference is not found in the catalog.
        """
        source = resolve_plan_source(self._catalog, ref.name, ctx=ctx)
        if source is None:
            msg = f"Unknown dataset reference: {ref.name!r}."
            raise KeyError(msg)
        plan = ensure_plan(source, label=ref.label or ref.name, ctx=ctx)
        if ref.query is None:
            return plan
        return _apply_query_spec(plan, ref.query, ctx=ctx)


def _apply_rule_meta(plan: Plan, rule: RelationshipRule, *, ctx: ExecutionContext) -> Plan:
    if not rule.emit_rule_meta:
        return plan
    schema = plan.schema(ctx=ctx)
    names = list(schema.names)
    exprs = [FieldExpr(name=name).to_expression() for name in names]
    if rule.rule_name_col not in names:
        exprs.append(ConstExpr(value=rule.name).to_expression())
        names.append(rule.rule_name_col)
    if rule.rule_priority_col not in names:
        exprs.append(ConstExpr(value=int(rule.priority)).to_expression())
        names.append(rule.rule_priority_col)
    return plan.project(exprs, names, label=plan.label or rule.name)


def compile_relation_plans(
    catalog: PlanCatalog,
    *,
    ctx: ExecutionContext,
    rule_table: pa.Table | None = None,
) -> dict[str, Plan]:
    """Compile relation rules into plans keyed by output dataset name.

    Returns
    -------
    dict[str, Plan]
        Mapping of output dataset names to relation plans.
    """
    rules = relationship_rules_from_table(rule_table or relation_rule_table_cached())
    resolver = CatalogPlanResolver(catalog)
    compiler = RelationshipRuleCompiler(resolver=resolver)
    plans: dict[str, Plan] = {}
    for rule in rules:
        compiled = compiler.compile_rule(rule, ctx=ctx)
        if compiled.plan is not None and not compiled.post_kernels:
            plans[rule.output_dataset] = _apply_rule_meta(compiled.plan, rule, ctx=ctx)
            continue
        table = compiled.execute(ctx=ctx, resolver=resolver)
        plans[rule.output_dataset] = Plan.table_source(table, label=rule.name)
    return plans


__all__ = [
    "EDGE_PLAN_SPECS",
    "RELATION_RULES",
    "CatalogPlanResolver",
    "compile_relation_plans",
    "edge_plan_specs",
    "relation_rule_table_cached",
    "relation_rules",
]
