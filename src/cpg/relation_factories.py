"""Relationship rule and edge plan factories for CPG edges."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.compute.expr_core import ScalarValue
from arrowdsl.spec.expr_ir import ExprIR
from cpg.kinds_ultimate import (
    SCIP_ROLE_DEFINITION,
    SCIP_ROLE_IMPORT,
    SCIP_ROLE_READ,
    SCIP_ROLE_WRITE,
    EdgeKind,
)
from cpg.specs import EdgeEmitSpec, EdgePlanSpec
from relspec.contracts import RELATION_OUTPUT_NAME
from relspec.model import (
    AddLiteralSpec,
    AmbiguityPolicy,
    ConfidencePolicy,
    DatasetRef,
    EvidenceSpec,
    FilterKernelSpec,
    HashJoinConfig,
    KernelSpecT,
    ProjectConfig,
    RelationshipRule,
    RuleFamilySpec,
    RuleKind,
)
from relspec.policies import ambiguity_kernels, confidence_expr
from relspec.policy_registry import resolve_ambiguity_policy, resolve_confidence_policy


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
    hit = _fill_null(hit, value=False)
    if invert:
        return _call("invert", hit)
    return hit


def _flag_predicate(field_name: str) -> ExprIR:
    return _fill_null(_field(field_name), value=False)


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


@dataclass(frozen=True)
class FilterRuleConfig:
    """Configuration for filter/project relationship rules."""

    name: str
    input_name: str
    predicate: ExprIR | None = None
    project: ProjectConfig | None = None
    post_kernels: Sequence[AddLiteralSpec] = ()
    contract_name: str = RELATION_OUTPUT_NAME
    confidence_policy: ConfidencePolicy | None = None
    ambiguity_policy: AmbiguityPolicy | None = None


def _with_confidence(
    project: ProjectConfig,
    policy: ConfidencePolicy | None,
    *,
    score_expr: ExprIR | None = None,
    source_field: str | None = None,
) -> ProjectConfig:
    if policy is None and score_expr is None:
        return project
    expr = score_expr or confidence_expr(policy or ConfidencePolicy(), source_field=source_field)
    exprs = dict(project.exprs)
    exprs.setdefault("confidence", expr)
    exprs.setdefault("score", expr)
    return ProjectConfig(select=project.select, exprs=exprs)


def _filter_rule(config: FilterRuleConfig) -> RelationshipRule:
    kernels: list[KernelSpecT] = []
    if config.predicate is not None:
        kernels.append(FilterKernelSpec(predicate=config.predicate))
    kernels.extend(config.post_kernels)
    kernels.extend(ambiguity_kernels(config.ambiguity_policy))
    inputs = (DatasetRef(name=config.input_name),)
    evidence = EvidenceSpec(sources=(config.input_name,))
    return RelationshipRule(
        name=config.name,
        kind=RuleKind.FILTER_PROJECT,
        output_dataset=config.name,
        contract_name=config.contract_name,
        inputs=inputs,
        project=_with_confidence(config.project or _project(), config.confidence_policy),
        post_kernels=tuple(kernels),
        evidence=evidence,
        confidence_policy=config.confidence_policy,
        ambiguity_policy=config.ambiguity_policy,
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


def _confidence_policy(spec: RuleFamilySpec) -> ConfidencePolicy | None:
    return resolve_confidence_policy(spec.confidence_policy)


def _ambiguity_policy(spec: RuleFamilySpec) -> AmbiguityPolicy | None:
    return resolve_ambiguity_policy(spec.ambiguity_policy)


def _require_inputs(spec: RuleFamilySpec, *, count: int) -> tuple[str, ...]:
    if len(spec.inputs) != count:
        msg = f"RuleFamilySpec {spec.name!r} requires {count} inputs."
        raise ValueError(msg)
    return spec.inputs


def build_symbol_role_rules(spec: RuleFamilySpec) -> tuple[RelationshipRule, ...]:
    input_name = _require_inputs(spec, count=1)[0]
    ambiguity = _ambiguity_policy(spec)
    base_project = _project(
        select=("name_ref_id", "symbol", "path", "bstart", "bend", "symbol_roles"),
    )
    return (
        _filter_rule(
            FilterRuleConfig(
                name="symbol_role_defines",
                input_name=input_name,
                predicate=_bitmask_predicate("symbol_roles", SCIP_ROLE_DEFINITION),
                project=base_project,
                confidence_policy=_confidence_policy(spec),
                ambiguity_policy=ambiguity,
            )
        ),
        _filter_rule(
            FilterRuleConfig(
                name="symbol_role_references",
                input_name=input_name,
                predicate=_bitmask_predicate("symbol_roles", SCIP_ROLE_DEFINITION, invert=True),
                project=base_project,
                confidence_policy=_confidence_policy(spec),
                ambiguity_policy=ambiguity,
            )
        ),
        _filter_rule(
            FilterRuleConfig(
                name="symbol_role_reads",
                input_name=input_name,
                predicate=_bitmask_predicate("symbol_roles", SCIP_ROLE_READ),
                project=base_project,
                confidence_policy=_confidence_policy(spec),
                ambiguity_policy=ambiguity,
            )
        ),
        _filter_rule(
            FilterRuleConfig(
                name="symbol_role_writes",
                input_name=input_name,
                predicate=_bitmask_predicate("symbol_roles", SCIP_ROLE_WRITE),
                project=base_project,
                confidence_policy=_confidence_policy(spec),
                ambiguity_policy=ambiguity,
            )
        ),
    )


def build_scip_symbol_rules(spec: RuleFamilySpec) -> tuple[RelationshipRule, ...]:
    input_name = _require_inputs(spec, count=1)[0]
    confidence = _confidence_policy(spec)
    ambiguity = _ambiguity_policy(spec)
    return (
        _filter_rule(
            FilterRuleConfig(
                name="scip_symbol_reference",
                input_name=input_name,
                predicate=_flag_predicate("is_reference"),
                project=_project(select=("symbol", "related_symbol", "is_reference")),
                confidence_policy=confidence,
                ambiguity_policy=ambiguity,
            )
        ),
        _filter_rule(
            FilterRuleConfig(
                name="scip_symbol_implementation",
                input_name=input_name,
                predicate=_flag_predicate("is_implementation"),
                project=_project(select=("symbol", "related_symbol", "is_implementation")),
                confidence_policy=confidence,
                ambiguity_policy=ambiguity,
            )
        ),
        _filter_rule(
            FilterRuleConfig(
                name="scip_symbol_type_definition",
                input_name=input_name,
                predicate=_flag_predicate("is_type_definition"),
                project=_project(select=("symbol", "related_symbol", "is_type_definition")),
                confidence_policy=confidence,
                ambiguity_policy=ambiguity,
            )
        ),
        _filter_rule(
            FilterRuleConfig(
                name="scip_symbol_definition",
                input_name=input_name,
                predicate=_flag_predicate("is_definition"),
                project=_project(select=("symbol", "related_symbol", "is_definition")),
                confidence_policy=confidence,
                ambiguity_policy=ambiguity,
            )
        ),
    )


def build_import_rules(spec: RuleFamilySpec) -> tuple[RelationshipRule, ...]:
    input_name = _require_inputs(spec, count=1)[0]
    confidence = _confidence_policy(spec)
    ambiguity = _ambiguity_policy(spec)
    return (
        _filter_rule(
            FilterRuleConfig(
                name="import_edges",
                input_name=input_name,
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
                confidence_policy=confidence,
                ambiguity_policy=ambiguity,
            )
        ),
    )


def build_call_rules(spec: RuleFamilySpec) -> tuple[RelationshipRule, ...]:
    input_name = _require_inputs(spec, count=1)[0]
    confidence = _confidence_policy(spec)
    ambiguity = _ambiguity_policy(spec)
    return (
        _filter_rule(
            FilterRuleConfig(
                name="call_edges",
                input_name=input_name,
                project=_project(
                    select=(
                        "call_id",
                        "symbol",
                        "path",
                        "call_bstart",
                        "call_bend",
                        "bstart",
                        "bend",
                    )
                ),
                confidence_policy=confidence,
                ambiguity_policy=ambiguity,
            )
        ),
    )


def build_qname_fallback_rules(spec: RuleFamilySpec) -> tuple[RelationshipRule, ...]:
    left, right = _require_inputs(spec, count=2)
    project = _project(
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
    )
    return (
        RelationshipRule(
            name="qname_fallback_calls",
            kind=RuleKind.HASH_JOIN,
            output_dataset="qname_fallback_calls",
            contract_name=RELATION_OUTPUT_NAME,
            inputs=(DatasetRef(name=left), DatasetRef(name=right)),
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
            project=_with_confidence(project, _confidence_policy(spec)),
            post_kernels=ambiguity_kernels(_ambiguity_policy(spec)),
            evidence=EvidenceSpec(sources=(left, right)),
            confidence_policy=_confidence_policy(spec),
            ambiguity_policy=_ambiguity_policy(spec),
        ),
    )


def build_diagnostic_rules(spec: RuleFamilySpec) -> tuple[RelationshipRule, ...]:
    input_name = _require_inputs(spec, count=1)[0]
    ambiguity = _ambiguity_policy(spec)
    base_project = _project(
        select=("diag_id", "file_id", "path", "bstart", "bend", "severity"),
        exprs={
            "origin": _fill_null(_field("diag_source"), "diagnostic"),
            "confidence": _severity_score_expr(),
            "score": _severity_score_expr(),
        },
    )
    return (
        _filter_rule(
            FilterRuleConfig(
                name="diagnostic_edges",
                input_name=input_name,
                project=base_project,
                confidence_policy=None,
                ambiguity_policy=ambiguity,
            )
        ),
    )


def build_type_rules(spec: RuleFamilySpec) -> tuple[RelationshipRule, ...]:
    input_name = _require_inputs(spec, count=1)[0]
    confidence = _confidence_policy(spec)
    ambiguity = _ambiguity_policy(spec)
    return (
        _filter_rule(
            FilterRuleConfig(
                name="type_annotation_edges",
                input_name=input_name,
                project=_project(select=("owner_def_id", "type_expr_id", "path", "bstart", "bend")),
                post_kernels=(
                    AddLiteralSpec(name="confidence", value=pa.scalar(1.0, type=pa.float32())),
                    AddLiteralSpec(name="score", value=pa.scalar(1.0, type=pa.float32())),
                ),
                confidence_policy=confidence,
                ambiguity_policy=ambiguity,
            )
        ),
        _filter_rule(
            FilterRuleConfig(
                name="inferred_type_edges",
                input_name=input_name,
                project=_project(select=("owner_def_id", "type_id")),
                post_kernels=(
                    AddLiteralSpec(name="path", value=pa.scalar(None, type=pa.string())),
                    AddLiteralSpec(name="bstart", value=pa.scalar(None, type=pa.int64())),
                    AddLiteralSpec(name="bend", value=pa.scalar(None, type=pa.int64())),
                    AddLiteralSpec(name="confidence", value=pa.scalar(1.0, type=pa.float32())),
                    AddLiteralSpec(name="score", value=pa.scalar(1.0, type=pa.float32())),
                ),
                confidence_policy=confidence,
                ambiguity_policy=ambiguity,
            )
        ),
    )


def build_runtime_rules(spec: RuleFamilySpec) -> tuple[RelationshipRule, ...]:
    signatures, params, members = _require_inputs(spec, count=3)
    confidence = _confidence_policy(spec)
    ambiguity = _ambiguity_policy(spec)
    base_kernels = (
        AddLiteralSpec(name="path", value=pa.scalar(None, type=pa.string())),
        AddLiteralSpec(name="bstart", value=pa.scalar(None, type=pa.int64())),
        AddLiteralSpec(name="bend", value=pa.scalar(None, type=pa.int64())),
        AddLiteralSpec(name="confidence", value=pa.scalar(1.0, type=pa.float32())),
        AddLiteralSpec(name="score", value=pa.scalar(1.0, type=pa.float32())),
    )
    return (
        _filter_rule(
            FilterRuleConfig(
                name="runtime_signatures",
                input_name=signatures,
                project=_project(select=("rt_id", "sig_id")),
                post_kernels=base_kernels,
                confidence_policy=confidence,
                ambiguity_policy=ambiguity,
            )
        ),
        _filter_rule(
            FilterRuleConfig(
                name="runtime_params",
                input_name=params,
                project=_project(select=("sig_id", "param_id")),
                post_kernels=base_kernels,
                confidence_policy=confidence,
                ambiguity_policy=ambiguity,
            )
        ),
        _filter_rule(
            FilterRuleConfig(
                name="runtime_members",
                input_name=members,
                project=_project(select=("rt_id", "member_id")),
                post_kernels=base_kernels,
                confidence_policy=confidence,
                ambiguity_policy=ambiguity,
            )
        ),
    )


RuleFamilyFactory = Callable[[RuleFamilySpec], tuple[RelationshipRule, ...]]

RULE_FAMILY_FACTORIES: dict[str, RuleFamilyFactory] = {
    "symbol_role": build_symbol_role_rules,
    "scip_symbol": build_scip_symbol_rules,
    "import": build_import_rules,
    "call": build_call_rules,
    "qname_fallback": build_qname_fallback_rules,
    "diagnostic": build_diagnostic_rules,
    "type": build_type_rules,
    "runtime": build_runtime_rules,
}


def build_rules_from_specs(specs: Sequence[RuleFamilySpec]) -> tuple[RelationshipRule, ...]:
    """Return relationship rules generated from family specs.

    Parameters
    ----------
    specs:
        Rule-family specs to expand into rules.

    Returns
    -------
    tuple[RelationshipRule, ...]
        Generated relationship rules.

    Raises
    ------
    ValueError
        Raised when a spec references an unknown factory.
    """
    rules: list[RelationshipRule] = []
    for spec in specs:
        factory = RULE_FAMILY_FACTORIES.get(spec.factory)
        if factory is None:
            msg = f"Unknown rule family factory: {spec.factory!r}."
            raise ValueError(msg)
        rules.extend(factory(spec))
    return tuple(rules)


def relation_rules(specs: Sequence[RuleFamilySpec]) -> tuple[RelationshipRule, ...]:
    """Return the full set of relationship rules.

    Parameters
    ----------
    specs:
        Rule-family specs to expand into rules.

    Returns
    -------
    tuple[RelationshipRule, ...]
        Relationship rules derived from specs.
    """
    return build_rules_from_specs(specs)


def edge_plan_specs() -> tuple[EdgePlanSpec, ...]:
    """Return edge plan specs for CPG edge emission.

    Returns
    -------
    tuple[EdgePlanSpec, ...]
        Edge plan specs for CPG edge emission.
    """
    return (
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


__all__ = ["RULE_FAMILY_FACTORIES", "build_rules_from_specs", "edge_plan_specs", "relation_rules"]
