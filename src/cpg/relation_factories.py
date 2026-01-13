"""Relationship rule and edge plan factories for CPG edges."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from arrowdsl.spec.expr_ir import ExprIR
from cpg.relation_template_specs import RuleDefinitionSpec
from cpg.specs import EdgeEmitSpec, EdgePlanSpec
from relspec.contracts import RELATION_OUTPUT_NAME
from relspec.model import (
    AmbiguityPolicy,
    ConfidencePolicy,
    DatasetRef,
    EvidenceSpec,
    FilterKernelSpec,
    KernelSpecT,
    ProjectConfig,
    RelationshipRule,
    RuleKind,
)
from relspec.policies import ambiguity_kernels, confidence_expr
from relspec.policy_registry import resolve_ambiguity_policy, resolve_confidence_policy


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


def _post_kernels(
    spec: RuleDefinitionSpec,
    ambiguity_policy: AmbiguityPolicy | None,
) -> tuple[KernelSpecT, ...]:
    kernels: list[KernelSpecT] = []
    if spec.predicate is not None:
        kernels.append(FilterKernelSpec(predicate=spec.predicate))
    kernels.extend(spec.post_kernels)
    kernels.extend(ambiguity_kernels(ambiguity_policy))
    return tuple(kernels)


def _expr_fields(expr: ExprIR | None) -> set[str]:
    if expr is None:
        return set()
    if expr.op == "field":
        return {expr.name} if expr.name else set()
    fields: set[str] = set()
    for arg in expr.args:
        fields.update(_expr_fields(arg))
    return fields


def _exprs_fields(exprs: Mapping[str, ExprIR]) -> set[str]:
    fields: set[str] = set()
    for expr in exprs.values():
        fields.update(_expr_fields(expr))
    return fields


def _project_required_columns(project: ProjectConfig | None) -> set[str]:
    if project is None:
        return set()
    required = set(project.select)
    required.update(_exprs_fields(project.exprs))
    return required


def _derived_required_columns(spec: RuleDefinitionSpec) -> set[str]:
    if spec.kind != RuleKind.FILTER_PROJECT or len(spec.inputs) != 1:
        return set()
    required = _project_required_columns(spec.project)
    required.update(_expr_fields(spec.predicate))
    return required


def _resolve_evidence_spec(spec: RuleDefinitionSpec) -> EvidenceSpec | None:
    sources = spec.evidence.sources if spec.evidence is not None else spec.inputs
    required_columns = spec.evidence.required_columns if spec.evidence is not None else ()
    required_types = dict(spec.evidence.required_types) if spec.evidence is not None else {}
    derived = _derived_required_columns(spec)
    if derived:
        required_columns = tuple(sorted(set(required_columns).union(derived)))
    if not sources and not required_columns and not required_types:
        return None
    return EvidenceSpec(
        sources=tuple(sources),
        required_columns=tuple(sorted(set(required_columns))),
        required_types=required_types,
    )


def build_rules_from_definitions(
    specs: Sequence[RuleDefinitionSpec],
) -> tuple[RelationshipRule, ...]:
    """Build relationship rules from definition specs.

    Parameters
    ----------
    specs:
        Definition specs to compile into rules.

    Returns
    -------
    tuple[RelationshipRule, ...]
        Relationship rules for compilation.
    """
    rules: list[RelationshipRule] = []
    for spec in specs:
        confidence_policy = resolve_confidence_policy(spec.confidence_policy)
        ambiguity_policy = resolve_ambiguity_policy(spec.ambiguity_policy)
        project = spec.project
        if project is None and confidence_policy is not None:
            project = ProjectConfig()
        if project is not None:
            project = _with_confidence(project, confidence_policy)
        post_kernels = _post_kernels(spec, ambiguity_policy)
        inputs = tuple(DatasetRef(name=name) for name in spec.inputs)
        evidence = _resolve_evidence_spec(spec)
        rules.append(
            RelationshipRule(
                name=spec.name,
                kind=spec.kind,
                output_dataset=spec.output_dataset or spec.name,
                contract_name=spec.contract_name or RELATION_OUTPUT_NAME,
                inputs=inputs,
                hash_join=spec.hash_join,
                interval_align=spec.interval_align,
                winner_select=spec.winner_select,
                project=project,
                post_kernels=post_kernels,
                evidence=evidence,
                confidence_policy=confidence_policy,
                ambiguity_policy=ambiguity_policy,
                priority=spec.priority,
                emit_rule_meta=spec.emit_rule_meta,
                rule_name_col=spec.rule_name_col,
                rule_priority_col=spec.rule_priority_col,
                execution_mode=spec.execution_mode,
            )
        )
    return tuple(rules)


def edge_plan_specs_from_definitions(
    specs: Sequence[RuleDefinitionSpec],
) -> tuple[EdgePlanSpec, ...]:
    """Return edge plan specs derived from rule definitions.

    Parameters
    ----------
    specs:
        Rule definition specs with optional edge metadata.

    Returns
    -------
    tuple[EdgePlanSpec, ...]
        Edge plan specs for CPG emission.
    """
    plans: list[EdgePlanSpec] = []
    for spec in specs:
        edge = spec.edge
        if edge is None:
            continue
        plans.append(
            EdgePlanSpec(
                name=spec.name,
                option_flag=edge.option_flag,
                relation_ref=spec.name,
                emit=EdgeEmitSpec(
                    edge_kind=edge.edge_kind,
                    src_cols=edge.src_cols,
                    dst_cols=edge.dst_cols,
                    origin=edge.origin,
                    default_resolution_method=edge.resolution_method,
                    path_cols=edge.path_cols,
                    bstart_cols=edge.bstart_cols,
                    bend_cols=edge.bend_cols,
                ),
            )
        )
    return tuple(plans)


__all__ = ["build_rules_from_definitions", "edge_plan_specs_from_definitions"]
