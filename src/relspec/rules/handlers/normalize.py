"""Normalize rule handler for centralized compilation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from arrowdsl.core.context import ExecutionContext
from arrowdsl.plan.catalog import PlanDeriver
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.spec.expr_ir import ExprIR
from ibis_engine.query_compiler import IbisQuerySpec
from normalize.plan_builders import resolve_plan_builder
from normalize.rule_defaults import apply_rule_defaults
from normalize.rule_model import (
    EvidenceOutput as NormalizeEvidenceOutput,
)
from normalize.rule_model import (
    EvidenceSpec as NormalizeEvidenceSpec,
)
from normalize.rule_model import NormalizeRule
from relspec.rules.compiler import RuleHandler
from relspec.rules.definitions import NormalizePayload
from relspec.rules.policies import PolicyRegistry
from relspec.rules.rel_ops import query_spec_from_rel_ops

if TYPE_CHECKING:
    from relspec.rules.definitions import EvidenceOutput, EvidenceSpec, RuleDefinition, RuleDomain
    from relspec.rules.rel_ops import RelOpT


@dataclass(frozen=True)
class NormalizeRuleHandler(RuleHandler):
    """Convert centralized rule definitions into normalize rule objects."""

    policies: PolicyRegistry = field(default_factory=PolicyRegistry)
    domain: RuleDomain = "normalize"

    def compile_rule(self, rule: RuleDefinition, *, ctx: ExecutionContext) -> NormalizeRule:
        """Compile a normalize rule definition.

        Returns
        -------
        NormalizeRule
            Normalize rule derived from the definition.
        """
        payload = rule.payload
        query = _normalize_query(payload, rel_ops=rule.rel_ops)
        derive = _normalize_derive(payload)
        base = NormalizeRule(
            name=rule.name,
            output=rule.output,
            inputs=rule.inputs,
            derive=derive,
            query=query,
            evidence=_normalize_evidence(rule.evidence),
            evidence_output=_normalize_evidence_output(rule.evidence_output, ctx=ctx),
            confidence_policy=self.policies.resolve_confidence(
                "normalize", rule.policy_overrides.confidence_policy
            ),
            ambiguity_policy=self.policies.resolve_ambiguity(
                "normalize", rule.policy_overrides.ambiguity_policy
            ),
            priority=rule.priority,
            emit_rule_meta=rule.emit_rule_meta,
            execution_mode=rule.execution_mode,
        )
        return apply_rule_defaults(base, registry=self.policies)


def _normalize_query(payload: object | None, *, rel_ops: tuple[RelOpT, ...]) -> QuerySpec | None:
    if isinstance(payload, NormalizePayload) and payload.query is not None:
        return _query_spec_for_plan(payload.query)
    if rel_ops:
        query = query_spec_from_rel_ops(rel_ops)
        return _query_spec_for_plan(query) if query is not None else None
    return None


def _query_spec_for_plan(spec: IbisQuerySpec) -> QuerySpec:
    projection = ProjectionSpec(
        base=spec.projection.base,
        derived={
            name: cast("ExprIR", expr).to_expr_spec()
            for name, expr in spec.projection.derived.items()
        },
    )
    predicate = (
        cast("ExprIR", spec.predicate).to_expr_spec() if spec.predicate is not None else None
    )
    pushdown = (
        cast("ExprIR", spec.pushdown_predicate).to_expr_spec()
        if spec.pushdown_predicate is not None
        else None
    )
    return QuerySpec(
        projection=projection,
        predicate=predicate,
        pushdown_predicate=pushdown,
    )


def _normalize_derive(payload: object | None) -> PlanDeriver | None:
    if isinstance(payload, NormalizePayload) and payload.plan_builder:
        return resolve_plan_builder(payload.plan_builder)
    return None


def _normalize_evidence(spec: EvidenceSpec | None) -> NormalizeEvidenceSpec | None:
    if spec is None:
        return None
    return NormalizeEvidenceSpec(
        sources=spec.sources,
        required_columns=spec.required_columns,
        required_types=spec.required_types,
        required_metadata=spec.required_metadata,
    )


def _normalize_evidence_output(
    spec: EvidenceOutput | None,
    *,
    ctx: ExecutionContext,
) -> NormalizeEvidenceOutput | None:
    if spec is None and not ctx.runtime.scan.scan_provenance_columns:
        return None
    column_map = dict(spec.column_map) if spec is not None else {}
    literals = dict(spec.literals) if spec is not None else {}
    provenance_columns = (
        tuple(
            dict.fromkeys(
                (
                    *(spec.provenance_columns if spec is not None else ()),
                    *ctx.runtime.scan.scan_provenance_columns,
                )
            )
        )
        if spec is not None or ctx.runtime.scan.scan_provenance_columns
        else ()
    )
    return NormalizeEvidenceOutput(
        column_map=column_map,
        literals=literals,
        provenance_columns=provenance_columns,
    )


__all__ = ["NormalizeRuleHandler"]
