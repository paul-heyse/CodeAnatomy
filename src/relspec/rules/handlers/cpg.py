"""CPG relationship rule handler for centralized compilation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from arrowdsl.core.context import ExecutionContext
from relspec.model import DatasetRef, FilterKernelSpec, RelationshipRule, RuleKind
from relspec.model import EvidenceSpec as RelationshipEvidenceSpec
from relspec.rules.compiler import RuleHandler
from relspec.rules.definitions import RelationshipPayload
from relspec.rules.policies import PolicyRegistry

if TYPE_CHECKING:
    from relspec.rules.definitions import EvidenceSpec, RuleDefinition, RuleDomain


@dataclass(frozen=True)
class RelationshipRuleHandler(RuleHandler):
    """Compile relationship rule definitions into RelationshipRule objects."""

    policies: PolicyRegistry = field(default_factory=PolicyRegistry)
    domain: RuleDomain = "cpg"

    def compile_rule(self, rule: RuleDefinition, *, ctx: ExecutionContext) -> RelationshipRule:
        """Compile a relationship rule definition into a RelationshipRule.

        Returns
        -------
        RelationshipRule
            Relationship rule instance.
        """
        _ = ctx
        return relationship_rule_from_definition(rule, policies=self.policies)


def relationship_rule_from_definition(
    rule: RuleDefinition, *, policies: PolicyRegistry | None = None
) -> RelationshipRule:
    """Convert a centralized rule definition into a RelationshipRule.

    Returns
    -------
    RelationshipRule
        Relationship rule built from the definition.

    Raises
    ------
    TypeError
        Raised when the rule payload is not a relationship payload.
    """
    policies = policies or PolicyRegistry()
    payload = rule.payload
    if not isinstance(payload, RelationshipPayload):
        msg = f"RuleDefinition {rule.name!r} missing relationship payload."
        raise TypeError(msg)
    inputs = tuple(DatasetRef(name=name) for name in rule.inputs)
    post_kernels = rule.post_kernels
    if payload.predicate is not None:
        post_kernels = (FilterKernelSpec(predicate=payload.predicate), *post_kernels)
    confidence_policy = policies.resolve_confidence("cpg", rule.policy_overrides.confidence_policy)
    ambiguity_policy = policies.resolve_ambiguity("cpg", rule.policy_overrides.ambiguity_policy)
    return RelationshipRule(
        name=rule.name,
        kind=RuleKind(rule.kind),
        output_dataset=payload.output_dataset or rule.output,
        contract_name=payload.contract_name,
        inputs=inputs,
        hash_join=payload.hash_join,
        interval_align=payload.interval_align,
        winner_select=payload.winner_select,
        project=payload.project,
        post_kernels=post_kernels,
        priority=rule.priority,
        emit_rule_meta=rule.emit_rule_meta,
        rule_name_col=payload.rule_name_col,
        rule_priority_col=payload.rule_priority_col,
        execution_mode=rule.execution_mode,
        evidence=_relationship_evidence(rule.evidence),
        confidence_policy=confidence_policy,
        ambiguity_policy=ambiguity_policy,
    )


def _relationship_evidence(spec: EvidenceSpec | None) -> RelationshipEvidenceSpec | None:
    if spec is None:
        return None
    return RelationshipEvidenceSpec(
        sources=spec.sources,
        required_columns=spec.required_columns,
        required_types=spec.required_types,
    )


__all__ = ["RelationshipRuleHandler", "relationship_rule_from_definition"]
