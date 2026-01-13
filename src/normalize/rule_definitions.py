"""Normalize rule definition specs and compilation helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from arrowdsl.plan.query import QuerySpec
from normalize.plan_builders import resolve_plan_builder
from normalize.policy_registry import resolve_ambiguity_policy, resolve_confidence_policy
from normalize.rule_model import NormalizeRule

if TYPE_CHECKING:
    from normalize.rule_model import EvidenceOutput, EvidenceSpec, ExecutionMode


@dataclass(frozen=True)
class NormalizeRuleDefinition:
    """Table-driven normalize rule definition."""

    name: str
    output: str
    inputs: tuple[str, ...] = ()
    plan_builder: str | None = None
    execution_mode: ExecutionMode = "auto"
    query: QuerySpec | None = None
    evidence: EvidenceSpec | None = None
    evidence_output: EvidenceOutput | None = None
    confidence_policy: str | None = None
    ambiguity_policy: str | None = None
    priority: int = 100
    emit_rule_meta: bool = True


def build_rule_from_definition(definition: NormalizeRuleDefinition) -> NormalizeRule:
    """Return a NormalizeRule built from a definition spec.

    Returns
    -------
    NormalizeRule
        Normalized rule instance.
    """
    derive = resolve_plan_builder(definition.plan_builder) if definition.plan_builder else None
    confidence_policy = (
        resolve_confidence_policy(definition.confidence_policy)
        if definition.confidence_policy
        else None
    )
    ambiguity_policy = (
        resolve_ambiguity_policy(definition.ambiguity_policy)
        if definition.ambiguity_policy
        else None
    )
    return NormalizeRule(
        name=definition.name,
        output=definition.output,
        inputs=definition.inputs,
        derive=derive,
        query=definition.query,
        evidence=definition.evidence,
        evidence_output=definition.evidence_output,
        confidence_policy=confidence_policy,
        ambiguity_policy=ambiguity_policy,
        priority=definition.priority,
        emit_rule_meta=definition.emit_rule_meta,
        execution_mode=definition.execution_mode,
    )


def build_rules_from_definitions(
    definitions: Sequence[NormalizeRuleDefinition],
) -> tuple[NormalizeRule, ...]:
    """Build normalize rules from rule definition specs.

    Returns
    -------
    tuple[NormalizeRule, ...]
        Normalize rules derived from the definitions.
    """
    return tuple(build_rule_from_definition(definition) for definition in definitions)


__all__ = [
    "NormalizeRuleDefinition",
    "build_rule_from_definition",
    "build_rules_from_definitions",
]
