"""Default policy and evidence resolution for normalize rules."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from arrowdsl.core.interop import SchemaLike
from normalize.evidence_specs import evidence_output_from_schema, evidence_spec_from_schema
from normalize.policies import (
    ambiguity_policy_from_schema,
    confidence_policy_from_schema,
    default_tie_breakers,
)
from normalize.registry_specs import dataset_schema
from relspec.normalize.rule_model import (
    AmbiguityPolicy,
    EvidenceOutput,
    EvidenceSpec,
    NormalizeRule,
)
from relspec.rules.policies import PolicyRegistry


def apply_rule_defaults(
    rule: NormalizeRule,
    *,
    registry: PolicyRegistry,
) -> NormalizeRule:
    """Apply policy and evidence defaults to a normalize rule.

    Returns
    -------
    NormalizeRule
        Rule with policy and evidence defaults applied.
    """
    updated = _apply_policy_defaults(rule, registry=registry)
    return _apply_evidence_defaults(updated)


def apply_policy_defaults(
    rules: Sequence[NormalizeRule],
    *,
    registry: PolicyRegistry,
) -> tuple[NormalizeRule, ...]:
    """Apply policy defaults to a sequence of normalize rules.

    Returns
    -------
    tuple[NormalizeRule, ...]
        Rules with policy defaults applied.
    """
    return tuple(_apply_policy_defaults(rule, registry=registry) for rule in rules)


def apply_evidence_defaults(rules: Sequence[NormalizeRule]) -> tuple[NormalizeRule, ...]:
    """Apply evidence defaults to a sequence of normalize rules.

    Returns
    -------
    tuple[NormalizeRule, ...]
        Rules with evidence defaults applied.
    """
    return tuple(_apply_evidence_defaults(rule) for rule in rules)


def _apply_policy_defaults(
    rule: NormalizeRule,
    *,
    registry: PolicyRegistry,
) -> NormalizeRule:
    schema = dataset_schema(rule.output)
    confidence = rule.confidence_policy or confidence_policy_from_schema(
        schema,
        registry=registry,
    )
    ambiguity = rule.ambiguity_policy or ambiguity_policy_from_schema(
        schema,
        registry=registry,
    )
    if ambiguity is not None:
        ambiguity = _apply_default_tie_breakers(ambiguity, schema=schema)
    if confidence == rule.confidence_policy and ambiguity == rule.ambiguity_policy:
        return rule
    return replace(
        rule,
        confidence_policy=confidence,
        ambiguity_policy=ambiguity,
    )


def _apply_evidence_defaults(rule: NormalizeRule) -> NormalizeRule:
    schema = dataset_schema(rule.output)
    evidence_defaults = evidence_spec_from_schema(schema)
    output_defaults = evidence_output_from_schema(schema)
    evidence = _merge_evidence(rule.evidence, evidence_defaults)
    evidence_output = _merge_evidence_output(rule.evidence_output, output_defaults)
    if evidence == rule.evidence and evidence_output == rule.evidence_output:
        return rule
    return replace(rule, evidence=evidence, evidence_output=evidence_output)


def _apply_default_tie_breakers(
    policy: AmbiguityPolicy,
    *,
    schema: SchemaLike,
) -> AmbiguityPolicy:
    if policy.winner_select is None:
        return policy
    if policy.tie_breakers or policy.winner_select.tie_breakers:
        return policy
    defaults = default_tie_breakers(schema)
    if not defaults:
        return policy
    return replace(policy, tie_breakers=defaults)


def _merge_evidence(
    base: EvidenceSpec | None,
    defaults: EvidenceSpec | None,
) -> EvidenceSpec | None:
    if base is None:
        return defaults
    if defaults is None:
        return base
    sources = base.sources or defaults.sources
    required_columns = tuple(sorted(set(base.required_columns).union(defaults.required_columns)))
    required_types = dict(defaults.required_types)
    required_types.update(base.required_types)
    required_metadata = dict(defaults.required_metadata)
    required_metadata.update(base.required_metadata)
    if not sources and not required_columns and not required_types and not required_metadata:
        return None
    return EvidenceSpec(
        sources=sources,
        required_columns=required_columns,
        required_types=required_types,
        required_metadata=required_metadata,
    )


def _merge_evidence_output(
    base: EvidenceOutput | None,
    defaults: EvidenceOutput | None,
) -> EvidenceOutput | None:
    if base is None:
        return defaults
    if defaults is None:
        return base
    column_map = dict(defaults.column_map)
    column_map.update(base.column_map)
    literals = dict(defaults.literals)
    literals.update(base.literals)
    provenance_columns = tuple(
        dict.fromkeys((*defaults.provenance_columns, *base.provenance_columns))
    )
    if not column_map and not literals and not provenance_columns:
        return None
    return EvidenceOutput(
        column_map=column_map,
        literals=literals,
        provenance_columns=provenance_columns,
    )


__all__ = ["apply_evidence_defaults", "apply_policy_defaults", "apply_rule_defaults"]
