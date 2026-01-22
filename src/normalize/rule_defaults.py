"""Default policy and evidence resolution for normalize rules."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from arrowdsl.core.interop import SchemaLike
from normalize.evidence_specs import evidence_output_from_schema, evidence_spec_from_schema
from normalize.policies import (
    ambiguity_policy_from_schema,
    confidence_policy_from_schema,
    default_tie_breakers,
)
from normalize.registry_runtime import dataset_schema
from relspec.model import AmbiguityPolicy, ConfidencePolicy
from relspec.policies import PolicyRegistry
from relspec.rules.definitions import EvidenceOutput, EvidenceSpec, RuleDefinition

if TYPE_CHECKING:
    from arrowdsl.core.expr_types import ScalarValue


@dataclass(frozen=True)
class NormalizeRuleDefaults:
    """Resolved evidence and policy defaults for a normalize rule."""

    evidence: EvidenceSpec | None
    evidence_output: EvidenceOutput | None
    confidence_policy: ConfidencePolicy | None
    ambiguity_policy: AmbiguityPolicy | None


def resolve_rule_defaults(
    rule: RuleDefinition,
    *,
    registry: PolicyRegistry,
    scan_provenance_columns: Sequence[str] = (),
) -> NormalizeRuleDefaults:
    """Resolve policy and evidence defaults for a normalize rule.

    Returns
    -------
    NormalizeRuleDefaults
        Resolved defaults derived from schema metadata and overrides.
    """
    schema = dataset_schema(rule.output)
    confidence = _resolve_confidence_policy(rule, schema=schema, registry=registry)
    ambiguity = _resolve_ambiguity_policy(rule, schema=schema, registry=registry)
    evidence = _merge_evidence(rule.evidence, evidence_spec_from_schema(schema))
    base_output = _base_evidence_output(rule.evidence_output, scan_provenance_columns)
    evidence_output = _merge_evidence_output(base_output, evidence_output_from_schema(schema))
    return NormalizeRuleDefaults(
        evidence=evidence,
        evidence_output=evidence_output,
        confidence_policy=confidence,
        ambiguity_policy=ambiguity,
    )


def resolve_rule_defaults_for_rules(
    rules: Sequence[RuleDefinition],
    *,
    registry: PolicyRegistry,
    scan_provenance_columns: Sequence[str] = (),
) -> tuple[NormalizeRuleDefaults, ...]:
    """Resolve defaults for a sequence of normalize rules.

    Returns
    -------
    tuple[NormalizeRuleDefaults, ...]
        Defaults resolved for each rule.
    """
    return tuple(
        resolve_rule_defaults(
            rule,
            registry=registry,
            scan_provenance_columns=scan_provenance_columns,
        )
        for rule in rules
    )


def _resolve_confidence_policy(
    rule: RuleDefinition,
    *,
    schema: SchemaLike,
    registry: PolicyRegistry,
) -> ConfidencePolicy | None:
    override = rule.policy_overrides.confidence_policy
    if override:
        policy = registry.resolve_confidence("normalize", override)
        if isinstance(policy, ConfidencePolicy):
            return policy
        msg = f"Expected ConfidencePolicy for policy {override!r}."
        raise TypeError(msg)
    return confidence_policy_from_schema(schema, registry=registry)


def _resolve_ambiguity_policy(
    rule: RuleDefinition,
    *,
    schema: SchemaLike,
    registry: PolicyRegistry,
) -> AmbiguityPolicy | None:
    override = rule.policy_overrides.ambiguity_policy
    if override:
        policy = registry.resolve_ambiguity("normalize", override)
        if isinstance(policy, AmbiguityPolicy):
            return _apply_default_tie_breakers(policy, schema=schema)
        msg = f"Expected AmbiguityPolicy for policy {override!r}."
        raise TypeError(msg)
    policy = ambiguity_policy_from_schema(schema, registry=registry)
    if policy is None:
        return None
    return _apply_default_tie_breakers(policy, schema=schema)


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


def _base_evidence_output(
    spec: EvidenceOutput | None,
    scan_provenance_columns: Sequence[str],
) -> EvidenceOutput | None:
    if spec is None and not scan_provenance_columns:
        return None
    column_map: dict[str, str] = dict(spec.column_map) if spec is not None else {}
    literals: dict[str, ScalarValue] = dict(spec.literals) if spec is not None else {}
    provenance_columns = tuple(
        dict.fromkeys(
            (
                *(spec.provenance_columns if spec is not None else ()),
                *scan_provenance_columns,
            )
        )
    )
    if not column_map and not literals and not provenance_columns:
        return None
    return EvidenceOutput(
        column_map=column_map,
        literals=literals,
        provenance_columns=provenance_columns,
    )


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


__all__ = [
    "NormalizeRuleDefaults",
    "resolve_rule_defaults",
    "resolve_rule_defaults_for_rules",
]
