"""Evidence-driven extraction plan compilation."""

from __future__ import annotations

from collections.abc import Sequence

from extract.evidence_specs import EvidenceSpec as ExtractEvidenceSpec
from extract.evidence_specs import evidence_spec
from extract.normalize_ops import normalize_ops_for_output
from relspec.model import EvidenceSpec as RelationshipEvidenceSpec
from relspec.model import RelationshipRule
from relspec.rules.definitions import EvidenceSpec, RuleDefinition
from relspec.rules.evidence import (
    EvidenceDatasetSpec,
    EvidencePlan,
    EvidenceRequirement,
)
from relspec.rules.evidence import (
    compile_evidence_plan as compile_central_plan,
)


def compile_evidence_plan(
    rules: Sequence[RelationshipRule | RuleDefinition],
    *,
    extra_sources: Sequence[str] = (),
) -> EvidencePlan:
    """Compile an evidence plan from relationship rules.

    Returns
    -------
    EvidencePlan
        Evidence plan describing required datasets and ops.
    """
    normalized = tuple(_normalize_rule(rule) for rule in rules)
    return compile_central_plan(
        normalized,
        extra_sources=extra_sources,
        evidence_spec=_evidence_spec_record,
        normalize_ops_for_output=normalize_ops_for_output,
    )


def _normalize_rule(rule: RelationshipRule | RuleDefinition) -> RuleDefinition:
    if isinstance(rule, RuleDefinition):
        return rule
    evidence = rule.evidence
    return RuleDefinition(
        name=rule.name,
        domain="cpg",
        kind=rule.kind.value,
        inputs=tuple(ref.name for ref in rule.inputs),
        output=rule.output_dataset,
        execution_mode=rule.execution_mode,
        priority=rule.priority,
        evidence=_evidence_from_relationship(evidence),
        emit_rule_meta=rule.emit_rule_meta,
    )


def _evidence_from_relationship(
    evidence: RelationshipEvidenceSpec | EvidenceSpec | None,
) -> EvidenceSpec | None:
    if evidence is None:
        return None
    if not isinstance(evidence, EvidenceSpec):
        spec = evidence
        return EvidenceSpec(
            sources=spec.sources,
            required_columns=spec.required_columns,
            required_types=spec.required_types,
        )
    return evidence


def _evidence_spec_record(name: str) -> EvidenceDatasetSpec | None:
    spec = _maybe_spec(name)
    if spec is None:
        return None
    return EvidenceDatasetSpec(
        name=spec.name,
        alias=spec.alias,
        template=spec.template,
        required_columns=spec.required_columns,
    )


def _maybe_spec(name: str) -> ExtractEvidenceSpec | None:
    try:
        return evidence_spec(name)
    except KeyError:
        return None


__all__ = ["EvidencePlan", "EvidenceRequirement", "compile_evidence_plan"]
