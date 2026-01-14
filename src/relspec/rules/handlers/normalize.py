"""Normalize rule handler for centralized compilation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from arrowdsl.core.context import ExecutionContext
from arrowdsl.plan.catalog import PlanDeriver
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
from relspec.rules.spec_tables import query_from_ops

if TYPE_CHECKING:
    from arrowdsl.plan.query import QuerySpec
    from relspec.rules.definitions import EvidenceOutput, EvidenceSpec, RuleDefinition, RuleDomain


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
        query = _normalize_query(payload, pipeline_ops=rule.pipeline_ops)
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
        return apply_rule_defaults(base)


def _normalize_query(
    payload: object | None, *, pipeline_ops: tuple[Mapping[str, object], ...]
) -> QuerySpec | None:
    if isinstance(payload, NormalizePayload) and payload.query is not None:
        return payload.query
    if pipeline_ops:
        return query_from_ops(pipeline_ops)
    return None


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
