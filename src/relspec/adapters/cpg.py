"""CPG adapter for centralized rule definitions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import cache
from typing import TYPE_CHECKING

from cpg.relation_registry_specs import RULE_TEMPLATE_SPECS, rule_definition_specs
from cpg.relation_template_specs import (
    EdgeDefinitionSpec,
    RuleDefinitionSpec,
    expand_rule_templates,
)
from cpg.relation_template_specs import (
    RuleTemplateSpec as CpgRuleTemplateSpec,
)
from relspec.rules.definitions import (
    EdgeEmitPayload,
    EvidenceSpec,
    PolicyOverrides,
    RelationshipPayload,
    RuleDefinition,
)
from relspec.rules.diagnostics import RuleDiagnostic
from relspec.rules.registry import RuleAdapter
from relspec.rules.templates import RuleTemplateSpec

if TYPE_CHECKING:
    from relspec.rules.definitions import RuleDomain


class CpgRuleAdapter(RuleAdapter):
    """Adapter that exposes CPG relationship rules as central RuleDefinitions."""

    domain: RuleDomain = "cpg"

    def rule_definitions(self) -> Sequence[RuleDefinition]:
        """Return CPG rule definitions for the central registry.

        Returns
        -------
        Sequence[RuleDefinition]
            CPG rule definitions.

        Raises
        ------
        ValueError
            Raised when the adapter domain is misconfigured.
        """
        if self.domain != "cpg":
            msg = f"Unexpected adapter domain: {self.domain!r}."
            raise ValueError(msg)
        return tuple(_rule_from_spec(spec) for spec in rule_definition_specs())

    def templates(self) -> Sequence[RuleTemplateSpec]:
        """Return template specs for the CPG adapter.

        Returns
        -------
        Sequence[RuleTemplateSpec]
            Centralized template specs.

        Raises
        ------
        ValueError
            Raised when the adapter domain is misconfigured.
        """
        if self.domain != "cpg":
            msg = f"Unexpected adapter domain: {self.domain!r}."
            raise ValueError(msg)
        specs, _ = _template_catalog()
        return specs

    def template_diagnostics(self) -> Sequence[RuleDiagnostic]:
        """Return template diagnostics for CPG templates.

        Returns
        -------
        Sequence[RuleDiagnostic]
            Template diagnostics for CPG templates.

        Raises
        ------
        ValueError
            Raised when the adapter domain is misconfigured.
        """
        if self.domain != "cpg":
            msg = f"Unexpected adapter domain: {self.domain!r}."
            raise ValueError(msg)
        _, diagnostics = _template_catalog()
        return diagnostics


@cache
def _template_catalog() -> tuple[tuple[RuleTemplateSpec, ...], tuple[RuleDiagnostic, ...]]:
    specs: list[RuleTemplateSpec] = []
    diagnostics: list[RuleDiagnostic] = []
    for spec in RULE_TEMPLATE_SPECS:
        outputs, output_diagnostics = _template_outputs(spec)
        diagnostics.extend(output_diagnostics)
        specs.append(
            RuleTemplateSpec(
                name=spec.name,
                domain="cpg",
                template=spec.factory,
                outputs=outputs,
                feature_flags=(),
                metadata=_template_metadata(spec),
            )
        )
    return tuple(specs), tuple(diagnostics)


def _template_outputs(
    spec: CpgRuleTemplateSpec,
) -> tuple[tuple[str, ...], tuple[RuleDiagnostic, ...]]:
    try:
        expanded = expand_rule_templates((spec,))
    except (KeyError, TypeError, ValueError) as exc:
        return (
            (),
            (
                RuleDiagnostic(
                    domain="cpg",
                    template=getattr(spec, "factory", None),
                    rule_name=None,
                    severity="error",
                    message=f"Template expansion failed for {getattr(spec, 'name', 'unknown')!r}: {exc}",
                    metadata={"template": str(getattr(spec, "factory", ""))},
                ),
            ),
        )
    outputs = tuple(item.name for item in expanded)
    return outputs, ()


def _template_metadata(spec: CpgRuleTemplateSpec) -> Mapping[str, str]:
    metadata: dict[str, str] = {"spec_name": str(getattr(spec, "name", ""))}
    factory = getattr(spec, "factory", None)
    if factory:
        metadata["factory"] = str(factory)
    inputs = getattr(spec, "inputs", ())
    if inputs:
        metadata["inputs"] = ",".join(str(name) for name in inputs)
    params = getattr(spec, "params", {})
    if isinstance(params, Mapping):
        for key, value in params.items():
            metadata[f"param.{key}"] = str(value)
    return metadata


def _rule_from_spec(spec: RuleDefinitionSpec) -> RuleDefinition:
    payload = RelationshipPayload(
        output_dataset=spec.output_dataset or spec.name,
        contract_name=spec.contract_name,
        hash_join=spec.hash_join,
        interval_align=spec.interval_align,
        winner_select=spec.winner_select,
        predicate=spec.predicate,
        project=spec.project,
        rule_name_col=spec.rule_name_col,
        rule_priority_col=spec.rule_priority_col,
        edge_emit=_edge_emit_payload(spec.edge),
    )
    return RuleDefinition(
        name=spec.name,
        domain="cpg",
        kind=spec.kind.value,
        inputs=spec.inputs,
        output=payload.output_dataset or spec.name,
        execution_mode=spec.execution_mode,
        priority=spec.priority,
        evidence=_evidence_payload(spec),
        policy_overrides=PolicyOverrides(
            confidence_policy=spec.confidence_policy,
            ambiguity_policy=spec.ambiguity_policy,
        ),
        emit_rule_meta=spec.emit_rule_meta,
        post_kernels=spec.post_kernels,
        payload=payload,
    )


def _evidence_payload(spec: RuleDefinitionSpec) -> EvidenceSpec | None:
    if spec.evidence is None:
        return None
    return EvidenceSpec(
        sources=spec.evidence.sources,
        required_columns=spec.evidence.required_columns,
        required_types=spec.evidence.required_types,
    )


def _edge_emit_payload(edge: EdgeDefinitionSpec | None) -> EdgeEmitPayload | None:
    if edge is None:
        return None
    return EdgeEmitPayload(
        edge_kind=edge.edge_kind.value,
        src_cols=edge.src_cols,
        dst_cols=edge.dst_cols,
        origin=edge.origin,
        resolution_method=edge.resolution_method,
        option_flag=edge.option_flag,
        path_cols=edge.path_cols,
        bstart_cols=edge.bstart_cols,
        bend_cols=edge.bend_cols,
    )


__all__ = ["CpgRuleAdapter"]
