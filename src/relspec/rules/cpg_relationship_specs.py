"""Declarative relationship rule specs for CPG edges."""

from __future__ import annotations

from collections.abc import Mapping
from functools import cache

from arrowdsl.spec.expr_ir import ExprIR
from cpg.kinds_ultimate import EdgeKind
from relspec.model import HashJoinConfig, ProjectConfig, RuleKind
from relspec.rules.cpg_relationship_templates import (
    EdgeDefinitionSpec,
    RuleDefinitionSpec,
    expand_rule_templates,
)
from relspec.rules.cpg_relationship_templates import (
    RuleTemplateSpec as CpgRuleTemplateSpec,
)
from relspec.rules.decorators import rule_bundle
from relspec.rules.definitions import (
    EdgeEmitPayload,
    EvidenceSpec,
    PolicyOverrides,
    RelationshipPayload,
    RuleDefinition,
)
from relspec.rules.diagnostics import RuleDiagnostic
from relspec.rules.templates import RuleTemplateSpec


@cache
def rule_template_specs() -> tuple[CpgRuleTemplateSpec, ...]:
    """Return CPG rule template specs for expansion.

    Returns
    -------
    tuple[RuleTemplateSpec, ...]
        Template specs for CPG relationship rules.
    """
    return (
        CpgRuleTemplateSpec(
            name="symbol_role",
            factory="symbol_role",
            inputs=("rel_name_symbol",),
            params={"confidence_policy": "scip", "option_flag": "emit_symbol_role_edges"},
        ),
        CpgRuleTemplateSpec(
            name="scip_symbol",
            factory="scip_symbol",
            inputs=("scip_symbol_relationships",),
            params={
                "confidence_policy": "scip",
                "option_flag": "emit_scip_symbol_relationship_edges",
            },
        ),
        CpgRuleTemplateSpec(
            name="import",
            factory="import",
            inputs=("rel_import_symbol",),
            params={"confidence_policy": "scip", "option_flag": "emit_import_edges"},
        ),
        CpgRuleTemplateSpec(
            name="call",
            factory="call",
            inputs=("rel_callsite_symbol",),
            params={"confidence_policy": "scip", "option_flag": "emit_call_edges"},
        ),
        CpgRuleTemplateSpec(
            name="diagnostic",
            factory="diagnostic",
            inputs=("diagnostics_norm",),
            params={"option_flag": "emit_diagnostic_edges"},
        ),
        CpgRuleTemplateSpec(
            name="type",
            factory="type",
            inputs=("type_exprs_norm",),
            params={"confidence_policy": "type", "option_flag": "emit_type_edges"},
        ),
        CpgRuleTemplateSpec(
            name="runtime",
            factory="runtime",
            inputs=("rt_signatures", "rt_signature_params", "rt_members"),
            params={"confidence_policy": "runtime", "option_flag": "emit_runtime_edges"},
        ),
    )


@cache
def _base_definition_specs() -> tuple[RuleDefinitionSpec, ...]:
    return (
        RuleDefinitionSpec(
            name="qname_fallback_calls",
            kind=RuleKind.HASH_JOIN,
            inputs=("rel_callsite_qname", "rel_callsite_symbol"),
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
                ),
                right_output=(),
            ),
            project=ProjectConfig(
                select=(
                    "call_id",
                    "qname_id",
                    "path",
                    "call_bstart",
                    "call_bend",
                ),
                exprs={"ambiguity_group_id": ExprIR(op="field", name="call_id")},
            ),
            confidence_policy="qname_fallback",
            ambiguity_policy="qname_fallback",
            edge=EdgeDefinitionSpec(
                edge_kind=EdgeKind.PY_CALLS_QNAME,
                src_cols=("call_id",),
                dst_cols=("qname_id",),
                origin="qnp",
                resolution_method="QNP_CALLEE_FALLBACK",
                option_flag="emit_qname_fallback_call_edges",
                path_cols=("path",),
                bstart_cols=("call_bstart", "bstart"),
                bend_cols=("call_bend", "bend"),
            ),
        ),
    )


@cache
def rule_definition_specs() -> tuple[RuleDefinitionSpec, ...]:
    """Return declarative relationship rule definitions.

    Returns
    -------
    tuple[RuleDefinitionSpec, ...]
        Rule definition specs for relationship rules.
    """
    expanded = expand_rule_templates(rule_template_specs())
    return (*expanded, *_base_definition_specs())


def cpg_template_specs() -> tuple[RuleTemplateSpec, ...]:
    """Return template specs for the CPG bundle.

    Returns
    -------
    tuple[RuleTemplateSpec, ...]
        Template specs for CPG rules.
    """
    specs, _ = _template_catalog()
    return specs


def cpg_template_diagnostics() -> tuple[RuleDiagnostic, ...]:
    """Return template diagnostics for the CPG bundle.

    Returns
    -------
    tuple[RuleDiagnostic, ...]
        Template diagnostics for CPG rules.
    """
    _, diagnostics = _template_catalog()
    return diagnostics


@rule_bundle(
    name="relspec.cpg.relationships",
    domain="cpg",
    templates=cpg_template_specs,
    diagnostics=cpg_template_diagnostics,
)
@cache
def cpg_rule_definitions() -> tuple[RuleDefinition, ...]:
    """Return CPG rule definitions for registry use.

    Returns
    -------
    tuple[RuleDefinition, ...]
        CPG rule definitions.
    """
    return tuple(_rule_from_spec(spec) for spec in rule_definition_specs())


@cache
def _template_catalog() -> tuple[tuple[RuleTemplateSpec, ...], tuple[RuleDiagnostic, ...]]:
    """Build the cached catalog of CPG templates and diagnostics.

    Returns
    -------
    tuple[tuple[RuleTemplateSpec, ...], tuple[RuleDiagnostic, ...]]
        Template specs and diagnostics for the CPG bundle.
    """
    specs: list[RuleTemplateSpec] = []
    diagnostics: list[RuleDiagnostic] = []
    for spec in rule_template_specs():
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
    """Expand a template spec and collect output diagnostics.

    Parameters
    ----------
    spec
        CPG rule template specification.

    Returns
    -------
    tuple[tuple[str, ...], tuple[RuleDiagnostic, ...]]
        Output names and any diagnostics from expansion.
    """
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
    """Build metadata for a CPG rule template.

    Parameters
    ----------
    spec
        CPG rule template specification.

    Returns
    -------
    Mapping[str, str]
        Metadata mapping for the template.
    """
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
    """Convert a CPG rule definition spec to a central RuleDefinition.

    Parameters
    ----------
    spec
        Rule definition spec from CPG registry.

    Returns
    -------
    RuleDefinition
        Central rule definition.
    """
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
    """Convert evidence config from a rule definition spec.

    Parameters
    ----------
    spec
        Rule definition spec containing evidence settings.

    Returns
    -------
    EvidenceSpec | None
        Evidence spec for centralized rules.
    """
    if spec.evidence is None:
        return None
    return EvidenceSpec(
        sources=spec.evidence.sources,
        required_columns=spec.evidence.required_columns,
        required_types=spec.evidence.required_types,
    )


def _edge_emit_payload(edge: EdgeDefinitionSpec | None) -> EdgeEmitPayload | None:
    """Convert an edge definition to an emit payload.

    Parameters
    ----------
    edge
        Edge definition spec.

    Returns
    -------
    EdgeEmitPayload | None
        Emit payload for edge outputs.
    """
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


__all__ = [
    "cpg_rule_definitions",
    "cpg_template_diagnostics",
    "cpg_template_specs",
    "rule_definition_specs",
    "rule_template_specs",
]
