"""Declarative relationship rule specs for CPG edges."""

from __future__ import annotations

from functools import cache

from arrowdsl.spec.expr_ir import ExprIR
from cpg.kinds_ultimate import EdgeKind
from cpg.relation_template_specs import (
    EdgeDefinitionSpec,
    RuleDefinitionSpec,
    RuleTemplateSpec,
    expand_rule_templates,
)
from relspec.model import HashJoinConfig, ProjectConfig, RuleKind

RULE_TEMPLATE_SPECS: tuple[RuleTemplateSpec, ...] = (
    RuleTemplateSpec(
        name="symbol_role",
        factory="symbol_role",
        inputs=("rel_name_symbol",),
        params={"confidence_policy": "scip", "option_flag": "emit_symbol_role_edges"},
    ),
    RuleTemplateSpec(
        name="scip_symbol",
        factory="scip_symbol",
        inputs=("scip_symbol_relationships",),
        params={
            "confidence_policy": "scip",
            "option_flag": "emit_scip_symbol_relationship_edges",
        },
    ),
    RuleTemplateSpec(
        name="import",
        factory="import",
        inputs=("rel_import_symbol",),
        params={"confidence_policy": "scip", "option_flag": "emit_import_edges"},
    ),
    RuleTemplateSpec(
        name="call",
        factory="call",
        inputs=("rel_callsite_symbol",),
        params={"confidence_policy": "scip", "option_flag": "emit_call_edges"},
    ),
    RuleTemplateSpec(
        name="diagnostic",
        factory="diagnostic",
        inputs=("diagnostics_norm",),
        params={"option_flag": "emit_diagnostic_edges"},
    ),
    RuleTemplateSpec(
        name="type",
        factory="type",
        inputs=("type_exprs_norm",),
        params={"confidence_policy": "type", "option_flag": "emit_type_edges"},
    ),
    RuleTemplateSpec(
        name="runtime",
        factory="runtime",
        inputs=("rt_signatures", "rt_signature_params", "rt_members"),
        params={"confidence_policy": "runtime", "option_flag": "emit_runtime_edges"},
    ),
)

RULE_DEFINITION_SPECS: tuple[RuleDefinitionSpec, ...] = (
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
    expanded = expand_rule_templates(RULE_TEMPLATE_SPECS)
    return (*expanded, *RULE_DEFINITION_SPECS)


__all__ = ["rule_definition_specs"]
