"""Default rule-family specs for relationship rule generation."""

from __future__ import annotations

from functools import cache

from relspec.model import RuleFamilySpec

RULE_FAMILY_SPECS: tuple[RuleFamilySpec, ...] = (
    RuleFamilySpec(
        name="symbol_role",
        factory="symbol_role",
        inputs=("rel_name_symbol",),
        confidence_policy="scip",
        option_flag="emit_symbol_role_edges",
    ),
    RuleFamilySpec(
        name="scip_symbol",
        factory="scip_symbol",
        inputs=("scip_symbol_relationships",),
        confidence_policy="scip",
        option_flag="emit_scip_symbol_relationship_edges",
    ),
    RuleFamilySpec(
        name="import",
        factory="import",
        inputs=("rel_import_symbol",),
        confidence_policy="scip",
        option_flag="emit_import_edges",
    ),
    RuleFamilySpec(
        name="call",
        factory="call",
        inputs=("rel_callsite_symbol",),
        confidence_policy="scip",
        option_flag="emit_call_edges",
    ),
    RuleFamilySpec(
        name="qname_fallback",
        factory="qname_fallback",
        inputs=("rel_callsite_qname", "rel_callsite_symbol"),
        confidence_policy="qname_fallback",
        ambiguity_policy="qname_fallback",
        option_flag="emit_qname_fallback_call_edges",
    ),
    RuleFamilySpec(
        name="diagnostic",
        factory="diagnostic",
        inputs=("diagnostics_norm",),
        option_flag="emit_diagnostic_edges",
    ),
    RuleFamilySpec(
        name="type",
        factory="type",
        inputs=("type_exprs_norm",),
        confidence_policy="type",
        option_flag="emit_type_edges",
    ),
    RuleFamilySpec(
        name="runtime",
        factory="runtime",
        inputs=("rt_signatures", "rt_signature_params", "rt_members"),
        confidence_policy="runtime",
        option_flag="emit_runtime_edges",
    ),
)


@cache
def rule_family_specs() -> tuple[RuleFamilySpec, ...]:
    """Return default rule-family specs.

    Returns
    -------
    tuple[RuleFamilySpec, ...]
        Rule family specification entries.
    """
    return RULE_FAMILY_SPECS


__all__ = ["rule_family_specs"]
