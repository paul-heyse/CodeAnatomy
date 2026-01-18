"""Cached accessors for centralized rule registries and tables."""

from __future__ import annotations

import hashlib
import json
from functools import cache

import pyarrow as pa

from relspec.adapters import CpgRuleAdapter, ExtractRuleAdapter, NormalizeRuleAdapter
from relspec.compiler import rel_plan_for_rule
from relspec.plan import rel_plan_signature
from relspec.rules.definitions import RuleDefinition, RuleDomain
from relspec.rules.diagnostics import RuleDiagnostic, rule_diagnostic_table
from relspec.rules.graph import rule_graph_signature
from relspec.rules.handlers.cpg import relationship_rule_from_definition
from relspec.rules.registry import RuleRegistry
from relspec.rules.rel_ops import rel_ops_signature
from relspec.rules.spec_tables import rule_definition_table


@cache
def rule_registry_cached() -> RuleRegistry:
    """Return the cached centralized rule registry.

    Returns
    -------
    RuleRegistry
        Cached rule registry with centralized adapters.
    """
    return RuleRegistry(
        adapters=(
            CpgRuleAdapter(),
            NormalizeRuleAdapter(),
            ExtractRuleAdapter(),
        )
    )


@cache
def rule_definitions_cached(domain: RuleDomain | None = None) -> tuple[RuleDefinition, ...]:
    """Return cached rule definitions, optionally filtered by domain.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Cached rule definitions for the requested domain.
    """
    registry = rule_registry_cached()
    if domain is None:
        return registry.rule_definitions()
    return registry.rules_for_domain(domain)


@cache
def rule_table_cached(domain: RuleDomain | None = None) -> pa.Table:
    """Return the canonical rule table, optionally filtered by domain.

    Returns
    -------
    pyarrow.Table
        Canonical rule definition table for the requested domain.
    """
    if domain is None:
        return rule_registry_cached().rule_table()
    definitions = rule_definitions_cached(domain)
    return rule_definition_table(definitions)


@cache
def template_table_cached() -> pa.Table:
    """Return the centralized template catalog table.

    Returns
    -------
    pyarrow.Table
        Template catalog table from the centralized registry.
    """
    return rule_registry_cached().template_table()


@cache
def template_diagnostics_table_cached() -> pa.Table:
    """Return the centralized template diagnostics table.

    Returns
    -------
    pyarrow.Table
        Template diagnostics table from the centralized registry.
    """
    return rule_registry_cached().template_diagnostics_table()


@cache
def rule_diagnostics_cached(domain: RuleDomain | None = None) -> tuple[RuleDiagnostic, ...]:
    """Return cached rule diagnostics, optionally filtered by domain.

    Returns
    -------
    tuple[RuleDiagnostic, ...]
        Diagnostics for rules in the requested domain.
    """
    diagnostics = rule_registry_cached().rule_diagnostics()
    if domain is None:
        return diagnostics
    return tuple(diag for diag in diagnostics if diag.domain == domain)


@cache
def rule_diagnostics_table_cached(domain: RuleDomain | None = None) -> pa.Table:
    """Return cached rule diagnostics table, optionally filtered by domain.

    Returns
    -------
    pyarrow.Table
        Diagnostics table for rules in the requested domain.
    """
    if domain is None:
        return rule_registry_cached().rule_diagnostics_table()
    diagnostics = rule_diagnostics_cached(domain)
    return rule_diagnostic_table(diagnostics)


@cache
def rule_plan_signatures_cached(domain: RuleDomain | None = None) -> dict[str, str]:
    """Return stable plan signatures for rules, optionally filtered by domain.

    Returns
    -------
    dict[str, str]
        Signature mapping keyed by rule name.
    """
    rules = rule_definitions_cached(domain)
    return {rule.name: _rule_signature(rule) for rule in rules}


@cache
def rule_plan_sql_cached(domain: RuleDomain | None = None) -> dict[str, str]:
    """Return optimized SQL strings for rule definitions.

    Returns
    -------
    dict[str, str]
        Mapping of rule names to optimized SQL strings.
    """
    diagnostics = rule_diagnostics_cached(domain)
    plan_sql: dict[str, str] = {}
    for diag in diagnostics:
        if diag.rule_name is None:
            continue
        optimized_sql = diag.metadata.get("optimized_sql")
        if optimized_sql and diag.rule_name not in plan_sql:
            plan_sql[diag.rule_name] = optimized_sql
    return plan_sql


@cache
def rule_graph_signature_cached(domain: RuleDomain | None = None) -> str:
    """Return a stable graph signature for rules, optionally filtered by domain.

    Returns
    -------
    str
        Deterministic graph signature for the requested domain.
    """
    rules = rule_definitions_cached(domain)
    signatures = rule_plan_signatures_cached(domain)
    return rule_graph_signature(
        rules,
        name_for=lambda rule: rule.name,
        signature_for=lambda rule: signatures.get(rule.name, ""),
        label=domain or "all",
    )


def _rule_signature(rule: RuleDefinition) -> str:
    """Return a stable signature for a rule definition.

    Parameters
    ----------
    rule
        Rule definition to sign.

    Returns
    -------
    str
        Deterministic hash for the rule definition.
    """
    if rule.domain == "cpg":
        rel_rule = relationship_rule_from_definition(rule)
        plan = rel_plan_for_rule(rel_rule)
        if plan is not None:
            return rel_plan_signature(plan)
    if rule.rel_ops:
        return rel_ops_signature(rule.rel_ops)
    payload = {
        "name": rule.name,
        "domain": rule.domain,
        "kind": rule.kind,
        "inputs": list(rule.inputs),
        "output": rule.output,
        "payload": repr(rule.payload),
        "execution_mode": rule.execution_mode,
    }
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


__all__ = [
    "rule_definitions_cached",
    "rule_diagnostics_cached",
    "rule_diagnostics_table_cached",
    "rule_graph_signature_cached",
    "rule_plan_signatures_cached",
    "rule_plan_sql_cached",
    "rule_registry_cached",
    "rule_table_cached",
    "template_diagnostics_table_cached",
    "template_table_cached",
]
