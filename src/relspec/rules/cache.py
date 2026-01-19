"""Cached accessors for centralized rule registries and tables."""

from __future__ import annotations

from collections.abc import Mapping
from functools import cache

import pyarrow as pa

from registry_common.arrow_payloads import payload_hash
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

RULE_SIGNATURE_VERSION = 1
_RULE_SIGNATURE_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("domain", pa.string()),
        pa.field("kind", pa.string()),
        pa.field("inputs", pa.list_(pa.string())),
        pa.field("output", pa.string()),
        pa.field("payload_repr", pa.string()),
        pa.field("execution_mode", pa.string()),
    ]
)


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
        "version": RULE_SIGNATURE_VERSION,
        "name": rule.name,
        "domain": rule.domain,
        "kind": str(rule.kind) if rule.kind is not None else None,
        "inputs": list(rule.inputs),
        "output": rule.output,
        "payload_repr": _stable_repr(rule.payload),
        "execution_mode": rule.execution_mode,
    }
    return payload_hash(payload, _RULE_SIGNATURE_SCHEMA)


def _stable_repr(value: object) -> str:
    if isinstance(value, Mapping):
        items = ", ".join(
            f"{_stable_repr(key)}:{_stable_repr(val)}"
            for key, val in sorted(value.items(), key=lambda item: str(item[0]))
        )
        return f"{{{items}}}"
    if isinstance(value, (list, tuple, set)):
        rendered = [_stable_repr(item) for item in value]
        if isinstance(value, set):
            rendered = sorted(rendered)
        items = ", ".join(rendered)
        bracket = "()" if isinstance(value, tuple) else "[]"
        return f"{bracket[0]}{items}{bracket[1]}"
    return repr(value)


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
