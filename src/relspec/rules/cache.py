"""Cached accessors for centralized rule registries and tables."""

from __future__ import annotations

from functools import cache

import pyarrow as pa

from relspec.adapters import (
    CpgRuleAdapter,
    ExtractRuleAdapter,
    NormalizeRuleAdapter,
    default_rule_factory_registry,
)
from relspec.graph import rule_graph_signature
from relspec.registry.rules import RuleRegistry
from relspec.registry.snapshot import RelspecSnapshot, build_relspec_snapshot
from relspec.rules.definitions import RuleDefinition, RuleDomain
from relspec.rules.diagnostics import RuleDiagnostic, rule_diagnostic_table
from relspec.rules.spec_tables import rule_definition_table


@cache
def rule_registry_cached() -> RuleRegistry:
    """Return the cached centralized rule registry.

    Returns
    -------
    RuleRegistry
        Cached rule registry with centralized adapters.
    """
    registry = default_rule_factory_registry()
    return RuleRegistry(
        adapters=(
            CpgRuleAdapter(registry=registry),
            NormalizeRuleAdapter(registry=registry),
            ExtractRuleAdapter(registry=registry),
        )
    )


@cache
def relspec_snapshot_cached() -> RelspecSnapshot:
    """Return the cached relspec snapshot.

    Returns
    -------
    RelspecSnapshot
        Snapshot of centralized rule tables and signatures.
    """
    return build_relspec_snapshot(rule_registry_cached())


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
        return relspec_snapshot_cached().rule_table
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
    return relspec_snapshot_cached().template_table


@cache
def template_diagnostics_table_cached() -> pa.Table:
    """Return the centralized template diagnostics table.

    Returns
    -------
    pyarrow.Table
        Template diagnostics table from the centralized registry.
    """
    return relspec_snapshot_cached().template_diagnostics


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
        return relspec_snapshot_cached().rule_diagnostics
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
    signatures = relspec_snapshot_cached().plan_signatures
    if domain is None:
        return dict(signatures)
    rules = rule_definitions_cached(domain)
    return {rule.name: signatures.get(rule.name, "") for rule in rules}


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


__all__ = [
    "relspec_snapshot_cached",
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
