"""Cached accessors for centralized rule registries and tables."""

from __future__ import annotations

from functools import cache

import pyarrow as pa

from relspec.adapters import CpgRuleAdapter, ExtractRuleAdapter, NormalizeRuleAdapter
from relspec.rules.definitions import RuleDefinition, RuleDomain
from relspec.rules.registry import RuleRegistry
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


__all__ = [
    "rule_definitions_cached",
    "rule_registry_cached",
    "rule_table_cached",
    "template_diagnostics_table_cached",
    "template_table_cached",
]
