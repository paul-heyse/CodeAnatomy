"""Normalize rule registry and lookup helpers."""

from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING

import pyarrow as pa

from arrowdsl.spec.tables.normalize import (
    normalize_rule_definition_table,
    normalize_rule_definitions_from_table,
    normalize_rule_family_specs_from_table,
    normalize_rule_family_table,
)
from normalize.registry_validation import validate_rule_specs
from normalize.rule_definitions import build_rules_from_definitions
from normalize.rule_factories import build_rule_definitions_from_specs
from normalize.rule_registry_specs import rule_family_specs

if TYPE_CHECKING:
    from normalize.rule_definitions import NormalizeRuleDefinition
    from normalize.rule_model import NormalizeRule


@cache
def rule_family_spec_table_cached() -> pa.Table:
    """Return the normalize rule family spec table.

    Returns
    -------
    pa.Table
        Arrow table of normalize rule family specs.
    """
    return normalize_rule_family_table(rule_family_specs())


@cache
def rule_definition_table_cached() -> pa.Table:
    """Return the normalize rule definition spec table.

    Returns
    -------
    pa.Table
        Arrow table of normalize rule definitions.
    """
    family_specs = normalize_rule_family_specs_from_table(rule_family_spec_table_cached())
    definitions = build_rule_definitions_from_specs(family_specs)
    return normalize_rule_definition_table(definitions)


@cache
def normalize_rule_definitions() -> tuple[NormalizeRuleDefinition, ...]:
    """Return normalize rule definition specs in registry order.

    Returns
    -------
    tuple[NormalizeRuleDefinition, ...]
        Normalize rule definitions.
    """
    return normalize_rule_definitions_from_table(rule_definition_table_cached())


@cache
def normalize_rules() -> tuple[NormalizeRule, ...]:
    """Return the normalize rules in registry order.

    Returns
    -------
    tuple[NormalizeRule, ...]
        Normalize rule registry.
    """
    rules = build_rules_from_definitions(normalize_rule_definitions())
    validate_rule_specs(rules)
    return rules


@cache
def _rules_by_name() -> dict[str, NormalizeRule]:
    return {rule.name: rule for rule in normalize_rules()}


@cache
def _rules_by_output() -> dict[str, NormalizeRule]:
    return {rule.output: rule for rule in normalize_rules()}


def normalize_rule(name: str) -> NormalizeRule:
    """Return a normalize rule by name.

    Returns
    -------
    NormalizeRule
        Normalize rule.
    """
    return _rules_by_name()[name]


def rule_for_output(output: str) -> NormalizeRule:
    """Return the normalize rule for a dataset output.

    Returns
    -------
    NormalizeRule
        Normalize rule.
    """
    return _rules_by_output()[output]


def normalize_rule_names() -> tuple[str, ...]:
    """Return normalize rule names in registry order.

    Returns
    -------
    tuple[str, ...]
        Normalize rule names.
    """
    return tuple(rule.name for rule in normalize_rules())


def normalize_rule_outputs() -> tuple[str, ...]:
    """Return normalize rule outputs in registry order.

    Returns
    -------
    tuple[str, ...]
        Normalize rule outputs.
    """
    return tuple(rule.output for rule in normalize_rules())


__all__ = [
    "normalize_rule",
    "normalize_rule_definitions",
    "normalize_rule_names",
    "normalize_rule_outputs",
    "normalize_rules",
    "rule_definition_table_cached",
    "rule_family_spec_table_cached",
    "rule_for_output",
]
