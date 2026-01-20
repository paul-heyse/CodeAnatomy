"""Default rule-family specs for normalize rule generation."""

from __future__ import annotations

from functools import cache

from normalize.rule_factories import build_rule_definitions_from_specs
from relspec.normalize.rule_specs import NormalizeRuleFamilySpec
from relspec.normalize.rule_template_specs import expand_rule_templates, rule_template_specs
from relspec.rules.decorators import rule_bundle
from relspec.rules.definitions import RuleDefinition


@cache
def rule_family_specs() -> tuple[NormalizeRuleFamilySpec, ...]:
    """Return default rule-family specs.

    Returns
    -------
    tuple[NormalizeRuleFamilySpec, ...]
        Rule family specification entries.
    """
    return expand_rule_templates(rule_template_specs())


@rule_bundle(name="relspec.normalize", domain="normalize")
@cache
def normalize_rule_definitions() -> tuple[RuleDefinition, ...]:
    """Return normalize rule definitions for registry use.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Normalize rule definitions.
    """
    return build_rule_definitions_from_specs(rule_family_specs())


__all__ = ["NormalizeRuleFamilySpec", "normalize_rule_definitions", "rule_family_specs"]
