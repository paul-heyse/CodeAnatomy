"""Default rule-family specs for normalize rule generation."""

from __future__ import annotations

from functools import cache

from normalize.rule_specs import NormalizeRuleFamilySpec
from normalize.rule_template_specs import expand_rule_templates, rule_template_specs

RULE_FAMILY_SPECS: tuple[NormalizeRuleFamilySpec, ...] = expand_rule_templates(
    rule_template_specs()
)


@cache
def rule_family_specs() -> tuple[NormalizeRuleFamilySpec, ...]:
    """Return default rule-family specs.

    Returns
    -------
    tuple[NormalizeRuleFamilySpec, ...]
        Rule family specification entries.
    """
    return RULE_FAMILY_SPECS


__all__ = ["NormalizeRuleFamilySpec", "rule_family_specs"]
