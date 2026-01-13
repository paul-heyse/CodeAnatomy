"""Normalize rule registry and lookup helpers."""

from __future__ import annotations

from normalize.rule_factories import (
    bytecode_rules,
    diagnostics_rules,
    span_error_rules,
    type_rules,
)
from normalize.rule_model import NormalizeRule

NORMALIZE_RULES: tuple[NormalizeRule, ...] = (
    *type_rules(),
    *bytecode_rules(),
    *diagnostics_rules(),
    *span_error_rules(),
)

_RULES_BY_NAME: dict[str, NormalizeRule] = {rule.name: rule for rule in NORMALIZE_RULES}
_RULES_BY_OUTPUT: dict[str, NormalizeRule] = {rule.output: rule for rule in NORMALIZE_RULES}


def normalize_rules() -> tuple[NormalizeRule, ...]:
    """Return the normalize rules in registry order.

    Returns
    -------
    tuple[NormalizeRule, ...]
        Normalize rule registry.
    """
    return NORMALIZE_RULES


def normalize_rule(name: str) -> NormalizeRule:
    """Return a normalize rule by name.

    Returns
    -------
    NormalizeRule
        Normalize rule.
    """
    return _RULES_BY_NAME[name]


def rule_for_output(output: str) -> NormalizeRule:
    """Return the normalize rule for a dataset output.

    Returns
    -------
    NormalizeRule
        Normalize rule.
    """
    return _RULES_BY_OUTPUT[output]


def normalize_rule_names() -> tuple[str, ...]:
    """Return normalize rule names in registry order.

    Returns
    -------
    tuple[str, ...]
        Normalize rule names.
    """
    return tuple(rule.name for rule in NORMALIZE_RULES)


def normalize_rule_outputs() -> tuple[str, ...]:
    """Return normalize rule outputs in registry order.

    Returns
    -------
    tuple[str, ...]
        Normalize rule outputs.
    """
    return tuple(rule.output for rule in NORMALIZE_RULES)


__all__ = [
    "normalize_rule",
    "normalize_rule_names",
    "normalize_rule_outputs",
    "normalize_rules",
    "rule_for_output",
]
