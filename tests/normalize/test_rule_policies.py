"""Tests for normalize policy defaults."""

from __future__ import annotations

from normalize.rule_registry import normalize_rules
from normalize.runner import apply_policy_defaults


def test_rule_policies_have_contract_defaults() -> None:
    """Ensure normalize rules derive confidence/ambiguity policies from contracts."""
    rules = apply_policy_defaults(normalize_rules())
    missing_confidence = sorted(rule.name for rule in rules if rule.confidence_policy is None)
    missing_ambiguity = sorted(rule.name for rule in rules if rule.ambiguity_policy is None)
    assert not missing_confidence, f"Missing confidence policies for: {missing_confidence}"
    assert not missing_ambiguity, f"Missing ambiguity policies for: {missing_ambiguity}"
