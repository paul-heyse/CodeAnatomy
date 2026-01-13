"""Tests for normalize rule family construction."""

from __future__ import annotations

from normalize.registry_specs import dataset_names
from normalize.rule_registry import normalize_rules


def test_rule_family_outputs_registered() -> None:
    """Validate that normalize rule outputs are registered datasets."""
    rules = normalize_rules()
    outputs = {rule.output for rule in rules}
    assert outputs
    registered = set(dataset_names())
    missing = sorted(outputs - registered)
    assert not missing, f"Missing dataset specs for: {missing}"
    assert len(outputs) == len(rules)
