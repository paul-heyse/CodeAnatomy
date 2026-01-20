"""Tests for normalize rule definition round-trips."""

from normalize.rule_factories import build_rule_definitions_from_specs
from relspec.normalize.rule_registry_specs import rule_family_specs
from relspec.rules.spec_tables import rule_definition_table, rule_definitions_from_table


def test_normalize_rule_definition_round_trip() -> None:
    """Ensure normalize rule definitions round-trip through spec tables."""
    rules = build_rule_definitions_from_specs(rule_family_specs())
    table = rule_definition_table(rules)
    round_tripped = rule_definitions_from_table(table)
    by_name = {rule.name: rule for rule in rules}
    round_by_name = {rule.name: rule for rule in round_tripped}
    assert by_name.keys() == round_by_name.keys()
    for name, rule in by_name.items():
        assert rule == round_by_name[name]
