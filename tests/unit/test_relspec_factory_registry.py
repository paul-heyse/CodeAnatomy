"""Unit tests for the relspec rule factory registry."""

from relspec.adapters import default_rule_factory_registry


def test_default_rule_factory_registry_domains() -> None:
    """Ensure default rule factories emit the expected domains."""
    registry = default_rule_factory_registry()
    expected = {
        "cpg": "cpg",
        "relspec": "cpg",
        "normalize": "normalize",
        "extract": "extract",
    }
    for name, domain in expected.items():
        rules = registry.rule_definitions(name)
        if not rules:
            continue
        assert all(rule.domain == domain for rule in rules)
