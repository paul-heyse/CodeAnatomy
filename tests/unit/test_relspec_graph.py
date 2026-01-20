"""Tests for relspec graph helpers."""

from dataclasses import dataclass

import pytest

from relspec.graph import RuleSelectors, order_rules_by_evidence, rule_graph_signature
from relspec.rules.definitions import EvidenceSpec
from relspec.rules.evidence import EvidenceCatalog


@dataclass(frozen=True)
class DummyRule:
    """Minimal rule model for graph ordering tests."""

    name: str
    output: str
    inputs: tuple[str, ...]
    priority: int = 100
    evidence: EvidenceSpec | None = None


def _selectors() -> RuleSelectors[DummyRule]:
    return RuleSelectors(
        inputs_for=lambda rule: rule.inputs,
        output_for=lambda rule: rule.output,
        name_for=lambda rule: rule.name,
        priority_for=lambda rule: rule.priority,
        evidence_for=lambda rule: rule.evidence,
    )


def test_order_rules_by_evidence_prefers_priority() -> None:
    """Select the lowest-priority rule per output."""
    rules = (
        DummyRule(name="first", output="out", inputs=("src",), priority=2),
        DummyRule(name="second", output="out", inputs=("src",), priority=1),
        DummyRule(name="third", output="other", inputs=("src",), priority=0),
    )
    evidence = EvidenceCatalog(sources={"src"})
    ordered = order_rules_by_evidence(
        rules,
        evidence=evidence,
        selectors=_selectors(),
    )
    assert [rule.name for rule in ordered] == ["third", "second"]


def test_order_rules_by_evidence_requires_sources() -> None:
    """Raise when evidence sources are missing."""
    rules = (DummyRule(name="rule", output="out", inputs=("src",)),)
    evidence = EvidenceCatalog(sources=set())
    with pytest.raises(ValueError, match="cannot resolve evidence"):
        order_rules_by_evidence(
            rules,
            evidence=evidence,
            selectors=_selectors(),
        )


def test_rule_graph_signature_stable() -> None:
    """Signature is stable regardless of rule ordering."""
    rules = (
        DummyRule(name="alpha", output="out", inputs=()),
        DummyRule(name="beta", output="other", inputs=()),
    )
    signature = rule_graph_signature(
        rules,
        name_for=lambda rule: rule.name,
        signature_for=lambda rule: f"sig:{rule.name}",
        label="test",
    )
    reversed_signature = rule_graph_signature(
        tuple(reversed(rules)),
        name_for=lambda rule: rule.name,
        signature_for=lambda rule: f"sig:{rule.name}",
        label="test",
    )
    assert signature == reversed_signature
