"""Tests for rustworkx incremental impact helpers."""

from __future__ import annotations

from relspec.incremental import relspec_rule_impact
from relspec.rules.definitions import RuleDefinition
from relspec.rustworkx_graph import build_rule_graph_from_definitions


def _rule(
    name: str,
    *,
    inputs: tuple[str, ...],
    output: str,
) -> RuleDefinition:
    return RuleDefinition(
        name=name,
        domain="cpg",
        kind="test",
        inputs=inputs,
        output=output,
        priority=100,
    )


def _sample_graph() -> tuple[RuleDefinition, ...]:
    return (
        _rule("alpha", inputs=("src1",), output="out1"),
        _rule("beta", inputs=("out1", "src2"), output="out2"),
    )


def test_relspec_rule_impact_full_refresh() -> None:
    """Full refresh should impact all rules and evidence."""
    rules = _sample_graph()
    graph = build_rule_graph_from_definitions(rules)
    impact = relspec_rule_impact(graph, changed_evidence=(), full_refresh=True)
    assert impact.full_refresh is True
    assert impact.changed_evidence == tuple(sorted(graph.evidence_idx))
    assert impact.impacted_rules == tuple(sorted(graph.rule_idx))


def test_relspec_rule_impact_filters_unknown_evidence() -> None:
    """Unknown evidence names should be ignored."""
    rules = _sample_graph()
    graph = build_rule_graph_from_definitions(rules)
    impact = relspec_rule_impact(graph, changed_evidence=("src1", "missing"))
    assert impact.full_refresh is False
    assert impact.changed_evidence == ("src1",)
    assert impact.impacted_rules == ("alpha", "beta")


def test_relspec_rule_impact_from_intermediate_output() -> None:
    """Intermediate outputs should impact downstream rules only."""
    rules = _sample_graph()
    graph = build_rule_graph_from_definitions(rules)
    impact = relspec_rule_impact(graph, changed_evidence=("out1",))
    assert impact.full_refresh is False
    assert impact.changed_evidence == ("out1",)
    assert impact.impacted_rules == ("beta",)
