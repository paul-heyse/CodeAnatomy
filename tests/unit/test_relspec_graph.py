"""Tests for rustworkx-backed relspec graphs."""

from __future__ import annotations

import pytest

from relspec.errors import RelspecValidationError
from relspec.rules.definitions import EvidenceSpec, RuleDefinition
from relspec.rules.evidence import EvidenceCatalog
from relspec.rustworkx_graph import (
    GraphEdge,
    build_rule_graph_from_definitions,
    rule_graph_diagnostics,
    rule_graph_signature_from_definitions,
)
from relspec.rustworkx_schedule import impacted_rules, provenance_for_rule, schedule_rules

EXPECTED_NODE_COUNT = 6
EXPECTED_EDGE_COUNT = 5


def _rule(
    name: str,
    *,
    inputs: tuple[str, ...],
    output: str,
    priority: int = 100,
    evidence: EvidenceSpec | None = None,
) -> RuleDefinition:
    return RuleDefinition(
        name=name,
        domain="cpg",
        kind="test",
        inputs=inputs,
        output=output,
        priority=priority,
        evidence=evidence,
    )


def test_rule_graph_builds_edges_with_requirements() -> None:
    """Build a rule graph with evidence requirements on edges."""
    evidence = EvidenceSpec(
        sources=("src1",),
        required_columns=("col_a",),
        required_types={"col_a": "string"},
        required_metadata={b"meta": b"value"},
    )
    rules = (
        _rule("alpha", inputs=("src1",), output="out1", priority=10, evidence=evidence),
        _rule("beta", inputs=("out1", "src2"), output="out2", priority=20),
    )
    graph = build_rule_graph_from_definitions(rules)
    assert graph.graph.num_nodes() == EXPECTED_NODE_COUNT
    assert graph.graph.num_edges() == EXPECTED_EDGE_COUNT

    requires = [
        payload
        for _, _, payload in graph.graph.weighted_edge_list()
        if isinstance(payload, GraphEdge) and payload.kind == "requires" and payload.name == "src1"
    ]
    assert len(requires) == 1
    edge = requires[0]
    assert edge.required_columns == ("col_a",)
    assert edge.required_types == (("col_a", "string"),)
    assert edge.required_metadata == ((b"meta", b"value"),)

    produces = [
        payload
        for _, _, payload in graph.graph.weighted_edge_list()
        if isinstance(payload, GraphEdge) and payload.kind == "produces"
    ]
    assert all(payload.required_columns == () for payload in produces)


def test_rule_graph_signature_stable() -> None:
    """Signature is stable regardless of rule ordering."""
    rules = (
        _rule("alpha", inputs=(), output="out1"),
        _rule("beta", inputs=(), output="out2"),
    )
    signatures = {rule.name: f"sig:{rule.name}" for rule in rules}
    signature = rule_graph_signature_from_definitions(
        rules,
        label="test",
        rule_signatures=signatures,
    )
    reversed_signature = rule_graph_signature_from_definitions(
        tuple(reversed(rules)),
        label="test",
        rule_signatures=signatures,
    )
    assert signature == reversed_signature


def test_schedule_rules_orders_by_priority() -> None:
    """Schedule rules deterministically by priority and name."""
    rules = (
        _rule("alpha", inputs=("src",), output="out1", priority=10),
        _rule("beta", inputs=("src",), output="out2", priority=5),
    )
    graph = build_rule_graph_from_definitions(rules)
    evidence = EvidenceCatalog(sources={"src"})
    schedule = schedule_rules(graph, evidence=evidence)
    assert schedule.ordered_rules == ("beta", "alpha")
    assert schedule.generations == (("beta", "alpha"),)


def test_schedule_rules_requires_evidence() -> None:
    """Raise when evidence sources are missing."""
    rules = (_rule("alpha", inputs=("src",), output="out1"),)
    graph = build_rule_graph_from_definitions(rules)
    evidence = EvidenceCatalog(sources=set())
    with pytest.raises(RelspecValidationError, match="cannot resolve evidence"):
        schedule_rules(graph, evidence=evidence)


def test_rule_graph_diagnostics_ok() -> None:
    """Provide diagnostics and critical path metadata for DAGs."""
    rules = (
        _rule("alpha", inputs=("src1",), output="out1"),
        _rule("beta", inputs=("out1", "src2"), output="out2"),
    )
    graph = build_rule_graph_from_definitions(rules)
    diagnostics = rule_graph_diagnostics(graph)
    assert diagnostics.status == "ok"
    assert diagnostics.dot is not None
    assert diagnostics.critical_path_length is not None


def test_rule_graph_diagnostics_cycle() -> None:
    """Detect cycles in rule graphs."""
    rules = (
        _rule("alpha", inputs=("out2",), output="out1"),
        _rule("beta", inputs=("out1",), output="out2"),
    )
    graph = build_rule_graph_from_definitions(rules)
    diagnostics = rule_graph_diagnostics(graph)
    assert diagnostics.status == "cycle"
    assert diagnostics.cycles


def test_impacted_rules_and_provenance() -> None:
    """Compute impact and provenance for evidence changes."""
    rules = (
        _rule("alpha", inputs=("src1",), output="out1"),
        _rule("beta", inputs=("out1", "src2"), output="out2"),
    )
    graph = build_rule_graph_from_definitions(rules)
    assert impacted_rules(graph, evidence_name="src1") == ("alpha", "beta")
    assert provenance_for_rule(graph, rule_name="beta") == ("out1", "src1", "src2")
