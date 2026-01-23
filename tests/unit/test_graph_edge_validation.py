"""Tests for graph edge validation module."""

from __future__ import annotations

from relspec.graph_edge_validation import (
    EdgeValidationResult,
    GraphValidationSummary,
    RuleValidationResult,
    ready_rules_with_column_validation,
    validate_edge_requirements,
    validate_edge_requirements_detailed,
    validate_graph_edges,
)
from relspec.rules.definitions import EvidenceSpec, RuleDefinition
from relspec.rules.evidence import EvidenceCatalog
from relspec.rustworkx_graph import build_rule_graph_from_definitions

EXPECTED_RULE_COUNT_TWO: int = 2
SUMMARY_TOTAL_RULES: int = 5
SUMMARY_VALID_RULES: int = 4
SUMMARY_INVALID_RULES: int = 1
SUMMARY_TOTAL_EDGES: int = 10
SUMMARY_VALID_EDGES: int = 9
SUMMARY_INVALID_EDGES: int = 1


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


def test_validate_edge_requirements_satisfied() -> None:
    """Validate when all required columns are present."""
    evidence = EvidenceSpec(
        sources=("src1",),
        required_columns=("col_a", "col_b"),
    )
    rules = (_rule("alpha", inputs=("src1",), output="out1", evidence=evidence),)
    graph = build_rule_graph_from_definitions(rules)

    catalog = EvidenceCatalog(sources={"src1"})
    catalog.columns_by_dataset["src1"] = {"col_a", "col_b", "col_c"}

    rule_idx = graph.rule_idx["alpha"]
    assert validate_edge_requirements(graph, rule_idx, catalog=catalog) is True


def test_validate_edge_requirements_missing_columns() -> None:
    """Validate when required columns are missing."""
    evidence = EvidenceSpec(
        sources=("src1",),
        required_columns=("col_a", "col_b"),
    )
    rules = (_rule("alpha", inputs=("src1",), output="out1", evidence=evidence),)
    graph = build_rule_graph_from_definitions(rules)

    catalog = EvidenceCatalog(sources={"src1"})
    catalog.columns_by_dataset["src1"] = {"col_a"}  # missing col_b

    rule_idx = graph.rule_idx["alpha"]
    assert validate_edge_requirements(graph, rule_idx, catalog=catalog) is False


def test_validate_edge_requirements_no_columns() -> None:
    """Pass validation when no columns are required."""
    rules = (_rule("alpha", inputs=("src1",), output="out1"),)
    graph = build_rule_graph_from_definitions(rules)

    catalog = EvidenceCatalog(sources={"src1"})

    rule_idx = graph.rule_idx["alpha"]
    assert validate_edge_requirements(graph, rule_idx, catalog=catalog) is True


def test_validate_edge_requirements_detailed() -> None:
    """Return detailed validation results."""
    evidence = EvidenceSpec(
        sources=("src1",),
        required_columns=("col_a", "col_b"),
    )
    rules = (_rule("alpha", inputs=("src1",), output="out1", evidence=evidence),)
    graph = build_rule_graph_from_definitions(rules)

    catalog = EvidenceCatalog(sources={"src1"})
    catalog.columns_by_dataset["src1"] = {"col_a"}  # missing col_b

    rule_idx = graph.rule_idx["alpha"]
    result = validate_edge_requirements_detailed(graph, rule_idx, catalog=catalog)

    assert result.rule_name == "alpha"
    assert result.is_valid is False
    assert "src1" in result.unsatisfied_edges
    assert len(result.edge_results) == 1
    assert "col_b" in result.edge_results[0].missing_columns


def test_validate_graph_edges() -> None:
    """Validate all edges in a graph."""
    evidence1 = EvidenceSpec(sources=("src1",), required_columns=("col_a",))
    evidence2 = EvidenceSpec(sources=("out1",), required_columns=("col_x",))

    rules = (
        _rule("alpha", inputs=("src1",), output="out1", evidence=evidence1),
        _rule("beta", inputs=("out1",), output="out2", evidence=evidence2),
    )
    graph = build_rule_graph_from_definitions(rules)

    catalog = EvidenceCatalog(sources={"src1"})
    catalog.columns_by_dataset["src1"] = {"col_a"}
    catalog.columns_by_dataset["out1"] = {"col_x"}

    summary = validate_graph_edges(graph, catalog=catalog)

    assert summary.total_rules == EXPECTED_RULE_COUNT_TWO
    assert summary.valid_rules == EXPECTED_RULE_COUNT_TWO
    assert summary.invalid_rules == 0


def test_validate_graph_edges_with_invalid() -> None:
    """Validate graph with some invalid edges."""
    evidence = EvidenceSpec(sources=("src1",), required_columns=("missing_col",))

    rules = (
        _rule("alpha", inputs=("src1",), output="out1", evidence=evidence),
        _rule("beta", inputs=("out1",), output="out2"),
    )
    graph = build_rule_graph_from_definitions(rules)

    catalog = EvidenceCatalog(sources={"src1"})
    catalog.columns_by_dataset["src1"] = {"col_a"}  # missing_col not present

    summary = validate_graph_edges(graph, catalog=catalog)

    assert summary.total_rules == EXPECTED_RULE_COUNT_TWO
    assert summary.invalid_rules == 1
    assert summary.valid_rules == 1


def test_ready_rules_with_column_validation() -> None:
    """Identify ready rules with column validation."""
    evidence = EvidenceSpec(sources=("src1",), required_columns=("col_a",))

    rules = (
        _rule("alpha", inputs=("src1",), output="out1", evidence=evidence),
        _rule("beta", inputs=("out1",), output="out2"),
    )
    graph = build_rule_graph_from_definitions(rules)

    catalog = EvidenceCatalog(sources={"src1"})
    catalog.columns_by_dataset["src1"] = {"col_a"}

    # No rules completed yet
    ready = ready_rules_with_column_validation(graph, catalog=catalog, completed=set())
    assert "alpha" in ready

    # After alpha completes
    catalog.columns_by_dataset["out1"] = {"some_col"}
    ready = ready_rules_with_column_validation(graph, catalog=catalog, completed={"alpha"})
    assert "beta" in ready
    assert "alpha" not in ready


def test_ready_rules_blocked_by_columns() -> None:
    """Rule blocked due to missing columns."""
    evidence = EvidenceSpec(sources=("src1",), required_columns=("missing_col",))

    rules = (_rule("alpha", inputs=("src1",), output="out1", evidence=evidence),)
    graph = build_rule_graph_from_definitions(rules)

    catalog = EvidenceCatalog(sources={"src1"})
    catalog.columns_by_dataset["src1"] = {"col_a"}  # missing_col not present

    ready = ready_rules_with_column_validation(graph, catalog=catalog, completed=set())
    assert "alpha" not in ready


def test_edge_validation_result_dataclass() -> None:
    """EdgeValidationResult fields."""
    result = EdgeValidationResult(
        source_name="table_a",
        target_rule="rule_1",
        is_valid=False,
        missing_columns=("col_x",),
        available_columns=("col_a", "col_b"),
    )
    assert result.source_name == "table_a"
    assert result.target_rule == "rule_1"
    assert result.is_valid is False
    assert "col_x" in result.missing_columns


def test_rule_validation_result_dataclass() -> None:
    """RuleValidationResult fields."""
    result = RuleValidationResult(
        rule_name="test_rule",
        is_valid=True,
        edge_results=(),
        unsatisfied_edges=(),
    )
    assert result.rule_name == "test_rule"
    assert result.is_valid is True


def test_graph_validation_summary_dataclass() -> None:
    """GraphValidationSummary fields."""
    summary = GraphValidationSummary(
        total_rules=SUMMARY_TOTAL_RULES,
        valid_rules=SUMMARY_VALID_RULES,
        invalid_rules=SUMMARY_INVALID_RULES,
        total_edges=SUMMARY_TOTAL_EDGES,
        valid_edges=SUMMARY_VALID_EDGES,
        invalid_edges=SUMMARY_INVALID_EDGES,
    )
    assert summary.total_rules == SUMMARY_TOTAL_RULES
    assert summary.invalid_edges == SUMMARY_INVALID_EDGES
