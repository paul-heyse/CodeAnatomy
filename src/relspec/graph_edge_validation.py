"""Edge-based validation for rule graphs with column-level requirements.

This module provides validation functions that check whether graph edges
have their column-level requirements satisfied by the evidence catalog.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from relspec.rustworkx_graph import GraphEdge, GraphNode, RuleGraph, RuleNode

if TYPE_CHECKING:
    from relspec.rules.evidence import EvidenceCatalog


@dataclass(frozen=True)
class EdgeValidationResult:
    """Result of validating a single edge's requirements.

    Attributes
    ----------
    source_name : str
        Name of the source evidence dataset.
    target_rule : str
        Name of the target rule.
    is_valid : bool
        Whether all requirements are satisfied.
    missing_columns : tuple[str, ...]
        Columns required but not present.
    available_columns : tuple[str, ...]
        Columns actually available in the catalog.
    """

    source_name: str
    target_rule: str
    is_valid: bool
    missing_columns: tuple[str, ...] = ()
    available_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class RuleValidationResult:
    """Result of validating all edges for a rule.

    Attributes
    ----------
    rule_name : str
        Name of the rule being validated.
    is_valid : bool
        Whether all predecessor edges are satisfied.
    edge_results : tuple[EdgeValidationResult, ...]
        Individual validation results for each edge.
    unsatisfied_edges : tuple[str, ...]
        Names of edges that failed validation.
    """

    rule_name: str
    is_valid: bool
    edge_results: tuple[EdgeValidationResult, ...] = ()
    unsatisfied_edges: tuple[str, ...] = ()


@dataclass(frozen=True)
class GraphValidationSummary:
    """Summary of validation across all rules in a graph.

    Attributes
    ----------
    total_rules : int
        Total number of rules validated.
    valid_rules : int
        Number of rules with all edges satisfied.
    invalid_rules : int
        Number of rules with unsatisfied edges.
    total_edges : int
        Total number of edges validated.
    valid_edges : int
        Number of edges with requirements satisfied.
    invalid_edges : int
        Number of edges with missing requirements.
    rule_results : tuple[RuleValidationResult, ...]
        Per-rule validation results.
    """

    total_rules: int
    valid_rules: int
    invalid_rules: int
    total_edges: int
    valid_edges: int
    invalid_edges: int
    rule_results: tuple[RuleValidationResult, ...] = ()


def validate_edge_requirements(
    graph: RuleGraph,
    rule_idx: int,
    *,
    catalog: EvidenceCatalog,
) -> bool:
    """Validate predecessor edges using GraphEdge.required_columns.

    Checks whether all columns required by incoming edges are available
    in the evidence catalog.

    Parameters
    ----------
    graph : RuleGraph
        The rule graph containing the rule.
    rule_idx : int
        Node index of the rule to validate.
    catalog : EvidenceCatalog
        Evidence catalog with available datasets and columns.

    Returns
    -------
    bool
        True if all predecessor edge requirements are satisfied.
    """
    for pred_idx in graph.graph.predecessor_indices(rule_idx):
        edge_data = graph.graph.get_edge_data(pred_idx, rule_idx)
        if not isinstance(edge_data, GraphEdge):
            continue
        if not edge_data.required_columns:
            continue
        available_cols = catalog.columns_by_dataset.get(edge_data.name, set())
        if not set(edge_data.required_columns).issubset(available_cols):
            return False
    return True


def validate_edge_requirements_detailed(
    graph: RuleGraph,
    rule_idx: int,
    *,
    catalog: EvidenceCatalog,
) -> RuleValidationResult:
    """Validate predecessor edges with detailed results.

    Similar to validate_edge_requirements but returns detailed information
    about which columns are missing from which edges.

    Parameters
    ----------
    graph : RuleGraph
        The rule graph containing the rule.
    rule_idx : int
        Node index of the rule to validate.
    catalog : EvidenceCatalog
        Evidence catalog with available datasets and columns.

    Returns
    -------
    RuleValidationResult
        Detailed validation result for the rule.

    Raises
    ------
    TypeError
        Raised when the graph node at rule_idx is not a RuleNode.
    """
    node = graph.graph[rule_idx]
    if not isinstance(node, GraphNode) or not isinstance(node.payload, RuleNode):
        msg = f"Expected RuleNode at index {rule_idx}"
        raise TypeError(msg)

    rule_name = node.payload.name
    edge_results: list[EdgeValidationResult] = []
    unsatisfied: list[str] = []

    for pred_idx in graph.graph.predecessor_indices(rule_idx):
        edge_data = graph.graph.get_edge_data(pred_idx, rule_idx)
        if not isinstance(edge_data, GraphEdge):
            continue

        available_cols = catalog.columns_by_dataset.get(edge_data.name, set())
        required_cols = set(edge_data.required_columns)
        missing_cols = required_cols - available_cols

        is_valid = len(missing_cols) == 0
        if not is_valid:
            unsatisfied.append(edge_data.name)

        edge_results.append(
            EdgeValidationResult(
                source_name=edge_data.name,
                target_rule=rule_name,
                is_valid=is_valid,
                missing_columns=tuple(sorted(missing_cols)),
                available_columns=tuple(sorted(available_cols)),
            )
        )

    return RuleValidationResult(
        rule_name=rule_name,
        is_valid=len(unsatisfied) == 0,
        edge_results=tuple(edge_results),
        unsatisfied_edges=tuple(unsatisfied),
    )


def validate_graph_edges(
    graph: RuleGraph,
    *,
    catalog: EvidenceCatalog,
) -> GraphValidationSummary:
    """Validate all edges in a rule graph against the evidence catalog.

    Parameters
    ----------
    graph : RuleGraph
        The rule graph to validate.
    catalog : EvidenceCatalog
        Evidence catalog with available datasets and columns.

    Returns
    -------
    GraphValidationSummary
        Summary of validation across all rules.
    """
    rule_results: list[RuleValidationResult] = []
    total_edges = 0
    valid_edges = 0

    for rule_idx in graph.rule_idx.values():
        result = validate_edge_requirements_detailed(graph, rule_idx, catalog=catalog)
        rule_results.append(result)
        total_edges += len(result.edge_results)
        valid_edges += sum(1 for e in result.edge_results if e.is_valid)

    valid_rules = sum(1 for r in rule_results if r.is_valid)
    invalid_rules = len(rule_results) - valid_rules
    invalid_edges = total_edges - valid_edges

    return GraphValidationSummary(
        total_rules=len(rule_results),
        valid_rules=valid_rules,
        invalid_rules=invalid_rules,
        total_edges=total_edges,
        valid_edges=valid_edges,
        invalid_edges=invalid_edges,
        rule_results=tuple(rule_results),
    )


def ready_rules_with_column_validation(
    graph: RuleGraph,
    *,
    catalog: EvidenceCatalog,
    completed: set[str],
) -> Sequence[str]:
    """Return rules that are ready for execution with column validation.

    A rule is ready if:
    1. All its predecessor rules have completed
    2. All column requirements on incoming edges are satisfied

    Parameters
    ----------
    graph : RuleGraph
        The rule graph.
    catalog : EvidenceCatalog
        Evidence catalog with available datasets and columns.
    completed : set[str]
        Set of completed rule names.

    Returns
    -------
    Sequence[str]
        Names of rules ready for execution.
    """
    ready: list[str] = []

    for rule_name, rule_idx in graph.rule_idx.items():
        if rule_name in completed:
            continue

        # Check predecessor rules are complete
        all_preds_complete = True
        for pred_idx in graph.graph.predecessor_indices(rule_idx):
            pred_node = graph.graph[pred_idx]
            if not isinstance(pred_node, GraphNode):
                continue
            if (
                pred_node.kind == "rule"
                and isinstance(pred_node.payload, RuleNode)
                and pred_node.payload.name not in completed
            ):
                all_preds_complete = False
                break

        if not all_preds_complete:
            continue

        # Check column requirements
        if validate_edge_requirements(graph, rule_idx, catalog=catalog):
            ready.append(rule_name)

    return tuple(sorted(ready))


__all__ = [
    "EdgeValidationResult",
    "GraphValidationSummary",
    "RuleValidationResult",
    "ready_rules_with_column_validation",
    "validate_edge_requirements",
    "validate_edge_requirements_detailed",
    "validate_graph_edges",
]
