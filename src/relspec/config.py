"""Configuration helpers for relspec policy injection."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ibis_engine.param_tables import ParamTablePolicy
from relspec.list_filter_gate import ListFilterGatePolicy
from relspec.pipeline_policy import KernelLanePolicy

if TYPE_CHECKING:
    from relspec.rustworkx_graph import RuleGraph


# -----------------------------------------------------------------------------
# Feature Flags for Calculation-Driven Scheduling Migration
# -----------------------------------------------------------------------------

# Phase 2: Use inferred dependencies for scheduling instead of declared
USE_INFERRED_DEPS: bool = True

# Threshold for significant order changes in migration validation
_SIGNIFICANT_ORDER_CHANGE_THRESHOLD: int = 2

# Phase 1: Log mismatches between declared and inferred dependencies
COMPARE_DECLARED_INFERRED: bool = True

# Phase 4: Generate Hamilton DAG modules from rustworkx graphs
HAMILTON_DAG_OUTPUT: bool = True


@dataclass(frozen=True)
class RelspecConfig:
    """Centralized configuration bundle for relspec rule wiring."""

    param_table_policy: ParamTablePolicy | None = None
    list_filter_gate_policy: ListFilterGatePolicy | None = None
    kernel_lane_policy: KernelLanePolicy | None = None


@dataclass(frozen=True)
class InferredDepsConfig:
    """Configuration for inferred dependencies feature.

    Attributes
    ----------
    use_inferred_deps : bool
        Use inferred dependencies for scheduling.
    compare_declared_inferred : bool
        Log comparison between declared and inferred.
    hamilton_dag_output : bool
        Generate Hamilton DAG modules.
    log_level : str
        Log level for mismatch warnings.
    allowlist : tuple[str, ...]
        Rules exempt from mismatch warnings.
    """

    use_inferred_deps: bool = USE_INFERRED_DEPS
    compare_declared_inferred: bool = COMPARE_DECLARED_INFERRED
    hamilton_dag_output: bool = HAMILTON_DAG_OUTPUT
    log_level: str = "WARNING"
    allowlist: tuple[str, ...] = ()


@dataclass(frozen=True)
class MigrationReport:
    """Report from migration safety validation.

    Attributes
    ----------
    is_safe : bool
        Whether migration is considered safe.
    topological_order_match : bool
        Whether topological orders match.
    declared_rule_count : int
        Number of rules in declared graph.
    inferred_rule_count : int
        Number of rules in inferred graph.
    rules_added : tuple[str, ...]
        Rules in inferred but not declared.
    rules_removed : tuple[str, ...]
        Rules in declared but not inferred.
    order_differences : Mapping[str, tuple[int, int]]
        Rules with different positions: name -> (declared_pos, inferred_pos).
    warnings : tuple[str, ...]
        Warning messages about potential regressions.
    """

    is_safe: bool
    topological_order_match: bool
    declared_rule_count: int
    inferred_rule_count: int
    rules_added: tuple[str, ...] = ()
    rules_removed: tuple[str, ...] = ()
    order_differences: Mapping[str, tuple[int, int]] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()


def validate_migration_safety(
    declared_graph: RuleGraph,
    inferred_graph: RuleGraph,
) -> MigrationReport:
    """Compare topological order between declared and inferred graphs.

    Used to validate that switching from declared to inferred dependencies
    won't introduce execution order regressions.

    Parameters
    ----------
    declared_graph : RuleGraph
        Graph built from declared dependencies.
    inferred_graph : RuleGraph
        Graph built from inferred dependencies.

    Returns
    -------
    MigrationReport
        Comparison results and safety assessment.
    """
    import rustworkx as rx

    from relspec.rustworkx_graph import GraphNode, RuleNode

    # Extract topological orders
    def _get_rule_order(graph: RuleGraph) -> dict[str, int]:
        order: dict[str, int] = {}
        try:
            sorted_indices = rx.topological_sort(graph.graph)
            position = 0
            for idx in sorted_indices:
                node = graph.graph[idx]
                if isinstance(node, GraphNode) and isinstance(node.payload, RuleNode):
                    order[node.payload.name] = position
                    position += 1
        except rx.DAGHasCycle:
            # Graph has cycles, use index order
            order.update(graph.rule_idx)
        return order

    declared_order = _get_rule_order(declared_graph)
    inferred_order = _get_rule_order(inferred_graph)

    declared_names = set(declared_order.keys())
    inferred_names = set(inferred_order.keys())

    rules_added = tuple(sorted(inferred_names - declared_names))
    rules_removed = tuple(sorted(declared_names - inferred_names))

    # Check order differences for common rules
    order_diffs: dict[str, tuple[int, int]] = {}
    for name in declared_names & inferred_names:
        d_pos = declared_order[name]
        i_pos = inferred_order[name]
        if d_pos != i_pos:
            order_diffs[name] = (d_pos, i_pos)

    # Build warnings
    warnings: list[str] = []
    if rules_removed:
        warnings.append(
            f"Rules removed in inferred graph: {list(rules_removed)}"
        )
    if rules_added:
        warnings.append(
            f"Rules added in inferred graph: {list(rules_added)}"
        )

    # Check for significant order changes (more than threshold positions)
    significant_changes = [
        name for name, (d, i) in order_diffs.items()
        if abs(d - i) > _SIGNIFICANT_ORDER_CHANGE_THRESHOLD
    ]
    if significant_changes:
        warnings.append(
            f"Significant order changes (>2 positions): {significant_changes}"
        )

    # Determine safety
    topological_match = len(order_diffs) == 0
    is_safe = (
        len(rules_removed) == 0
        and len(significant_changes) == 0
        and topological_match
    )

    return MigrationReport(
        is_safe=is_safe,
        topological_order_match=topological_match,
        declared_rule_count=len(declared_order),
        inferred_rule_count=len(inferred_order),
        rules_added=rules_added,
        rules_removed=rules_removed,
        order_differences=order_diffs,
        warnings=tuple(warnings),
    )


def get_inferred_deps_config() -> InferredDepsConfig:
    """Return current inferred dependencies configuration.

    Returns
    -------
    InferredDepsConfig
        Current configuration from module-level flags.
    """
    return InferredDepsConfig(
        use_inferred_deps=USE_INFERRED_DEPS,
        compare_declared_inferred=COMPARE_DECLARED_INFERRED,
        hamilton_dag_output=HAMILTON_DAG_OUTPUT,
    )


__all__ = [
    "COMPARE_DECLARED_INFERRED",
    "HAMILTON_DAG_OUTPUT",
    "USE_INFERRED_DEPS",
    "InferredDepsConfig",
    "MigrationReport",
    "RelspecConfig",
    "get_inferred_deps_config",
    "validate_migration_safety",
]
