"""Unit tests for plan-statistics-driven scheduling in execution_plan."""

from __future__ import annotations

import rustworkx as rx

from relspec.evidence import EvidenceCatalog
from relspec.execution_planning_runtime import (
    TaskPlanMetrics,
    bottom_level_costs,
    derive_task_costs_from_plan,
    task_slack_by_task,
)
from relspec.rustworkx_graph import (
    EvidenceNode,
    GraphEdge,
    GraphNode,
    TaskGraph,
    TaskNode,
    task_dependency_graph,
)
from relspec.rustworkx_schedule import (
    ScheduleCostContext,
    ScheduleOptions,
    schedule_tasks,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_task_node(name: str, *, priority: int = 1) -> TaskNode:
    """Build a minimal TaskNode for testing.

    Parameters
    ----------
    name
        Task name.
    priority
        Task priority (default 1).

    Returns:
    -------
    TaskNode
        Minimal task node.
    """
    return TaskNode(
        name=name,
        output=name,
        inputs=(),
        sources=(),
        priority=priority,
        task_kind="semantic",
    )


def _linear_dependency_graph(
    names: list[str],
) -> rx.PyDiGraph:
    """Build a linear chain A -> B -> C from the given task names.

    Nodes are ``TaskNode`` objects so that ``bottom_level_costs`` and
    ``task_slack_by_task`` can extract names and priorities.

    Parameters
    ----------
    names
        Ordered task names forming the chain.

    Returns:
    -------
    rx.PyDiGraph
        Linear dependency graph.
    """
    graph: rx.PyDiGraph = rx.PyDiGraph(multigraph=False, check_cycle=True)
    indices = {name: graph.add_node(_make_task_node(name)) for name in names}
    for i in range(len(names) - 1):
        graph.add_edge(indices[names[i]], indices[names[i + 1]], (names[i],))
    return graph


def _diamond_dependency_graph() -> rx.PyDiGraph:
    """Build a diamond graph: A -> B, A -> C, B -> D, C -> D.

    Returns:
    -------
    rx.PyDiGraph
        Diamond dependency graph with four tasks.
    """
    graph: rx.PyDiGraph = rx.PyDiGraph(multigraph=False, check_cycle=True)
    a = graph.add_node(_make_task_node("A"))
    b = graph.add_node(_make_task_node("B"))
    c = graph.add_node(_make_task_node("C"))
    d = graph.add_node(_make_task_node("D"))
    graph.add_edge(a, b, ("A",))
    graph.add_edge(a, c, ("A",))
    graph.add_edge(b, d, ("B",))
    graph.add_edge(c, d, ("C",))
    return graph


# ---------------------------------------------------------------------------
# derive_task_costs_from_plan - priority cascade
# ---------------------------------------------------------------------------


class TestDeriveTaskCostsFromPlanCascade:
    """Verify the cost derivation priority cascade."""

    def test_uses_duration_ms(self) -> None:
        """Use duration_ms as the highest-priority cost source."""
        metrics = {
            "task_a": TaskPlanMetrics(
                duration_ms=42.0,
                output_rows=100,
                stats_row_count=200,
            ),
        }
        costs = derive_task_costs_from_plan(metrics)
        assert "task_a" in costs
        # duration_ms=42 is the base; physical adjustments may add to it
        assert costs["task_a"] >= 42.0

    def test_uses_output_rows_when_no_duration(self) -> None:
        """Fall back to output_rows when duration_ms is absent."""
        metrics = {
            "task_a": TaskPlanMetrics(output_rows=500),
        }
        costs = derive_task_costs_from_plan(metrics)
        assert "task_a" in costs
        assert costs["task_a"] >= 500.0

    def test_uses_stats_row_count(self) -> None:
        """Fall back to stats_row_count when higher-priority fields are absent."""
        metrics = {
            "task_a": TaskPlanMetrics(
                stats_row_count=1000,
                stats_available=True,
            ),
        }
        costs = derive_task_costs_from_plan(metrics)
        assert "task_a" in costs
        assert costs["task_a"] >= 1000.0

    def test_uses_stats_total_bytes(self) -> None:
        """Fall back to stats_total_bytes (converted to MB) when row count absent."""
        metrics = {
            "task_a": TaskPlanMetrics(
                stats_total_bytes=10 * 1048576,  # 10 MB
                stats_available=True,
            ),
        }
        costs = derive_task_costs_from_plan(metrics)
        assert "task_a" in costs
        assert costs["task_a"] >= 10.0

    def test_uses_partition_count(self) -> None:
        """Fall back to partition_count when all higher-priority fields are absent."""
        metrics = {
            "task_a": TaskPlanMetrics(partition_count=8),
        }
        costs = derive_task_costs_from_plan(metrics)
        assert "task_a" in costs
        # Base is partition_count=8, plus physical adjustment adds partition_count again
        assert costs["task_a"] >= 8.0

    def test_uses_repartition_count(self) -> None:
        """Fall back to repartition_count as the lowest-priority source."""
        metrics = {
            "task_a": TaskPlanMetrics(repartition_count=3),
        }
        costs = derive_task_costs_from_plan(metrics)
        assert "task_a" in costs
        assert costs["task_a"] >= 3.0


# ---------------------------------------------------------------------------
# derive_task_costs_from_plan - edge cases
# ---------------------------------------------------------------------------


class TestDeriveTaskCostsFromPlanEdgeCases:
    """Verify edge cases for the public cost derivation function."""

    def test_empty_metrics_returns_empty(self) -> None:
        """Return an empty dict when no plan metrics are provided."""
        costs = derive_task_costs_from_plan({})
        assert costs == {}

    def test_no_usable_stats_skips_task(self) -> None:
        """Skip tasks where all metric fields are None."""
        metrics = {
            "task_a": TaskPlanMetrics(),
        }
        costs = derive_task_costs_from_plan(metrics)
        assert "task_a" not in costs

    def test_multiple_tasks(self) -> None:
        """Derive costs for multiple tasks independently."""
        metrics = {
            "task_a": TaskPlanMetrics(duration_ms=10.0),
            "task_b": TaskPlanMetrics(output_rows=500),
            "task_c": TaskPlanMetrics(),
        }
        costs = derive_task_costs_from_plan(metrics)
        assert "task_a" in costs
        assert "task_b" in costs
        assert "task_c" not in costs

    def test_physical_adjustments_add_repartition_boost(self) -> None:
        """Verify repartition_count adds a significant boost to base cost."""
        base_only = derive_task_costs_from_plan(
            {
                "task_a": TaskPlanMetrics(duration_ms=10.0),
            }
        )
        with_repart = derive_task_costs_from_plan(
            {
                "task_a": TaskPlanMetrics(duration_ms=10.0, repartition_count=5),
            }
        )
        # Repartition adds repartition_count * 10.0
        assert with_repart["task_a"] > base_only["task_a"]
        assert with_repart["task_a"] >= base_only["task_a"] + 50.0

    def test_physical_adjustments_add_partition_boost(self) -> None:
        """Verify partition_count adds to the base cost."""
        base_only = derive_task_costs_from_plan(
            {
                "task_a": TaskPlanMetrics(duration_ms=100.0),
            }
        )
        with_partitions = derive_task_costs_from_plan(
            {
                "task_a": TaskPlanMetrics(duration_ms=100.0, partition_count=16),
            }
        )
        assert with_partitions["task_a"] > base_only["task_a"]
        assert with_partitions["task_a"] >= base_only["task_a"] + 16.0


# ---------------------------------------------------------------------------
# bottom_level_costs with derived costs
# ---------------------------------------------------------------------------


class TestBottomLevelCostsWithDerivedCosts:
    """Verify that bottom_level_costs uses derived costs when provided."""

    def test_uniform_vs_derived_costs_differ(self) -> None:
        """Bottom-level costs differ when using derived vs uniform costs."""
        graph = _linear_dependency_graph(["A", "B", "C"])
        uniform_costs = bottom_level_costs(graph)
        derived = derive_task_costs_from_plan(
            {
                "A": TaskPlanMetrics(duration_ms=100.0),
                "B": TaskPlanMetrics(duration_ms=10.0),
                "C": TaskPlanMetrics(duration_ms=1.0),
            }
        )
        derived_costs = bottom_level_costs(graph, task_costs=derived)
        # With uniform costs all nodes have priority-based cost (1.0 each)
        # With derived costs, A has 100, B has 10, C has 1
        assert derived_costs["A"] > uniform_costs["A"]

    def test_leaf_node_cost_matches_derived(self) -> None:
        """Leaf node bottom-level cost equals its own derived cost."""
        graph = _linear_dependency_graph(["A", "B"])
        derived = derive_task_costs_from_plan(
            {
                "A": TaskPlanMetrics(duration_ms=50.0),
                "B": TaskPlanMetrics(duration_ms=200.0),
            }
        )
        bl_costs = bottom_level_costs(graph, task_costs=derived)
        # B is the leaf; its bottom-level cost should equal its own cost
        assert bl_costs["B"] == derived["B"]


# ---------------------------------------------------------------------------
# task_slack_by_task with derived costs
# ---------------------------------------------------------------------------


class TestTaskSlackWithDerivedCosts:
    """Verify that task_slack_by_task uses derived costs when provided."""

    def test_linear_chain_zero_slack(self) -> None:
        """All tasks on a linear chain have zero slack."""
        graph = _linear_dependency_graph(["A", "B", "C"])
        derived = derive_task_costs_from_plan(
            {
                "A": TaskPlanMetrics(duration_ms=100.0),
                "B": TaskPlanMetrics(duration_ms=10.0),
                "C": TaskPlanMetrics(duration_ms=1.0),
            }
        )
        slack = task_slack_by_task(graph, task_costs=derived)
        for name in ["A", "B", "C"]:
            assert slack[name] == 0.0, f"Task {name} should have zero slack on linear chain"

    def test_diamond_parallel_paths_have_slack(self) -> None:
        """In a diamond graph, the shorter parallel path has positive slack."""
        graph = _diamond_dependency_graph()
        derived = derive_task_costs_from_plan(
            {
                "A": TaskPlanMetrics(duration_ms=10.0),
                "B": TaskPlanMetrics(duration_ms=100.0),  # Long path
                "C": TaskPlanMetrics(duration_ms=1.0),  # Short path
                "D": TaskPlanMetrics(duration_ms=10.0),
            }
        )
        slack = task_slack_by_task(graph, task_costs=derived)
        # C is on the shorter path (cost ~1) vs B (cost ~100)
        # So C should have positive slack
        assert slack["C"] > 0.0
        # B is on the critical path, should have zero slack
        assert slack["B"] == 0.0


# ---------------------------------------------------------------------------
# Schedule order: uniform vs statistics-based costs
# ---------------------------------------------------------------------------


def _build_diamond_task_graph() -> tuple[TaskGraph, EvidenceCatalog]:
    """Build a diamond TaskGraph with evidence nodes for schedule tests.

    Returns:
    -------
    tuple[TaskGraph, EvidenceCatalog]
        ``(task_graph, evidence)`` pair ready for scheduling.
    """
    g: rx.PyDiGraph = rx.PyDiGraph(multigraph=False, check_cycle=True)

    ev_input = g.add_node(GraphNode(kind="evidence", payload=EvidenceNode(name="input_data")))

    idx_b = g.add_node(GraphNode(kind="task", payload=_make_task_node("B", priority=1)))
    idx_c = g.add_node(GraphNode(kind="task", payload=_make_task_node("C", priority=1)))
    idx_d = g.add_node(GraphNode(kind="task", payload=_make_task_node("D", priority=1)))

    ev_b = g.add_node(GraphNode(kind="evidence", payload=EvidenceNode(name="B")))
    ev_c = g.add_node(GraphNode(kind="evidence", payload=EvidenceNode(name="C")))
    ev_d = g.add_node(GraphNode(kind="evidence", payload=EvidenceNode(name="D")))

    g.add_edge(ev_input, idx_b, GraphEdge(kind="requires", name="input_data", inferred=True))
    g.add_edge(ev_input, idx_c, GraphEdge(kind="requires", name="input_data", inferred=True))
    g.add_edge(idx_b, ev_b, GraphEdge(kind="produces", name="B", inferred=True))
    g.add_edge(idx_c, ev_c, GraphEdge(kind="produces", name="C", inferred=True))
    g.add_edge(ev_b, idx_d, GraphEdge(kind="requires", name="B", inferred=True))
    g.add_edge(ev_c, idx_d, GraphEdge(kind="requires", name="C", inferred=True))
    g.add_edge(idx_d, ev_d, GraphEdge(kind="produces", name="D", inferred=True))

    task_graph = TaskGraph(
        graph=g,
        evidence_idx={"input_data": ev_input, "B": ev_b, "C": ev_c, "D": ev_d},
        task_idx={"B": idx_b, "C": idx_c, "D": idx_d},
        output_policy="all_producers",
    )

    evidence = EvidenceCatalog()
    evidence.sources.add("input_data")
    return task_graph, evidence


def _schedule_with_cost_context(
    task_graph: TaskGraph,
    evidence: EvidenceCatalog,
    cost_context: ScheduleCostContext | None,
) -> tuple[str, ...]:
    """Run the scheduler with a given cost context.

    Returns:
    -------
    tuple[str, ...]
        Ordered task names from the schedule.
    """
    ctx = cost_context if cost_context is not None else ScheduleCostContext()
    schedule = schedule_tasks(
        task_graph,
        evidence=evidence,
        options=ScheduleOptions(cost_context=ctx),
    )
    return schedule.ordered_tasks


class TestScheduleOrderUniformVsStatistics:
    """Verify that statistics-based costs produce a different schedule order."""

    def test_schedule_order_differs_with_statistics(self) -> None:
        """Schedule order changes when statistics-derived costs are used.

        Construct a diamond graph where task B has a very high cost and task C
        has a very low cost.  With uniform costs, the scheduler uses name-based
        tiebreaking.  With statistics-based costs, B should be prioritized over
        C due to its higher bottom-level cost contribution.
        """
        task_graph, evidence = _build_diamond_task_graph()

        # Schedule with uniform costs
        uniform_order = _schedule_with_cost_context(task_graph, evidence, None)

        # Schedule with statistics: B is very expensive, C is very cheap
        plan_metrics = {
            "B": TaskPlanMetrics(stats_row_count=1_000_000),
            "C": TaskPlanMetrics(stats_row_count=100),
            "D": TaskPlanMetrics(stats_row_count=500),
        }
        derived = derive_task_costs_from_plan(plan_metrics)
        dep_graph = task_dependency_graph(task_graph)
        bl_costs = bottom_level_costs(dep_graph, task_costs=derived)
        slack = task_slack_by_task(dep_graph, task_costs=derived)

        stats_order = _schedule_with_cost_context(
            task_graph,
            evidence,
            ScheduleCostContext(
                task_costs=derived,
                bottom_level_costs=bl_costs,
                slack_by_task=slack,
            ),
        )

        # Both schedules must contain the same tasks
        assert set(uniform_order) == set(stats_order)

        # With statistics, B (1M rows) should be scheduled before C (100 rows)
        stats_list = list(stats_order)
        b_idx = stats_list.index("B")
        c_idx = stats_list.index("C")
        assert b_idx < c_idx, (
            f"Expected B (1M rows) before C (100 rows) in stats schedule, got order: {stats_list}"
        )
