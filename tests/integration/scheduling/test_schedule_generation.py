"""Schedule generation integration tests.

Scope: Validate topologically-ordered schedule generation from
task graphs. Tests exercise schedule_tasks() behavior under
various graph topologies and options.

Key API references:
- schedule_tasks() in relspec.rustworkx_schedule (line 60)
- TaskSchedule in relspec.rustworkx_schedule (line 28)
- ScheduleOptions in relspec.rustworkx_schedule (line 48)
- TaskGraph, GraphNode, NodeKind in relspec.rustworkx_graph
- build_task_graph_from_inferred_deps() in relspec.rustworkx_graph
- EvidenceCatalog in relspec.evidence
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from relspec.evidence import EvidenceCatalog
from relspec.inferred_deps import InferredDeps
from relspec.rustworkx_graph import (
    TaskGraph,
    build_task_graph_from_inferred_deps,
)
from relspec.rustworkx_schedule import (
    ScheduleCostContext,
    ScheduleOptions,
    TaskSchedule,
    schedule_tasks,
)


@pytest.fixture
def simple_linear_deps() -> tuple[InferredDeps, ...]:
    """Two tasks with linear dependency: task_a -> ev_a -> task_b.

    Returns:
    -------
    tuple[InferredDeps, ...]
        Dependency set encoding a simple linear chain.
    """
    return (
        InferredDeps(
            task_name="task_a",
            output="ev_a",
            inputs=(),
            plan_fingerprint="fp-a",
        ),
        InferredDeps(
            task_name="task_b",
            output="ev_b",
            inputs=("ev_a",),
            required_columns={"ev_a": ("col_x",)},
            plan_fingerprint="fp-b",
        ),
    )


@pytest.fixture
def independent_deps() -> tuple[InferredDeps, ...]:
    """Three independent tasks with no inter-dependencies.

    Returns:
    -------
    tuple[InferredDeps, ...]
        Dependency set containing three independent tasks.
    """
    return (
        InferredDeps(
            task_name="task_x",
            output="ev_x",
            inputs=(),
            plan_fingerprint="fp-x",
        ),
        InferredDeps(
            task_name="task_y",
            output="ev_y",
            inputs=(),
            plan_fingerprint="fp-y",
        ),
        InferredDeps(
            task_name="task_z",
            output="ev_z",
            inputs=(),
            plan_fingerprint="fp-z",
        ),
    )


@pytest.fixture
def diamond_deps() -> tuple[InferredDeps, ...]:
    """Diamond dependency fixture.

    task_a produces ev_a, task_b and task_c consume ev_a, and task_d
    consumes both ev_b and ev_c.

    Returns:
    -------
    tuple[InferredDeps, ...]
        Dependency set encoding a diamond-shaped graph.
    """
    return (
        InferredDeps(
            task_name="task_a",
            output="ev_a",
            inputs=(),
            plan_fingerprint="fp-a",
        ),
        InferredDeps(
            task_name="task_b",
            output="ev_b",
            inputs=("ev_a",),
            plan_fingerprint="fp-b",
        ),
        InferredDeps(
            task_name="task_c",
            output="ev_c",
            inputs=("ev_a",),
            plan_fingerprint="fp-c",
        ),
        InferredDeps(
            task_name="task_d",
            output="ev_d",
            inputs=("ev_b", "ev_c"),
            plan_fingerprint="fp-d",
        ),
    )


@pytest.fixture
def simple_linear_graph(simple_linear_deps: tuple[InferredDeps, ...]) -> TaskGraph:
    """Build TaskGraph from simple linear deps.

    Returns:
    -------
    TaskGraph
        Task graph built from ``simple_linear_deps``.
    """
    return build_task_graph_from_inferred_deps(simple_linear_deps)


@pytest.fixture
def independent_graph(independent_deps: tuple[InferredDeps, ...]) -> TaskGraph:
    """Build TaskGraph from independent deps.

    Returns:
    -------
    TaskGraph
        Task graph built from ``independent_deps``.
    """
    return build_task_graph_from_inferred_deps(independent_deps)


@pytest.fixture
def diamond_graph(diamond_deps: tuple[InferredDeps, ...]) -> TaskGraph:
    """Build TaskGraph from diamond deps.

    Returns:
    -------
    TaskGraph
        Task graph built from ``diamond_deps``.
    """
    return build_task_graph_from_inferred_deps(diamond_deps)


@pytest.fixture
def empty_evidence_catalog() -> EvidenceCatalog:
    """Empty evidence catalog (no pre-registered evidence).

    Returns:
    -------
    EvidenceCatalog
        Empty catalog instance.
    """
    return EvidenceCatalog()


@pytest.fixture
def evidence_catalog_with_ev_a() -> EvidenceCatalog:
    """Evidence catalog with ev_a pre-registered.

    This simulates an evidence source that already exists (e.g.,
    from a previous extraction run).

    Returns:
    -------
    EvidenceCatalog
        Catalog containing a registered schema for ``ev_a``.
    """
    catalog = EvidenceCatalog()
    schema = pa.schema([("col_x", pa.string()), ("col_y", pa.int64())])
    catalog.register_schema("ev_a", schema)
    return catalog


@pytest.mark.integration
class TestScheduleGeneration:
    """Tests for schedule_tasks() behavior."""

    def test_schedule_returns_task_schedule(
        self,
        simple_linear_graph: TaskGraph,
        empty_evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify schedule_tasks() returns a TaskSchedule."""
        schedule = schedule_tasks(simple_linear_graph, evidence=empty_evidence_catalog)
        assert isinstance(schedule, TaskSchedule)

    def test_schedule_ordered_tasks_are_strings(
        self,
        simple_linear_graph: TaskGraph,
        empty_evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify ordered_tasks contains task name strings."""
        schedule = schedule_tasks(simple_linear_graph, evidence=empty_evidence_catalog)
        assert isinstance(schedule.ordered_tasks, tuple)
        for name in schedule.ordered_tasks:
            assert isinstance(name, str)

    def test_schedule_respects_topological_order(
        self,
        simple_linear_graph: TaskGraph,
        empty_evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify every task scheduled after its dependencies.

        For the linear graph: task_a must come before task_b since
        task_b depends on ev_a (produced by task_a).

        IMPORTANT: Check via predecessor graph topology, not by comparing
        TaskNode.inputs (which are evidence names, not task names).
        """
        schedule = schedule_tasks(simple_linear_graph, evidence=empty_evidence_catalog)
        task_positions = {name: i for i, name in enumerate(schedule.ordered_tasks)}

        # task_a must appear before task_b
        if "task_a" in task_positions and "task_b" in task_positions:
            assert task_positions["task_a"] < task_positions["task_b"], (
                "task_a should be scheduled before task_b"
            )

    def test_diamond_topological_order(
        self,
        diamond_graph: TaskGraph,
        empty_evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify diamond dependency ordering."""
        schedule = schedule_tasks(diamond_graph, evidence=empty_evidence_catalog)
        positions = {name: i for i, name in enumerate(schedule.ordered_tasks)}

        # task_a must come before task_b and task_c
        if "task_a" in positions:
            if "task_b" in positions:
                assert positions["task_a"] < positions["task_b"]
            if "task_c" in positions:
                assert positions["task_a"] < positions["task_c"]
        # task_d must come after task_b and task_c
        if "task_d" in positions:
            if "task_b" in positions:
                assert positions["task_b"] < positions["task_d"]
            if "task_c" in positions:
                assert positions["task_c"] < positions["task_d"]

    def test_generations_allow_parallel_execution(
        self,
        independent_graph: TaskGraph,
        empty_evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify independent tasks placed in same generation."""
        schedule = schedule_tasks(independent_graph, evidence=empty_evidence_catalog)
        # Independent tasks should have at least one generation with multiple tasks
        multi_task_gens = [g for g in schedule.generations if len(g) > 1]
        assert len(multi_task_gens) > 0, "Expected parallel tasks in some generation"

    def test_generations_respect_dependency_order(
        self,
        diamond_graph: TaskGraph,
        empty_evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify generation waves respect dependencies."""
        schedule = schedule_tasks(diamond_graph, evidence=empty_evidence_catalog)
        # Build generation index
        gen_for_task: dict[str, int] = {}
        for gen_idx, gen_tasks in enumerate(schedule.generations):
            for task_name in gen_tasks:
                gen_for_task[task_name] = gen_idx

        # task_a should be in an earlier generation than task_d
        if "task_a" in gen_for_task and "task_d" in gen_for_task:
            assert gen_for_task["task_a"] < gen_for_task["task_d"]

    def test_partial_scheduling_with_allow_partial(
        self,
        diamond_graph: TaskGraph,
    ) -> None:
        """Verify partial scheduling when allow_partial=True.

        When evidence is missing, partial scheduling should still
        schedule what it can and report missing tasks.
        """
        # Create catalog with only partial evidence
        catalog = EvidenceCatalog()
        options = ScheduleOptions(allow_partial=True)
        schedule = schedule_tasks(
            diamond_graph,
            evidence=catalog,
            options=options,
        )
        # Some tasks should be scheduled, some may be missing
        assert isinstance(schedule.missing_tasks, tuple)
        total = len(schedule.ordered_tasks) + len(schedule.missing_tasks)
        assert total > 0

    def test_schedule_with_cost_context(
        self,
        diamond_graph: TaskGraph,
        empty_evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify schedule accepts cost context via ScheduleOptions.

        Integration scope: validate that cost context wiring works,
        not the cost formula itself (that's unit-level).
        """
        cost_context = ScheduleCostContext(
            task_costs={"task_a": 10.0, "task_b": 5.0, "task_c": 3.0, "task_d": 1.0},
            bottom_level_costs={"task_a": 15.0, "task_b": 6.0, "task_c": 4.0, "task_d": 1.0},
        )
        options = ScheduleOptions(cost_context=cost_context)
        schedule = schedule_tasks(
            diamond_graph,
            evidence=empty_evidence_catalog,
            options=options,
        )
        # Schedule should still be valid with cost context
        assert len(schedule.ordered_tasks) > 0

    def test_schedule_all_tasks_present_in_output(
        self,
        simple_linear_graph: TaskGraph,
        empty_evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify all graph tasks appear in schedule output."""
        schedule = schedule_tasks(simple_linear_graph, evidence=empty_evidence_catalog)
        all_tasks = set(schedule.ordered_tasks) | set(schedule.missing_tasks)
        graph_tasks = set(simple_linear_graph.task_idx.keys())
        assert all_tasks == graph_tasks

    def test_schedule_deterministic_across_calls(
        self,
        diamond_graph: TaskGraph,
        empty_evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify schedule is deterministic across repeated calls."""
        schedule1 = schedule_tasks(diamond_graph, evidence=empty_evidence_catalog)
        schedule2 = schedule_tasks(diamond_graph, evidence=empty_evidence_catalog)
        assert schedule1.ordered_tasks == schedule2.ordered_tasks
        assert schedule1.generations == schedule2.generations


@pytest.mark.integration
class TestBipartiteGraphStructure:
    """Tests for bipartite graph node structure.

    Validates that TaskGraph contains both EvidenceNode and TaskNode
    types, with correct edge directions.
    """

    def test_task_graph_has_evidence_and_task_nodes(
        self,
        simple_linear_graph: TaskGraph,
    ) -> None:
        """Verify graph contains both evidence and task node kinds."""
        from relspec.rustworkx_graph import GraphNode

        has_evidence = False
        has_task = False
        for node_idx in simple_linear_graph.graph.node_indices():
            node = simple_linear_graph.graph[node_idx]
            if isinstance(node, GraphNode):
                if node.kind == "evidence":
                    has_evidence = True
                elif node.kind == "task":
                    has_task = True
        assert has_evidence, "Graph should contain evidence nodes"
        assert has_task, "Graph should contain task nodes"

    def test_evidence_idx_maps_to_evidence_nodes(
        self,
        simple_linear_graph: TaskGraph,
    ) -> None:
        """Verify evidence_idx maps to GraphNode with kind=evidence."""
        from relspec.rustworkx_graph import GraphNode

        for name, idx in simple_linear_graph.evidence_idx.items():
            node = simple_linear_graph.graph[idx]
            assert isinstance(node, GraphNode)
            assert node.kind == "evidence", f"Evidence idx {name} should map to evidence node"
            assert node.payload.name == name

    def test_task_idx_maps_to_task_nodes(
        self,
        simple_linear_graph: TaskGraph,
    ) -> None:
        """Verify task_idx maps to GraphNode with kind=task."""
        from relspec.rustworkx_graph import GraphNode

        for name, idx in simple_linear_graph.task_idx.items():
            node = simple_linear_graph.graph[idx]
            assert isinstance(node, GraphNode)
            assert node.kind == "task", f"Task idx {name} should map to task node"
            assert node.payload.name == name

    def test_task_node_inputs_are_evidence_names(
        self,
        simple_linear_graph: TaskGraph,
    ) -> None:
        """Verify TaskNode.inputs contains evidence dataset names.

        IMPORTANT: TaskNode.inputs are evidence names, NOT task names.
        """
        from relspec.rustworkx_graph import GraphNode, TaskNode

        for idx in simple_linear_graph.task_idx.values():
            node = simple_linear_graph.graph[idx]
            if isinstance(node, GraphNode) and isinstance(node.payload, TaskNode):
                for input_name in node.payload.inputs:
                    # Each input should be an evidence name, not a task name
                    assert input_name not in simple_linear_graph.task_idx, (
                        f"TaskNode.input '{input_name}' should be an evidence name, not a task name"
                    )

    def test_graph_edges_have_metadata(
        self,
        simple_linear_graph: TaskGraph,
    ) -> None:
        """Verify graph edges carry edge metadata."""
        from relspec.rustworkx_graph import GraphEdge

        edge_count = 0
        for edge_idx in simple_linear_graph.graph.edge_indices():
            edge = simple_linear_graph.graph.get_edge_data_by_index(edge_idx)
            if isinstance(edge, GraphEdge):
                edge_count += 1
                assert edge.kind in {"requires", "produces"}
        assert edge_count > 0, "Graph should have edges with metadata"
