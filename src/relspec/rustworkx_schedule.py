"""Rustworkx scheduling helpers for task graphs."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import rustworkx as rx

from relspec import graph_edge_validation
from relspec.errors import RelspecValidationError
from relspec.evidence import EvidenceCatalog
from relspec.rustworkx_graph import (
    EvidenceNode,
    TaskGraph,
    TaskNode,
    task_dependency_reduction,
)
from relspec.schedule_events import TaskScheduleMetadata
from serde_msgspec import StructBaseStrict

if TYPE_CHECKING:
    from datafusion_engine.schema.contracts import SchemaContract
    from schema_spec.system import ContractSpec, DatasetSpec


class TaskSchedule(StructBaseStrict, frozen=True):
    """Deterministic schedule for task execution."""

    ordered_tasks: tuple[str, ...]
    generations: tuple[tuple[str, ...], ...]
    missing_tasks: tuple[str, ...] = ()
    validation_summary: graph_edge_validation.GraphValidationSummary | None = None


class ScheduleCostContext(StructBaseStrict, frozen=True):
    """Schedule ordering inputs derived from cost modeling."""

    task_costs: Mapping[str, float] | None = None
    bottom_level_costs: Mapping[str, float] | None = None
    slack_by_task: Mapping[str, float] | None = None
    betweenness_centrality: Mapping[str, float] | None = None
    articulation_tasks: frozenset[str] | None = None
    bridge_tasks: frozenset[str] | None = None


@dataclass(frozen=True)
class ScheduleOptions:
    """Scheduling options for plan compilation."""

    output_schema_for: (
        Callable[[str], SchemaContract | DatasetSpec | ContractSpec | None] | None
    ) = None
    allow_partial: bool = False
    reduced_dependency_graph: rx.PyDiGraph | None = None
    cost_context: ScheduleCostContext = field(default_factory=ScheduleCostContext)


def schedule_tasks(
    graph: TaskGraph,
    *,
    evidence: EvidenceCatalog,
    options: ScheduleOptions | None = None,
) -> TaskSchedule:
    """Return a deterministic task schedule driven by a task graph.

    Returns
    -------
    TaskSchedule
        Ordered task names and generation waves.

    Raises
    ------
    RelspecValidationError
        Raised when evidence requirements cannot be satisfied.
    """
    working = evidence.clone()
    resolved_options = options or ScheduleOptions()
    sorter = _make_sorter(graph, seed_nodes=_seed_nodes(graph, working.sources))
    ordered: list[str] = []
    visited_tasks: set[str] = set()
    while sorter.is_active():
        ready = list(sorter.get_ready())
        if not ready:
            break
        ready_tasks = _sorted_ready_tasks(
            graph,
            ready,
            cost_context=resolved_options.cost_context,
        )
        ready_evidence = _ready_evidence_nodes(graph, ready)
        valid_tasks: list[TaskNode] = []
        for task in ready_tasks:
            task_idx = graph.task_idx.get(task.name)
            if task_idx is None:
                continue
            if not graph_edge_validation.validate_edge_requirements(
                graph,
                task_idx,
                catalog=working,
            ):
                continue
            ordered.append(task.name)
            visited_tasks.add(task.name)
            valid_tasks.append(task)
        _register_ready_evidence(
            graph,
            ready_evidence,
            evidence=working,
            output_schema_for=resolved_options.output_schema_for,
        )
        sorter.done([*ready_evidence, *[graph.task_idx[task.name] for task in valid_tasks]])
    missing = tuple(sorted(set(graph.task_idx) - visited_tasks))
    validation_summary = graph_edge_validation.validate_graph_edges(graph, catalog=working)
    if missing and not resolved_options.allow_partial:
        msg = (
            "Task graph cannot resolve evidence for: "
            f"{list(missing)}. Invalid edges: {validation_summary.invalid_edges}."
        )
        raise RelspecValidationError(msg)
    generations = _task_generations(
        graph,
        visited_tasks=visited_tasks,
        reduced_dependency_graph=resolved_options.reduced_dependency_graph,
        cost_context=resolved_options.cost_context,
    )
    return TaskSchedule(
        ordered_tasks=tuple(ordered),
        generations=tuple(generations),
        missing_tasks=missing,
        validation_summary=validation_summary,
    )


def impacted_tasks(
    graph: TaskGraph,
    *,
    evidence_name: str,
) -> tuple[str, ...]:
    """Return task names impacted by a changed evidence dataset.

    Returns
    -------
    tuple[str, ...]
        Sorted impacted task names.
    """
    node = graph.evidence_idx.get(evidence_name)
    if node is None:
        return ()
    impacted = rx.descendants(graph.graph, node)
    names = [graph.graph[idx].payload.name for idx in impacted if graph.graph[idx].kind == "task"]
    return tuple(sorted(set(names)))


def impacted_tasks_for_evidence(
    graph: TaskGraph,
    *,
    evidence_names: Iterable[str],
) -> tuple[str, ...]:
    """Return impacted tasks for multiple evidence datasets.

    Returns
    -------
    tuple[str, ...]
        Sorted impacted task names.
    """
    impacted: set[str] = set()
    for name in evidence_names:
        impacted.update(impacted_tasks(graph, evidence_name=name))
    return tuple(sorted(impacted))


def provenance_for_task(
    graph: TaskGraph,
    *,
    task_name: str,
) -> tuple[str, ...]:
    """Return evidence names that contribute to a task.

    Returns
    -------
    tuple[str, ...]
        Sorted evidence names contributing to the task.
    """
    node = graph.task_idx.get(task_name)
    if node is None:
        return ()
    ancestors = rx.ancestors(graph.graph, node)
    names = [
        graph.graph[idx].payload.name for idx in ancestors if graph.graph[idx].kind == "evidence"
    ]
    return tuple(sorted(set(names)))


def task_schedule_metadata(
    schedule: TaskSchedule,
) -> dict[str, TaskScheduleMetadata]:
    """Return per-task schedule metadata for execution events.

    Returns
    -------
    dict[str, TaskScheduleMetadata]
        Mapping of task names to schedule metadata.
    """
    order_index = {name: idx for idx, name in enumerate(schedule.ordered_tasks)}
    mapping: dict[str, TaskScheduleMetadata] = {}
    for gen_idx, generation in enumerate(schedule.generations):
        generation_size = len(generation)
        for gen_order, name in enumerate(generation):
            schedule_index = order_index.get(name)
            if schedule_index is None:
                continue
            mapping[name] = TaskScheduleMetadata(
                schedule_index=schedule_index,
                generation_index=gen_idx,
                generation_order=gen_order,
                generation_size=generation_size,
            )
    return mapping


def _seed_nodes(graph: TaskGraph, seed_evidence: Iterable[str]) -> list[int]:
    nodes: set[int] = set()
    for name in seed_evidence:
        idx = graph.evidence_idx.get(name)
        if idx is not None and not tuple(graph.graph.predecessor_indices(idx)):
            nodes.add(idx)
    for evidence_idx in graph.evidence_idx.values():
        if not tuple(graph.graph.predecessor_indices(evidence_idx)):
            nodes.add(evidence_idx)
    for task_idx in graph.task_idx.values():
        if not tuple(graph.graph.predecessor_indices(task_idx)):
            nodes.add(task_idx)
    return sorted(nodes)


def _make_sorter(graph: TaskGraph, *, seed_nodes: list[int]) -> rx.TopologicalSorter:
    try:
        return rx.TopologicalSorter(graph.graph, check_cycle=True, initial=seed_nodes)
    except (TypeError, ValueError) as exc:
        msg = "Task graph contains a cycle or invalid dependency."
        raise RelspecValidationError(msg) from exc


def _sorted_ready_tasks(
    graph: TaskGraph,
    ready: Iterable[int],
    *,
    cost_context: ScheduleCostContext,
) -> list[TaskNode]:
    tasks: list[TaskNode] = []
    for idx in ready:
        node = graph.graph[idx]
        if node.kind != "task":
            continue
        payload = node.payload
        if not isinstance(payload, TaskNode):
            msg = "Expected TaskNode payload for task graph node."
            raise TypeError(msg)
        tasks.append(payload)
    tasks.sort(
        key=lambda task: _task_sort_key(
            task,
            cost_context=cost_context,
        )
    )
    return tasks


def _ready_evidence_nodes(graph: TaskGraph, ready: Iterable[int]) -> list[int]:
    nodes: list[int] = []
    for idx in ready:
        node = graph.graph[idx]
        if node.kind != "evidence":
            continue
        payload = node.payload
        if not isinstance(payload, EvidenceNode):
            msg = "Expected EvidenceNode payload for evidence graph node."
            raise TypeError(msg)
        nodes.append(idx)
    return nodes


def _register_ready_evidence(
    graph: TaskGraph,
    ready: Iterable[int],
    *,
    evidence: EvidenceCatalog,
    output_schema_for: Callable[[str], SchemaContract | DatasetSpec | ContractSpec | None] | None,
) -> None:
    for idx in ready:
        node = graph.graph[idx]
        if node.kind != "evidence":
            continue
        payload = node.payload
        if not isinstance(payload, EvidenceNode):
            msg = "Expected EvidenceNode payload for evidence graph node."
            raise TypeError(msg)
        name = payload.name
        if name in evidence.sources:
            continue
        if not _has_task_predecessor(graph, idx):
            continue
        contract = output_schema_for(name) if output_schema_for is not None else None
        if contract is None:
            evidence.sources.add(name)
            continue
        from datafusion_engine.schema.contracts import SchemaContract
        from schema_spec.system import ContractSpec, DatasetSpec

        if isinstance(contract, SchemaContract):
            evidence.register_contract(name, contract)
        elif isinstance(contract, DatasetSpec):
            evidence.register_from_dataset_spec(name, contract)
        elif isinstance(contract, ContractSpec):
            evidence.register_from_contract_spec(name, contract)
        else:
            msg = f"Unsupported output contract type for {name!r}: {type(contract).__name__}."
            raise TypeError(msg)


def _has_task_predecessor(graph: TaskGraph, node_idx: int) -> bool:
    for pred_idx in graph.graph.predecessor_indices(node_idx):
        pred = graph.graph[pred_idx]
        if pred.kind == "task":
            return True
    return False


def _task_generations(
    graph: TaskGraph,
    *,
    visited_tasks: set[str],
    reduced_dependency_graph: rx.PyDiGraph | None,
    cost_context: ScheduleCostContext,
) -> tuple[tuple[str, ...], ...]:
    dependency_graph = (
        reduced_dependency_graph
        if reduced_dependency_graph is not None
        else task_dependency_reduction(graph).reduced_graph
    )
    if dependency_graph.num_nodes() == 0:
        return ()
    generations = rx.topological_generations(dependency_graph)
    output: list[tuple[str, ...]] = []
    for generation in generations:
        payloads: list[TaskNode] = []
        for node_idx in generation:
            node = dependency_graph[node_idx]
            if not isinstance(node, TaskNode):
                continue
            if node.name not in visited_tasks:
                continue
            payloads.append(node)
        payloads.sort(
            key=lambda task: _task_sort_key(
                task,
                cost_context=cost_context,
            )
        )
        if payloads:
            output.append(tuple(task.name for task in payloads))
    return tuple(output)


def _task_cost_value(task: TaskNode, *, cost_context: ScheduleCostContext) -> float:
    task_costs = cost_context.task_costs
    if task_costs is not None and task.name in task_costs:
        return float(max(task_costs[task.name], 1.0))
    return float(max(task.priority, 1))


def _task_centrality_value(task: TaskNode, *, cost_context: ScheduleCostContext) -> float:
    centrality = cost_context.betweenness_centrality
    if centrality is not None and task.name in centrality:
        return float(max(centrality[task.name], 0.0))
    return 0.0


def _task_structural_bonus(task: TaskNode, *, cost_context: ScheduleCostContext) -> float:
    bonus = 0.0
    if cost_context.articulation_tasks is not None and task.name in cost_context.articulation_tasks:
        bonus += 1.0
    if cost_context.bridge_tasks is not None and task.name in cost_context.bridge_tasks:
        bonus += 0.5
    return bonus


def _task_sort_key(
    task: TaskNode,
    *,
    cost_context: ScheduleCostContext,
) -> tuple[float, float, float, str]:
    base_cost = _task_cost_value(task, cost_context=cost_context)
    structural_bonus = _task_structural_bonus(task, cost_context=cost_context)
    bottom_cost = (
        cost_context.bottom_level_costs.get(task.name, base_cost)
        if cost_context.bottom_level_costs is not None
        else base_cost
    )
    centrality = _task_centrality_value(task, cost_context=cost_context)
    slack = (
        cost_context.slack_by_task.get(task.name, 0.0)
        if cost_context.slack_by_task is not None
        else 0.0
    )
    return (-float(bottom_cost + structural_bonus), -float(centrality), float(slack), task.name)


__all__ = [
    "ScheduleCostContext",
    "ScheduleOptions",
    "TaskSchedule",
    "impacted_tasks",
    "impacted_tasks_for_evidence",
    "provenance_for_task",
    "schedule_tasks",
    "task_schedule_metadata",
]
