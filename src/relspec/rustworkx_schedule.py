"""Rustworkx scheduling helpers for task graphs."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import rustworkx as rx

from relspec.errors import RelspecValidationError
from relspec.evidence import EvidenceCatalog
from relspec.graph_edge_validation import validate_edge_requirements
from relspec.rustworkx_graph import EvidenceNode, TaskGraph, TaskNode
from relspec.schedule_events import TaskScheduleMetadata

if TYPE_CHECKING:
    from arrowdsl.core.interop import SchemaLike


@dataclass(frozen=True)
class TaskSchedule:
    """Deterministic schedule for task execution."""

    ordered_tasks: tuple[str, ...]
    generations: tuple[tuple[str, ...], ...]
    missing_tasks: tuple[str, ...] = ()


def schedule_tasks(
    graph: TaskGraph,
    *,
    evidence: EvidenceCatalog,
    output_schema_for: Callable[[str], SchemaLike | None] | None = None,
    allow_partial: bool = False,
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
    seed_nodes = _seed_nodes(graph, working.sources)
    sorter = _make_sorter(graph, seed_nodes=seed_nodes)
    ordered: list[str] = []
    generations: list[tuple[str, ...]] = []
    visited_tasks: set[str] = set()
    while sorter.is_active():
        ready = list(sorter.get_ready())
        if not ready:
            break
        ready_tasks = _sorted_ready_tasks(graph, ready)
        ready_evidence = _ready_evidence_nodes(graph, ready)
        valid_tasks: list[TaskNode] = []
        for task in ready_tasks:
            task_idx = graph.task_idx.get(task.name)
            if task_idx is None:
                continue
            if not validate_edge_requirements(graph, task_idx, catalog=working):
                continue
            ordered.append(task.name)
            visited_tasks.add(task.name)
            valid_tasks.append(task)
        _register_ready_evidence(
            graph,
            ready_evidence,
            evidence=working,
            output_schema_for=output_schema_for,
        )
        sorter.done([*ready_evidence, *[graph.task_idx[task.name] for task in valid_tasks]])
        if valid_tasks:
            generations.append(tuple(task.name for task in valid_tasks))
    missing = tuple(sorted(set(graph.task_idx) - visited_tasks))
    if missing and not allow_partial:
        msg = f"Task graph cannot resolve evidence for: {list(missing)}."
        raise RelspecValidationError(msg)
    return TaskSchedule(
        ordered_tasks=tuple(ordered),
        generations=tuple(generations),
        missing_tasks=missing,
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
    nodes: list[int] = []
    for name in seed_evidence:
        idx = graph.evidence_idx.get(name)
        if idx is not None:
            nodes.append(idx)
    return nodes


def _make_sorter(graph: TaskGraph, *, seed_nodes: list[int]) -> rx.TopologicalSorter:
    try:
        return rx.TopologicalSorter(graph.graph, check_cycle=True, initial=seed_nodes)
    except (TypeError, ValueError) as exc:
        msg = "Task graph contains a cycle or invalid dependency."
        raise RelspecValidationError(msg) from exc


def _sorted_ready_tasks(graph: TaskGraph, ready: Iterable[int]) -> list[TaskNode]:
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
    tasks.sort(key=lambda task: (task.priority, task.name))
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
    output_schema_for: Callable[[str], SchemaLike | None] | None,
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
        schema = output_schema_for(name) if output_schema_for is not None else None
        if schema is None:
            evidence.sources.add(name)
            continue
        evidence.register(name, schema)


def _has_task_predecessor(graph: TaskGraph, node_idx: int) -> bool:
    for pred_idx in graph.graph.predecessor_indices(node_idx):
        pred = graph.graph[pred_idx]
        if pred.kind == "task":
            return True
    return False


__all__ = [
    "TaskSchedule",
    "impacted_tasks",
    "impacted_tasks_for_evidence",
    "provenance_for_task",
    "schedule_tasks",
    "task_schedule_metadata",
]
