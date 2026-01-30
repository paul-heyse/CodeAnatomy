"""Edge-based validation for task graphs with column-level requirements.

This module provides validation functions that check whether graph edges
have their column-level requirements satisfied by the evidence catalog.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from relspec.evidence import EvidenceCatalog
from relspec.rustworkx_graph import GraphEdge, GraphNode, TaskGraph, TaskNode
from utils.validation import find_missing


@dataclass(frozen=True)
class EdgeValidationResult:
    """Result of validating a single edge's requirements.

    Attributes
    ----------
    source_name : str
        Name of the source evidence dataset.
    target_task : str
        Name of the target task.
    is_valid : bool
        Whether all requirements are satisfied.
    missing_columns : tuple[str, ...]
        Columns required but not present.
    missing_types : tuple[tuple[str, str], ...]
        Required column/type pairs missing or mismatched.
    missing_metadata : tuple[tuple[bytes, bytes], ...]
        Required metadata entries missing or mismatched.
    contract_violations : tuple[str, ...]
        Contract violations detected for the evidence source.
    available_columns : tuple[str, ...]
        Columns actually available in the catalog.
    available_types : tuple[tuple[str, str], ...]
        Available column/type pairs in the catalog.
    available_metadata : tuple[tuple[bytes, bytes], ...]
        Available metadata entries in the catalog.
    """

    source_name: str
    target_task: str
    is_valid: bool
    missing_columns: tuple[str, ...] = ()
    missing_types: tuple[tuple[str, str], ...] = ()
    missing_metadata: tuple[tuple[bytes, bytes], ...] = ()
    contract_violations: tuple[str, ...] = ()
    available_columns: tuple[str, ...] = ()
    available_types: tuple[tuple[str, str], ...] = ()
    available_metadata: tuple[tuple[bytes, bytes], ...] = ()


@dataclass(frozen=True)
class TaskValidationResult:
    """Result of validating all edges for a task.

    Attributes
    ----------
    task_name : str
        Name of the task being validated.
    is_valid : bool
        Whether all predecessor edges are satisfied.
    edge_results : tuple[EdgeValidationResult, ...]
        Individual validation results for each edge.
    unsatisfied_edges : tuple[str, ...]
        Names of edges that failed validation.
    contract_violations : tuple[str, ...]
        Flattened contract violations across all incoming edges.
    """

    task_name: str
    is_valid: bool
    edge_results: tuple[EdgeValidationResult, ...] = ()
    unsatisfied_edges: tuple[str, ...] = ()
    contract_violations: tuple[str, ...] = ()


@dataclass(frozen=True)
class GraphValidationSummary:
    """Summary of validation across all tasks in a graph.

    Attributes
    ----------
    total_tasks : int
        Total number of tasks validated.
    valid_tasks : int
        Number of tasks with all edges satisfied.
    invalid_tasks : int
        Number of tasks with unsatisfied edges.
    total_edges : int
        Total number of edges validated.
    valid_edges : int
        Number of edges with requirements satisfied.
    invalid_edges : int
        Number of edges with missing requirements.
    task_results : tuple[TaskValidationResult, ...]
        Per-task validation results.
    """

    total_tasks: int
    valid_tasks: int
    invalid_tasks: int
    total_edges: int
    valid_edges: int
    invalid_edges: int
    task_results: tuple[TaskValidationResult, ...] = ()


def validate_edge_requirements(
    graph: TaskGraph,
    task_idx: int,
    *,
    catalog: EvidenceCatalog,
) -> bool:
    """Validate predecessor edges using column/type/metadata requirements.

    Checks whether all requirements on incoming edges are satisfied
    by the evidence catalog.

    Parameters
    ----------
    graph : TaskGraph
        The task graph containing the task.
    task_idx : int
        Node index of the task to validate.
    catalog : EvidenceCatalog
        Evidence catalog with available datasets and columns.

    Returns
    -------
    bool
        True if all predecessor edge requirements are satisfied.
    """
    for pred_idx in graph.graph.predecessor_indices(task_idx):
        edge_data = graph.graph.get_edge_data(pred_idx, task_idx)
        if not isinstance(edge_data, GraphEdge):
            continue
        available_cols = catalog.columns_by_dataset.get(edge_data.name)
        if _missing_required_columns(edge_data.required_columns, available_cols):
            return False
        available_types = catalog.types_by_dataset.get(edge_data.name)
        if (
            edge_data.required_types
            and available_types is not None
            and _missing_required_types(edge_data.required_types, available_types)
        ):
            return False
        available_metadata = catalog.metadata_by_dataset.get(edge_data.name)
        if (
            edge_data.required_metadata
            and available_metadata is not None
            and _missing_required_metadata(edge_data.required_metadata, available_metadata)
        ):
            return False
        violations = catalog.contract_violations_by_dataset.get(edge_data.name)
        if violations:
            return False
    return True


def validate_edge_requirements_detailed(
    graph: TaskGraph,
    task_idx: int,
    *,
    catalog: EvidenceCatalog,
) -> TaskValidationResult:
    """Validate predecessor edges with detailed results.

    Similar to validate_edge_requirements but returns detailed information
    about which requirements are missing from which edges.

    Parameters
    ----------
    graph : TaskGraph
        The task graph containing the task.
    task_idx : int
        Node index of the task to validate.
    catalog : EvidenceCatalog
        Evidence catalog with available datasets and columns.

    Returns
    -------
    TaskValidationResult
        Detailed validation result for the task.

    Raises
    ------
    TypeError
        Raised when the graph node at task_idx is not a TaskNode.
    """
    node = graph.graph[task_idx]
    if not isinstance(node, GraphNode) or not isinstance(node.payload, TaskNode):
        msg = f"Expected TaskNode at index {task_idx}"
        raise TypeError(msg)

    task_name = node.payload.name
    edge_results: list[EdgeValidationResult] = []
    unsatisfied: list[str] = []
    contract_violations: set[str] = set()

    for pred_idx in graph.graph.predecessor_indices(task_idx):
        edge_data = graph.graph.get_edge_data(pred_idx, task_idx)
        if not isinstance(edge_data, GraphEdge):
            continue
        edge_result, is_valid, violation_messages = _edge_validation_result(
            edge_data,
            task_name=task_name,
            catalog=catalog,
        )
        contract_violations.update(violation_messages)
        if not is_valid:
            unsatisfied.append(edge_data.name)
        edge_results.append(edge_result)

    return TaskValidationResult(
        task_name=task_name,
        is_valid=len(unsatisfied) == 0,
        edge_results=tuple(edge_results),
        unsatisfied_edges=tuple(unsatisfied),
        contract_violations=tuple(sorted(contract_violations)),
    )


def _edge_validation_result(
    edge: GraphEdge,
    *,
    task_name: str,
    catalog: EvidenceCatalog,
) -> tuple[EdgeValidationResult, bool, tuple[str, ...]]:
    available_cols = catalog.columns_by_dataset.get(edge.name)
    missing_cols = _missing_required_columns(edge.required_columns, available_cols)
    available_types = catalog.types_by_dataset.get(edge.name)
    missing_types = (
        _missing_required_types(edge.required_types, available_types)
        if available_types is not None
        else ()
    )
    available_metadata = catalog.metadata_by_dataset.get(edge.name)
    missing_metadata = (
        _missing_required_metadata(edge.required_metadata, available_metadata)
        if available_metadata is not None
        else ()
    )
    violations = catalog.contract_violations_by_dataset.get(edge.name)
    violation_messages = tuple(str(item) for item in violations) if violations else ()
    is_valid = (
        not missing_cols and not missing_types and not missing_metadata and not violation_messages
    )
    return (
        EdgeValidationResult(
            source_name=edge.name,
            target_task=task_name,
            is_valid=is_valid,
            missing_columns=tuple(sorted(missing_cols)),
            missing_types=missing_types,
            missing_metadata=missing_metadata,
            contract_violations=violation_messages,
            available_columns=tuple(sorted(available_cols or ())),
            available_types=_sorted_type_pairs(available_types or {}),
            available_metadata=_sorted_metadata_pairs(available_metadata or {}),
        ),
        is_valid,
        violation_messages,
    )


def _missing_required_columns(
    required_columns: tuple[str, ...],
    available_columns: set[str] | None,
) -> tuple[str, ...]:
    if available_columns is None:
        return ()
    return tuple(find_missing(required_columns, available_columns))


def validate_graph_edges(
    graph: TaskGraph,
    *,
    catalog: EvidenceCatalog,
) -> GraphValidationSummary:
    """Validate all edges in a task graph against the evidence catalog.

    Parameters
    ----------
    graph : TaskGraph
        The task graph to validate.
    catalog : EvidenceCatalog
        Evidence catalog with available datasets and columns.

    Returns
    -------
    GraphValidationSummary
        Summary of validation across all tasks.
    """
    task_results: list[TaskValidationResult] = []
    total_edges = 0
    valid_edges = 0

    for task_idx in graph.task_idx.values():
        result = validate_edge_requirements_detailed(graph, task_idx, catalog=catalog)
        task_results.append(result)
        total_edges += len(result.edge_results)
        valid_edges += sum(1 for e in result.edge_results if e.is_valid)

    valid_tasks = sum(1 for r in task_results if r.is_valid)
    invalid_tasks = len(task_results) - valid_tasks
    invalid_edges = total_edges - valid_edges

    return GraphValidationSummary(
        total_tasks=len(task_results),
        valid_tasks=valid_tasks,
        invalid_tasks=invalid_tasks,
        total_edges=total_edges,
        valid_edges=valid_edges,
        invalid_edges=invalid_edges,
        task_results=tuple(task_results),
    )


def _missing_required_types(
    required: tuple[tuple[str, str], ...],
    available: Mapping[str, str],
) -> tuple[tuple[str, str], ...]:
    missing: list[tuple[str, str]] = []
    for name, dtype in required:
        if available.get(name) != dtype:
            missing.append((name, dtype))
    return tuple(missing)


def _missing_required_metadata(
    required: tuple[tuple[bytes, bytes], ...],
    available: Mapping[bytes, bytes],
) -> tuple[tuple[bytes, bytes], ...]:
    missing: list[tuple[bytes, bytes]] = []
    for key, value in required:
        if available.get(key) != value:
            missing.append((key, value))
    return tuple(missing)


def _sorted_type_pairs(items: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted(items.items(), key=lambda pair: pair[0]))


def _sorted_metadata_pairs(
    items: Mapping[bytes, bytes],
) -> tuple[tuple[bytes, bytes], ...]:
    return tuple(sorted(items.items(), key=lambda pair: pair[0]))


def ready_tasks_with_column_validation(
    graph: TaskGraph,
    *,
    catalog: EvidenceCatalog,
    completed: set[str],
) -> Sequence[str]:
    """Return tasks that are ready for execution with column validation.

    A task is ready if:
    1. All its predecessor tasks have completed
    2. All column requirements on incoming edges are satisfied

    Parameters
    ----------
    graph : TaskGraph
        The task graph.
    catalog : EvidenceCatalog
        Evidence catalog with available datasets and columns.
    completed : set[str]
        Set of completed task names.

    Returns
    -------
    Sequence[str]
        Names of tasks ready for execution.
    """
    ready: list[str] = []

    for task_name, task_idx in graph.task_idx.items():
        if task_name in completed:
            continue

        # Check predecessor tasks are complete
        all_preds_complete = True
        for pred_idx in graph.graph.predecessor_indices(task_idx):
            pred_node = graph.graph[pred_idx]
            if not isinstance(pred_node, GraphNode):
                continue
            if (
                pred_node.kind == "task"
                and isinstance(pred_node.payload, TaskNode)
                and pred_node.payload.name not in completed
            ):
                all_preds_complete = False
                break

        if not all_preds_complete:
            continue

        # Check column requirements
        if validate_edge_requirements(graph, task_idx, catalog=catalog):
            ready.append(task_name)

    return tuple(sorted(ready))


__all__ = [
    "EdgeValidationResult",
    "GraphValidationSummary",
    "TaskValidationResult",
    "ready_tasks_with_column_validation",
    "validate_edge_requirements",
    "validate_edge_requirements_detailed",
    "validate_graph_edges",
]
