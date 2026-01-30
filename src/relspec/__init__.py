"""Inference-first relationship specification helpers."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "EdgeValidationResult": ("relspec.graph_edge_validation", "EdgeValidationResult"),
    "EvidenceCatalog": ("relspec.evidence", "EvidenceCatalog"),
    "EvidenceNode": ("relspec.rustworkx_graph", "EvidenceNode"),
    "ExecutionPlan": ("relspec.execution_plan", "ExecutionPlan"),
    "GraphDiagnostics": ("relspec.rustworkx_graph", "GraphDiagnostics"),
    "GraphEdge": ("relspec.rustworkx_graph", "GraphEdge"),
    "GraphNode": ("relspec.rustworkx_graph", "GraphNode"),
    "GraphValidationSummary": ("relspec.graph_edge_validation", "GraphValidationSummary"),
    "TaskGraphSnapshot": ("relspec.rustworkx_graph", "TaskGraphSnapshot"),
    "TaskNode": ("relspec.rustworkx_graph", "TaskNode"),
    "TaskSchedule": ("relspec.rustworkx_schedule", "TaskSchedule"),
    "TaskValidationResult": ("relspec.graph_edge_validation", "TaskValidationResult"),
    "TaskGraph": ("relspec.rustworkx_graph", "TaskGraph"),
    "build_task_graph_from_views": ("relspec.rustworkx_graph", "build_task_graph_from_views"),
    "bottom_level_costs": ("relspec.execution_plan", "bottom_level_costs"),
    "compile_execution_plan": ("relspec.execution_plan", "compile_execution_plan"),
    "dependency_map_from_inferred": ("relspec.execution_plan", "dependency_map_from_inferred"),
    "downstream_task_closure": ("relspec.execution_plan", "downstream_task_closure"),
    "impacted_tasks": ("relspec.rustworkx_schedule", "impacted_tasks"),
    "impacted_tasks_for_evidence": ("relspec.rustworkx_schedule", "impacted_tasks_for_evidence"),
    "provenance_for_task": ("relspec.rustworkx_schedule", "provenance_for_task"),
    "priority_for_task": ("relspec.execution_plan", "priority_for_task"),
    "upstream_task_closure": ("relspec.execution_plan", "upstream_task_closure"),
    "ready_tasks_with_column_validation": (
        "relspec.graph_edge_validation",
        "ready_tasks_with_column_validation",
    ),
    "task_graph_diagnostics": ("relspec.rustworkx_graph", "task_graph_diagnostics"),
    "task_graph_impact_subgraph": ("relspec.rustworkx_graph", "task_graph_impact_subgraph"),
    "task_graph_isolate_labels": ("relspec.rustworkx_graph", "task_graph_isolate_labels"),
    "task_graph_node_label": ("relspec.rustworkx_graph", "task_graph_node_label"),
    "task_graph_node_link_json": ("relspec.rustworkx_graph", "task_graph_node_link_json"),
    "task_graph_signature": ("relspec.rustworkx_graph", "task_graph_signature"),
    "task_graph_snapshot": ("relspec.rustworkx_graph", "task_graph_snapshot"),
    "task_graph_subgraph": ("relspec.rustworkx_graph", "task_graph_subgraph"),
    "task_schedule_metadata": ("relspec.rustworkx_schedule", "task_schedule_metadata"),
    "schedule_tasks": ("relspec.rustworkx_schedule", "schedule_tasks"),
    "validate_edge_requirements": ("relspec.graph_edge_validation", "validate_edge_requirements"),
    "validate_edge_requirements_detailed": (
        "relspec.graph_edge_validation",
        "validate_edge_requirements_detailed",
    ),
    "validate_graph_edges": ("relspec.graph_edge_validation", "validate_graph_edges"),
}

if TYPE_CHECKING:
    EdgeValidationResult: Any
    EvidenceCatalog: Any
    EvidenceNode: Any
    ExecutionPlan: Any
    GraphDiagnostics: Any
    GraphEdge: Any
    GraphNode: Any
    GraphValidationSummary: Any
    TaskGraphSnapshot: Any
    TaskNode: Any
    TaskSchedule: Any
    TaskValidationResult: Any
    TaskGraph: Any
    build_task_graph_from_views: Any
    bottom_level_costs: Any
    compile_execution_plan: Any
    dependency_map_from_inferred: Any
    downstream_task_closure: Any
    impacted_tasks: Any
    impacted_tasks_for_evidence: Any
    provenance_for_task: Any
    priority_for_task: Any
    upstream_task_closure: Any
    ready_tasks_with_column_validation: Any
    task_graph_diagnostics: Any
    task_graph_impact_subgraph: Any
    task_graph_isolate_labels: Any
    task_graph_node_label: Any
    task_graph_node_link_json: Any
    task_graph_signature: Any
    task_graph_snapshot: Any
    task_graph_subgraph: Any
    task_schedule_metadata: Any
    schedule_tasks: Any
    validate_edge_requirements: Any
    validate_edge_requirements_detailed: Any
    validate_graph_edges: Any


def __getattr__(name: str) -> object:
    target = _EXPORT_MAP.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = target
    module = importlib.import_module(module_path)
    return getattr(module, attr_name)


def __dir__() -> list[str]:
    return sorted(list(globals()) + list(_EXPORT_MAP))


__all__ = (
    "EdgeValidationResult",
    "EvidenceCatalog",
    "EvidenceNode",
    "ExecutionPlan",
    "GraphDiagnostics",
    "GraphEdge",
    "GraphNode",
    "GraphValidationSummary",
    "TaskGraph",
    "TaskGraphSnapshot",
    "TaskNode",
    "TaskSchedule",
    "TaskValidationResult",
    "bottom_level_costs",
    "build_task_graph_from_views",
    "compile_execution_plan",
    "dependency_map_from_inferred",
    "downstream_task_closure",
    "impacted_tasks",
    "impacted_tasks_for_evidence",
    "priority_for_task",
    "provenance_for_task",
    "ready_tasks_with_column_validation",
    "schedule_tasks",
    "task_graph_diagnostics",
    "task_graph_impact_subgraph",
    "task_graph_isolate_labels",
    "task_graph_node_label",
    "task_graph_node_link_json",
    "task_graph_signature",
    "task_graph_snapshot",
    "task_graph_subgraph",
    "task_schedule_metadata",
    "upstream_task_closure",
    "validate_edge_requirements",
    "validate_edge_requirements_detailed",
    "validate_graph_edges",
)
