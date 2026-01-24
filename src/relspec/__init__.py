"""Inference-first relationship specification helpers."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "CachePolicy": ("relspec.task_catalog", "CachePolicy"),
    "EdgeValidationResult": ("relspec.graph_edge_validation", "EdgeValidationResult"),
    "EvidenceCatalog": ("relspec.evidence", "EvidenceCatalog"),
    "EvidenceNode": ("relspec.rustworkx_graph", "EvidenceNode"),
    "GraphDiagnostics": ("relspec.rustworkx_graph", "GraphDiagnostics"),
    "GraphEdge": ("relspec.rustworkx_graph", "GraphEdge"),
    "GraphNode": ("relspec.rustworkx_graph", "GraphNode"),
    "GraphValidationSummary": ("relspec.graph_edge_validation", "GraphValidationSummary"),
    "IncrementalDiff": ("relspec.incremental", "IncrementalDiff"),
    "PlanArtifact": ("relspec.plan_catalog", "PlanArtifact"),
    "PlanCatalog": ("relspec.plan_catalog", "PlanCatalog"),
    "TaskGraphSnapshot": ("relspec.rustworkx_graph", "TaskGraphSnapshot"),
    "TaskNode": ("relspec.rustworkx_graph", "TaskNode"),
    "TaskSchedule": ("relspec.rustworkx_schedule", "TaskSchedule"),
    "TaskValidationResult": ("relspec.graph_edge_validation", "TaskValidationResult"),
    "TaskBuildContext": ("relspec.task_catalog", "TaskBuildContext"),
    "TaskCatalog": ("relspec.task_catalog", "TaskCatalog"),
    "TaskExecutionRequest": ("relspec.execution", "TaskExecutionRequest"),
    "TaskGraph": ("relspec.graph_inference", "TaskGraph"),
    "TaskKind": ("relspec.task_catalog", "TaskKind"),
    "TaskSpec": ("relspec.task_catalog", "TaskSpec"),
    "build_task_catalog": ("relspec.task_catalog_builders", "build_task_catalog"),
    "build_task_graph": ("relspec.graph_inference", "build_task_graph"),
    "compile_task_catalog": ("relspec.plan_catalog", "compile_task_catalog"),
    "compile_task_plan": ("relspec.plan_catalog", "compile_task_plan"),
    "diff_plan_catalog": ("relspec.incremental", "diff_plan_catalog"),
    "execute_plan_artifact": ("relspec.execution", "execute_plan_artifact"),
    "impacted_tasks": ("relspec.rustworkx_schedule", "impacted_tasks"),
    "impacted_tasks_for_evidence": ("relspec.rustworkx_schedule", "impacted_tasks_for_evidence"),
    "provenance_for_task": ("relspec.rustworkx_schedule", "provenance_for_task"),
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
    "task_graph_from_catalog": ("relspec.graph_inference", "task_graph_from_catalog"),
    "validate_edge_requirements": ("relspec.graph_edge_validation", "validate_edge_requirements"),
    "validate_edge_requirements_detailed": (
        "relspec.graph_edge_validation",
        "validate_edge_requirements_detailed",
    ),
    "validate_graph_edges": ("relspec.graph_edge_validation", "validate_graph_edges"),
}

if TYPE_CHECKING:
    CachePolicy: Any
    EdgeValidationResult: Any
    EvidenceCatalog: Any
    EvidenceNode: Any
    GraphDiagnostics: Any
    GraphEdge: Any
    GraphNode: Any
    GraphValidationSummary: Any
    IncrementalDiff: Any
    PlanArtifact: Any
    PlanCatalog: Any
    TaskGraphSnapshot: Any
    TaskNode: Any
    TaskSchedule: Any
    TaskValidationResult: Any
    TaskBuildContext: Any
    TaskCatalog: Any
    TaskExecutionRequest: Any
    TaskGraph: Any
    TaskKind: Any
    TaskSpec: Any
    build_task_catalog: Any
    build_task_graph: Any
    compile_task_catalog: Any
    compile_task_plan: Any
    diff_plan_catalog: Any
    execute_plan_artifact: Any
    impacted_tasks: Any
    impacted_tasks_for_evidence: Any
    provenance_for_task: Any
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
    task_graph_from_catalog: Any
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
    "CachePolicy",
    "EdgeValidationResult",
    "EvidenceCatalog",
    "EvidenceNode",
    "GraphDiagnostics",
    "GraphEdge",
    "GraphNode",
    "GraphValidationSummary",
    "IncrementalDiff",
    "PlanArtifact",
    "PlanCatalog",
    "TaskBuildContext",
    "TaskCatalog",
    "TaskExecutionRequest",
    "TaskGraph",
    "TaskGraphSnapshot",
    "TaskKind",
    "TaskNode",
    "TaskSchedule",
    "TaskSpec",
    "TaskValidationResult",
    "build_task_catalog",
    "build_task_graph",
    "compile_task_catalog",
    "compile_task_plan",
    "diff_plan_catalog",
    "execute_plan_artifact",
    "impacted_tasks",
    "impacted_tasks_for_evidence",
    "provenance_for_task",
    "ready_tasks_with_column_validation",
    "schedule_tasks",
    "task_graph_diagnostics",
    "task_graph_from_catalog",
    "task_graph_impact_subgraph",
    "task_graph_isolate_labels",
    "task_graph_node_label",
    "task_graph_node_link_json",
    "task_graph_signature",
    "task_graph_snapshot",
    "task_graph_subgraph",
    "task_schedule_metadata",
    "validate_edge_requirements",
    "validate_edge_requirements_detailed",
    "validate_graph_edges",
)
