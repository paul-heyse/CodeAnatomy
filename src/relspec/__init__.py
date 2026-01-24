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
    "RuleGraph": ("relspec.rustworkx_graph", "RuleGraph"),
    "RuleGraphSnapshot": ("relspec.rustworkx_graph", "RuleGraphSnapshot"),
    "RuleNode": ("relspec.rustworkx_graph", "RuleNode"),
    "RuleSchedule": ("relspec.rustworkx_schedule", "RuleSchedule"),
    "RuleValidationResult": ("relspec.graph_edge_validation", "RuleValidationResult"),
    "TaskBuildContext": ("relspec.task_catalog", "TaskBuildContext"),
    "TaskCatalog": ("relspec.task_catalog", "TaskCatalog"),
    "TaskExecutionRequest": ("relspec.execution", "TaskExecutionRequest"),
    "TaskGraph": ("relspec.graph_inference", "TaskGraph"),
    "TaskKind": ("relspec.task_catalog", "TaskKind"),
    "TaskRegistry": ("relspec.task_registry", "TaskRegistry"),
    "TaskSpec": ("relspec.task_catalog", "TaskSpec"),
    "build_task_catalog": ("relspec.task_catalog_builders", "build_task_catalog"),
    "build_task_graph": ("relspec.graph_inference", "build_task_graph"),
    "compile_task_catalog": ("relspec.plan_catalog", "compile_task_catalog"),
    "compile_task_plan": ("relspec.plan_catalog", "compile_task_plan"),
    "diff_plan_catalog": ("relspec.incremental", "diff_plan_catalog"),
    "execute_plan_artifact": ("relspec.execution", "execute_plan_artifact"),
    "impacted_rules": ("relspec.rustworkx_schedule", "impacted_rules"),
    "impacted_rules_for_evidence": ("relspec.rustworkx_schedule", "impacted_rules_for_evidence"),
    "provenance_for_rule": ("relspec.rustworkx_schedule", "provenance_for_rule"),
    "ready_rules_with_column_validation": (
        "relspec.graph_edge_validation",
        "ready_rules_with_column_validation",
    ),
    "rule_graph_diagnostics": ("relspec.rustworkx_graph", "rule_graph_diagnostics"),
    "rule_graph_signature": ("relspec.rustworkx_graph", "rule_graph_signature"),
    "rule_graph_snapshot": ("relspec.rustworkx_graph", "rule_graph_snapshot"),
    "rule_schedule_metadata": ("relspec.rustworkx_schedule", "rule_schedule_metadata"),
    "schedule_rules": ("relspec.rustworkx_schedule", "schedule_rules"),
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
    RuleGraph: Any
    RuleGraphSnapshot: Any
    RuleNode: Any
    RuleSchedule: Any
    RuleValidationResult: Any
    TaskBuildContext: Any
    TaskCatalog: Any
    TaskExecutionRequest: Any
    TaskGraph: Any
    TaskKind: Any
    TaskRegistry: Any
    TaskSpec: Any
    build_task_catalog: Any
    build_task_graph: Any
    compile_task_catalog: Any
    compile_task_plan: Any
    diff_plan_catalog: Any
    execute_plan_artifact: Any
    impacted_rules: Any
    impacted_rules_for_evidence: Any
    provenance_for_rule: Any
    ready_rules_with_column_validation: Any
    rule_graph_diagnostics: Any
    rule_graph_signature: Any
    rule_graph_snapshot: Any
    rule_schedule_metadata: Any
    schedule_rules: Any
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
    "RuleGraph",
    "RuleGraphSnapshot",
    "RuleNode",
    "RuleSchedule",
    "RuleValidationResult",
    "TaskBuildContext",
    "TaskCatalog",
    "TaskExecutionRequest",
    "TaskGraph",
    "TaskKind",
    "TaskRegistry",
    "TaskSpec",
    "build_task_catalog",
    "build_task_graph",
    "compile_task_catalog",
    "compile_task_plan",
    "diff_plan_catalog",
    "execute_plan_artifact",
    "impacted_rules",
    "impacted_rules_for_evidence",
    "provenance_for_rule",
    "ready_rules_with_column_validation",
    "rule_graph_diagnostics",
    "rule_graph_signature",
    "rule_graph_snapshot",
    "rule_schedule_metadata",
    "schedule_rules",
    "task_graph_from_catalog",
    "validate_edge_requirements",
    "validate_edge_requirements_detailed",
    "validate_graph_edges",
)
