"""Hamilton nodes for executing tasks from inferred plans."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import rustworkx as rx
from hamilton.function_modifiers import tag

from arrowdsl.schema.build import empty_table
from arrowdsl.schema.serialization import schema_fingerprint
from relspec.evidence import EvidenceCatalog
from relspec.execution import TaskExecutionRequest, execute_plan_artifact
from relspec.graph_inference import TaskGraph
from relspec.plan_catalog import PlanArtifact, PlanCatalog
from relspec.runtime_artifacts import MaterializedTableSpec, RuntimeArtifacts, TableLike
from relspec.rustworkx_graph import GraphNode, RuleNode
from relspec.rustworkx_schedule import schedule_rules

if TYPE_CHECKING:
    from arrowdsl.core.execution_context import ExecutionContext
    from ibis_engine.execution import IbisExecutionContext
    from relspec.incremental import IncrementalDiff


@dataclass(frozen=True)
class TaskExecutionResults:
    """Collected outputs from task execution."""

    outputs: Mapping[str, TableLike]


def _initial_evidence(catalog: PlanCatalog) -> EvidenceCatalog:
    outputs = {artifact.task.output for artifact in catalog.artifacts}
    required_sources: set[str] = set()
    required_columns: dict[str, set[str]] = {}
    for artifact in catalog.artifacts:
        required_sources.update(artifact.deps.inputs)
        for source, cols in artifact.deps.required_columns.items():
            required_columns.setdefault(source, set()).update(cols)
    seed_sources = required_sources - outputs
    evidence = EvidenceCatalog(sources=set(seed_sources))
    for source, cols in required_columns.items():
        if source in seed_sources:
            evidence.columns_by_dataset[source] = set(cols)
    return evidence


def _artifact_by_task(catalog: PlanCatalog) -> dict[str, PlanArtifact]:
    return {artifact.task.name: artifact for artifact in catalog.artifacts}


def _record_output(
    artifact: PlanArtifact,
    *,
    table: TableLike,
    outputs: dict[str, TableLike],
    evidence: EvidenceCatalog,
    runtime: RuntimeArtifacts,
) -> None:
    outputs[artifact.task.output] = table
    schema = table.schema
    evidence.register(artifact.task.output, schema)
    runtime.register_materialized(
        artifact.task.output,
        table,
        spec=MaterializedTableSpec(
            source_rule=artifact.task.name,
            schema_fingerprint=schema_fingerprint(schema),
            plan_fingerprint=artifact.plan_fingerprint,
        ),
    )


@tag(layer="execution", artifact="task_outputs", kind="bundle")
def task_outputs(
    plan_catalog: PlanCatalog,
    task_graph: TaskGraph,
    _ctx: ExecutionContext,
    ibis_execution: IbisExecutionContext,
    incremental_plan_diff: IncrementalDiff | None = None,
) -> TaskExecutionResults:
    """Execute tasks in inferred order and return output tables.

    Returns
    -------
    TaskExecutionResults
        Output tables keyed by dataset name.
    """
    evidence = _initial_evidence(plan_catalog)
    runtime = RuntimeArtifacts(execution=ibis_execution)
    schedule = schedule_rules(
        task_graph.graph,
        evidence=evidence,
        allow_partial=True,
    )
    by_task = _artifact_by_task(plan_catalog)
    allowed_tasks = _allowed_task_names(task_graph, incremental_plan_diff)
    outputs: dict[str, TableLike] = {}
    for task_name in schedule.ordered_rules:
        if allowed_tasks is not None and task_name not in allowed_tasks:
            continue
        artifact = by_task.get(task_name)
        if artifact is None:
            continue
        table = execute_plan_artifact(TaskExecutionRequest(artifact=artifact, runtime=runtime))
        _record_output(
            artifact,
            table=table,
            outputs=outputs,
            evidence=evidence,
            runtime=runtime,
        )
    return TaskExecutionResults(outputs=outputs)


def _allowed_task_names(
    task_graph: TaskGraph,
    diff: IncrementalDiff | None,
) -> set[str] | None:
    if diff is None:
        return None
    changed = (*diff.changed_tasks, *diff.added_tasks)
    if not changed:
        return None
    graph = task_graph.graph.graph
    allowed: set[str] = set()
    for name in changed:
        idx = task_graph.graph.rule_idx.get(name)
        if idx is None:
            continue
        allowed.add(name)
        impacted = rx.ancestors(graph, idx) | rx.descendants(graph, idx)
        for node_idx in impacted:
            node = graph[node_idx]
            if not isinstance(node, GraphNode):
                continue
            if node.kind != "rule":
                continue
            payload = node.payload
            if not isinstance(payload, RuleNode):
                continue
            allowed.add(payload.name)
    return allowed


@tag(layer="execution", artifact="cpg_nodes_final", kind="table")
def cpg_nodes_final(task_outputs: TaskExecutionResults) -> TableLike:
    """Return the final CPG nodes table.

    Returns
    -------
    TableLike
        Final nodes table.
    """
    return task_outputs.outputs.get("cpg_nodes_v1", empty_table(pa.schema([])))


@tag(layer="execution", artifact="cpg_edges_final", kind="table")
def cpg_edges_final(task_outputs: TaskExecutionResults) -> TableLike:
    """Return the final CPG edges table.

    Returns
    -------
    TableLike
        Final edges table.
    """
    return task_outputs.outputs.get("cpg_edges_v1", empty_table(pa.schema([])))


@tag(layer="execution", artifact="cpg_props_final", kind="table")
def cpg_props_final(task_outputs: TaskExecutionResults) -> TableLike:
    """Return the final CPG properties table.

    Returns
    -------
    TableLike
        Final properties table.
    """
    return task_outputs.outputs.get("cpg_props_v1", empty_table(pa.schema([])))


__all__ = [
    "TaskExecutionResults",
    "cpg_edges_final",
    "cpg_nodes_final",
    "cpg_props_final",
    "task_outputs",
]
