"""Hamilton nodes for executing tasks from inferred plans."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import rustworkx as rx
from hamilton.function_modifiers import tag

from arrowdsl.core.interop import TableLike as ArrowTableLike
from arrowdsl.schema.build import empty_table
from arrowdsl.schema.serialization import schema_fingerprint
from cpg.schemas import CPG_EDGES_CONTRACT, CPG_NODES_CONTRACT, CPG_PROPS_CONTRACT
from datafusion_engine.finalize import Contract, normalize_only
from relspec.evidence import EvidenceCatalog
from relspec.execution import TaskExecutionRequest, execute_plan_artifact
from relspec.graph_inference import TaskGraph
from relspec.plan_catalog import PlanArtifact, PlanCatalog
from relspec.runtime_artifacts import MaterializedTableSpec, RuntimeArtifacts, TableLike
from relspec.rustworkx_graph import GraphNode, TaskNode
from relspec.rustworkx_schedule import schedule_tasks

if TYPE_CHECKING:
    from arrowdsl.core.execution_context import ExecutionContext
    from ibis_engine.execution import IbisExecutionContext
    from relspec.incremental import IncrementalDiff


@dataclass(frozen=True)
class TaskExecutionResults:
    """Collected outputs from task execution."""

    outputs: Mapping[str, TableLike]


def _finalize_cpg_table(
    table: TableLike,
    *,
    contract: Contract,
    ctx: ExecutionContext,
) -> TableLike:
    schema_names = getattr(table.schema, "names", [])
    if not schema_names:
        return empty_table(contract.schema)
    return normalize_only(cast("ArrowTableLike", table), contract=contract, ctx=ctx)


@dataclass(frozen=True)
class TaskRequirements:
    """Aggregated dependency requirements for task scheduling."""

    sources: set[str]
    columns: dict[str, set[str]]
    types: dict[str, dict[str, str]]
    metadata: dict[str, dict[bytes, bytes]]


def _initial_evidence(catalog: PlanCatalog, *, ctx_id: int | None = None) -> EvidenceCatalog:
    outputs = {artifact.task.output for artifact in catalog.artifacts}
    requirements = _collect_task_requirements(catalog)
    seed_sources = requirements.sources - outputs
    evidence = EvidenceCatalog(sources=set(seed_sources))
    for source in seed_sources:
        if evidence.register_from_registry(source, ctx_id=ctx_id):
            continue
        _seed_evidence_from_requirements(evidence, source, requirements)
    return evidence


def _collect_task_requirements(catalog: PlanCatalog) -> TaskRequirements:
    required_sources: set[str] = set()
    required_columns: dict[str, set[str]] = {}
    required_types: dict[str, dict[str, str]] = {}
    required_metadata: dict[str, dict[bytes, bytes]] = {}
    for artifact in catalog.artifacts:
        required_sources.update(artifact.deps.inputs)
        _merge_required_columns(required_columns, artifact.deps.required_columns)
        _merge_required_types(required_types, artifact.deps.required_types)
        _merge_required_metadata(required_metadata, artifact.deps.required_metadata)
    return TaskRequirements(
        sources=required_sources,
        columns=required_columns,
        types=required_types,
        metadata=required_metadata,
    )


def _merge_required_columns(
    target: dict[str, set[str]],
    incoming: Mapping[str, tuple[str, ...]],
) -> None:
    for source, cols in incoming.items():
        target.setdefault(source, set()).update(cols)


def _merge_required_types(
    target: dict[str, dict[str, str]],
    incoming: Mapping[str, tuple[tuple[str, str], ...]],
) -> None:
    for source, pairs in incoming.items():
        types_map = target.setdefault(source, {})
        for name, dtype in pairs:
            types_map.setdefault(name, dtype)


def _merge_required_metadata(
    target: dict[str, dict[bytes, bytes]],
    incoming: Mapping[str, tuple[tuple[bytes, bytes], ...]],
) -> None:
    for source, pairs in incoming.items():
        meta_map = target.setdefault(source, {})
        for key, value in pairs:
            meta_map.setdefault(key, value)


def _seed_evidence_from_requirements(
    evidence: EvidenceCatalog,
    source: str,
    requirements: TaskRequirements,
) -> None:
    cols = requirements.columns.get(source)
    if cols is not None:
        evidence.columns_by_dataset[source] = set(cols)
    types = requirements.types.get(source)
    if types is not None:
        evidence.types_by_dataset[source] = dict(types)
    metadata = requirements.metadata.get(source)
    if metadata is not None:
        evidence.metadata_by_dataset[source] = dict(metadata)


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
            source_task=artifact.task.name,
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
    df_profile = ibis_execution.ctx.runtime.datafusion
    ctx_id = id(df_profile.session_context()) if df_profile is not None else None
    evidence = _initial_evidence(plan_catalog, ctx_id=ctx_id)
    runtime = RuntimeArtifacts(execution=ibis_execution)
    schedule = schedule_tasks(
        task_graph,
        evidence=evidence,
        allow_partial=True,
    )
    by_task = _artifact_by_task(plan_catalog)
    allowed_tasks = allowed_task_names(task_graph, incremental_plan_diff)
    outputs: dict[str, TableLike] = {}
    for task_name in schedule.ordered_tasks:
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


def allowed_task_names(
    task_graph: TaskGraph,
    diff: IncrementalDiff | None,
) -> set[str] | None:
    """Return allowed task names based on an incremental diff.

    Returns
    -------
    set[str] | None
        Allowed task names when diff is provided, otherwise ``None``.
    """
    if diff is None:
        return None
    changed = (*diff.changed_tasks, *diff.added_tasks)
    if not changed:
        return None
    graph = task_graph.graph
    allowed: set[str] = set()
    for name in changed:
        idx = task_graph.task_idx.get(name)
        if idx is None:
            continue
        allowed.add(name)
        impacted = rx.ancestors(graph, idx) | rx.descendants(graph, idx)
        for node_idx in impacted:
            node = graph[node_idx]
            if not isinstance(node, GraphNode):
                continue
            if node.kind != "task":
                continue
            payload = node.payload
            if not isinstance(payload, TaskNode):
                continue
            allowed.add(payload.name)
    return allowed


@tag(layer="execution", artifact="cpg_nodes_final", kind="table")
def cpg_nodes_final(task_outputs: TaskExecutionResults, ctx: ExecutionContext) -> TableLike:
    """Return the final CPG nodes table.

    Returns
    -------
    TableLike
        Final nodes table.
    """
    table = task_outputs.outputs.get("cpg_nodes_v1")
    if table is None:
        return empty_table(CPG_NODES_CONTRACT.schema)
    return _finalize_cpg_table(table, contract=CPG_NODES_CONTRACT, ctx=ctx)


@tag(layer="execution", artifact="cpg_edges_final", kind="table")
def cpg_edges_final(task_outputs: TaskExecutionResults, ctx: ExecutionContext) -> TableLike:
    """Return the final CPG edges table.

    Returns
    -------
    TableLike
        Final edges table.
    """
    table = task_outputs.outputs.get("cpg_edges_v1")
    if table is None:
        return empty_table(CPG_EDGES_CONTRACT.schema)
    return _finalize_cpg_table(table, contract=CPG_EDGES_CONTRACT, ctx=ctx)


@tag(layer="execution", artifact="cpg_props_final", kind="table")
def cpg_props_final(task_outputs: TaskExecutionResults, ctx: ExecutionContext) -> TableLike:
    """Return the final CPG properties table.

    Returns
    -------
    TableLike
        Final properties table.
    """
    table = task_outputs.outputs.get("cpg_props_v1")
    if table is None:
        return empty_table(CPG_PROPS_CONTRACT.schema)
    return _finalize_cpg_table(table, contract=CPG_PROPS_CONTRACT, ctx=ctx)


__all__ = [
    "TaskExecutionResults",
    "allowed_task_names",
    "cpg_edges_final",
    "cpg_nodes_final",
    "cpg_props_final",
    "task_outputs",
]
