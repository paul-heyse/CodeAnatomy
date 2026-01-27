"""Canonical execution plan compilation for rustworkx-driven scheduling."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import rustworkx as rx

from incremental.plan_fingerprints import PlanFingerprintSnapshot
from relspec.evidence import EvidenceCatalog, initial_evidence_from_views, known_dataset_specs
from relspec.inferred_deps import InferredDeps, infer_deps_from_view_nodes
from relspec.rustworkx_graph import (
    GraphDiagnostics,
    TaskGraph,
    build_task_graph_from_inferred_deps,
    task_graph_diagnostics,
    task_graph_signature,
    task_graph_snapshot,
    task_graph_subgraph,
)
from relspec.rustworkx_schedule import TaskSchedule, schedule_tasks, task_schedule_metadata
from relspec.schedule_events import TaskScheduleMetadata
from relspec.view_defs import (
    DEFAULT_REL_TASK_PRIORITY,
    REL_CALLSITE_QNAME_OUTPUT,
    REL_CALLSITE_SYMBOL_OUTPUT,
    REL_DEF_SYMBOL_OUTPUT,
    REL_IMPORT_SYMBOL_OUTPUT,
    REL_NAME_SYMBOL_OUTPUT,
    RELATION_OUTPUT_NAME,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.view_graph_registry import ViewNode
    from schema_spec.system import ContractSpec, DatasetSpec
    OutputContract = ContractSpec | DatasetSpec | object
else:
    OutputContract = object

_RELSPEC_OUTPUTS: frozenset[str] = frozenset(
    {
        REL_NAME_SYMBOL_OUTPUT,
        REL_IMPORT_SYMBOL_OUTPUT,
        REL_DEF_SYMBOL_OUTPUT,
        REL_CALLSITE_SYMBOL_OUTPUT,
        REL_CALLSITE_QNAME_OUTPUT,
        RELATION_OUTPUT_NAME,
    }
)


@dataclass(frozen=True)
class ExecutionPlan:
    """Compiled execution plan that unifies scheduling and execution."""

    view_nodes: tuple[ViewNode, ...]
    task_graph: TaskGraph
    evidence: EvidenceCatalog
    task_schedule: TaskSchedule
    schedule_metadata: Mapping[str, TaskScheduleMetadata]
    plan_fingerprints: Mapping[str, str]
    plan_snapshots: Mapping[str, PlanFingerprintSnapshot]
    plan_signature: str
    diagnostics: GraphDiagnostics
    bottom_level_costs: Mapping[str, float]
    dependency_map: Mapping[str, tuple[str, ...]]
    dataset_specs: Mapping[str, DatasetSpec]
    active_tasks: frozenset[str]


@dataclass(frozen=True)
class ExecutionPlanRequest:
    """Describe inputs to the execution plan compiler."""

    view_nodes: Sequence[ViewNode]
    snapshot: Mapping[str, object] | None = None
    requested_task_names: Iterable[str] | None = None
    impacted_task_names: Iterable[str] | None = None
    allow_partial: bool = False


@dataclass(frozen=True)
class _PlanBuildContext:
    """Bundle intermediate plan compilation artifacts."""

    view_nodes: tuple[ViewNode, ...]
    inferred: tuple[InferredDeps, ...]
    task_graph: TaskGraph
    active_tasks: set[str]
    output_contracts: Mapping[str, OutputContract]
    evidence: EvidenceCatalog
    dataset_spec_map: Mapping[str, DatasetSpec]


def priority_for_task(task_name: str) -> int:
    """Return a deterministic scheduling priority for a task name.

    Returns
    -------
    int
        Priority value used by the scheduler.
    """
    if task_name in _RELSPEC_OUTPUTS:
        return DEFAULT_REL_TASK_PRIORITY
    if task_name.startswith("cpg_"):
        return 100
    return 100


def compile_execution_plan(
    *,
    session: SessionContext,
    request: ExecutionPlanRequest,
) -> ExecutionPlan:
    """Compile the canonical execution plan for the current session.

    Parameters
    ----------
    session : SessionContext
        Active DataFusion session used for schema discovery.
    request : ExecutionPlanRequest
        Inputs that shape plan compilation and pruning.

    Returns
    -------
    ExecutionPlan
        Fully compiled execution plan for this session.

    """
    context = _prepare_plan_context(session, request)
    schedule = schedule_tasks(
        context.task_graph,
        evidence=context.evidence,
        output_schema_for=context.output_contracts.get,
        allow_partial=request.allow_partial,
    )
    schedule_meta = task_schedule_metadata(schedule)
    plan_fingerprints = _plan_fingerprint_map(context.inferred)
    plan_snapshots = _plan_snapshot_map(context.view_nodes, plan_fingerprints)
    signature, diagnostics = _plan_signature_and_diagnostics(
        context.task_graph,
        plan_fingerprints,
    )
    bottom_costs = bottom_level_costs(context.task_graph.graph)
    dependency_map = dependency_map_from_inferred(
        context.inferred,
        active_tasks=context.active_tasks,
    )
    return ExecutionPlan(
        view_nodes=context.view_nodes,
        task_graph=context.task_graph,
        evidence=context.evidence,
        task_schedule=schedule,
        schedule_metadata=schedule_meta,
        plan_fingerprints=plan_fingerprints,
        plan_snapshots=plan_snapshots,
        plan_signature=signature,
        diagnostics=diagnostics,
        bottom_level_costs=bottom_costs,
        dependency_map=dependency_map,
        dataset_specs=context.dataset_spec_map,
        active_tasks=frozenset(context.active_tasks),
    )


def _prepare_plan_context(
    session: SessionContext,
    request: ExecutionPlanRequest,
) -> _PlanBuildContext:
    nodes_with_ast, missing_ast = _partition_view_nodes(request.view_nodes)
    requested = set(request.requested_task_names or ())
    if requested and missing_ast & requested:
        missing = sorted(missing_ast & requested)
        msg = f"Requested tasks are missing SQLGlot ASTs: {missing}."
        raise ValueError(msg)
    if not nodes_with_ast:
        msg = "Execution plan requires view nodes with SQLGlot ASTs."
        raise ValueError(msg)

    inferred = infer_deps_from_view_nodes(nodes_with_ast, snapshot=request.snapshot)
    priorities = _priority_map(inferred)
    dataset_spec_map = _dataset_spec_map(session)
    graph = build_task_graph_from_inferred_deps(
        inferred,
        priorities=priorities,
        extra_evidence=tuple(sorted(dataset_spec_map)),
    )
    active_tasks = _resolve_active_tasks(
        graph,
        requested_task_names=requested,
        impacted_task_names=request.impacted_task_names,
        allow_partial=request.allow_partial,
    )
    pruned_graph = _prune_task_graph(graph, active_tasks=active_tasks)
    pruned_nodes = _prune_view_nodes(nodes_with_ast, active_tasks=active_tasks)
    pruned_inferred = tuple(dep for dep in inferred if dep.task_name in active_tasks)
    output_contracts = _output_contract_map(
        session,
        pruned_nodes,
        dataset_spec_map=dataset_spec_map,
    )
    evidence = initial_evidence_from_views(
        pruned_nodes,
        ctx=session,
        task_names=set(active_tasks),
        dataset_specs=tuple(dataset_spec_map[name] for name in sorted(dataset_spec_map)),
    )
    return _PlanBuildContext(
        view_nodes=pruned_nodes,
        inferred=pruned_inferred,
        task_graph=pruned_graph,
        active_tasks=active_tasks,
        output_contracts=output_contracts,
        evidence=evidence,
        dataset_spec_map=dataset_spec_map,
    )


def _dataset_spec_map(session: SessionContext) -> Mapping[str, DatasetSpec]:
    dataset_specs = known_dataset_specs(ctx=session)
    return {spec.name: spec for spec in dataset_specs}


def _plan_signature_and_diagnostics(
    graph: TaskGraph,
    plan_fingerprints: Mapping[str, str],
) -> tuple[str, GraphDiagnostics]:
    snapshot_payload = task_graph_snapshot(
        graph,
        label="execution_plan",
        task_signatures=plan_fingerprints,
    )
    signature = task_graph_signature(snapshot_payload)
    diagnostics = task_graph_diagnostics(graph, include_node_link=True)
    return signature, diagnostics


def dependency_map_from_inferred(
    inferred: Sequence[InferredDeps],
    *,
    active_tasks: Iterable[str],
) -> dict[str, tuple[str, ...]]:
    """Build a task-to-task dependency map from inferred dependencies.

    Returns
    -------
    dict[str, tuple[str, ...]]
        Mapping of output task name to sorted dependency names.
    """
    outputs = set(active_tasks)
    dependency_map: dict[str, tuple[str, ...]] = {}
    for dep in inferred:
        inputs = tuple(sorted(name for name in dep.inputs if name in outputs))
        dependency_map[dep.output] = inputs
    return dependency_map


def bottom_level_costs(graph: rx.PyDiGraph) -> dict[str, float]:
    """Compute bottom-level costs to prioritize critical-path tasks.

    Returns
    -------
    dict[str, float]
        Mapping of task name to critical-path priority score.
    """
    topo = list(rx.topological_sort(graph))
    bottom: dict[int, float] = {}
    for node_idx in reversed(topo):
        succs = graph.successor_indices(node_idx)
        best_succ = max((bottom[s] for s in succs), default=0.0)
        node = graph[node_idx]
        cost = 1.0
        if getattr(node, "kind", None) == "task":
            payload = getattr(node, "payload", None)
            priority = getattr(payload, "priority", 100)
            cost = float(max(priority, 1))
        bottom[node_idx] = cost + best_succ
    out: dict[str, float] = {}
    for node_idx, score in bottom.items():
        node = graph[node_idx]
        if getattr(node, "kind", None) != "task":
            continue
        payload = getattr(node, "payload", None)
        name = getattr(payload, "name", None)
        if isinstance(name, str):
            out[name] = score
    return out


def _partition_view_nodes(
    view_nodes: Sequence[ViewNode],
) -> tuple[tuple[ViewNode, ...], set[str]]:
    nodes_with_ast: list[ViewNode] = []
    missing: set[str] = set()
    for node in view_nodes:
        if node.sqlglot_ast is None:
            missing.add(node.name)
            continue
        nodes_with_ast.append(node)
    return tuple(nodes_with_ast), missing


def _priority_map(inferred: Sequence[InferredDeps]) -> dict[str, int]:
    priorities: dict[str, int] = {}
    for dep in inferred:
        priorities[dep.task_name] = priority_for_task(dep.task_name)
    return priorities


def _task_names_from_node_ids(graph: TaskGraph, node_ids: Iterable[int]) -> set[str]:
    names: set[str] = set()
    for node_idx in set(node_ids):
        node = graph.graph[node_idx]
        if getattr(node, "kind", None) != "task":
            continue
        payload = getattr(node, "payload", None)
        name = getattr(payload, "name", None)
        if isinstance(name, str):
            names.add(name)
    return names


def upstream_task_closure(graph: TaskGraph, task_names: Iterable[str]) -> set[str]:
    """Return upstream task closure for the requested tasks.

    Returns
    -------
    set[str]
        Task names including upstream dependencies.
    """
    node_ids: set[int] = set()
    for name in task_names:
        task_idx = graph.task_idx.get(name)
        if task_idx is None:
            continue
        node_ids.add(task_idx)
        node_ids.update(rx.ancestors(graph.graph, task_idx))
        evidence_idx = graph.evidence_idx.get(name)
        if evidence_idx is not None:
            node_ids.add(evidence_idx)
    return _task_names_from_node_ids(graph, node_ids)


def downstream_task_closure(graph: TaskGraph, task_names: Iterable[str]) -> set[str]:
    """Return downstream task closure for the requested tasks.

    Returns
    -------
    set[str]
        Task names including downstream dependents.
    """
    node_ids: set[int] = set()
    for name in task_names:
        task_idx = graph.task_idx.get(name)
        if task_idx is None:
            continue
        node_ids.add(task_idx)
        node_ids.update(rx.descendants(graph.graph, task_idx))
        evidence_idx = graph.evidence_idx.get(name)
        if evidence_idx is not None:
            node_ids.add(evidence_idx)
    return _task_names_from_node_ids(graph, node_ids)


def _resolve_active_tasks(
    graph: TaskGraph,
    *,
    requested_task_names: Iterable[str],
    impacted_task_names: Iterable[str] | None,
    allow_partial: bool,
) -> set[str]:
    all_tasks = set(graph.task_idx)
    active = set(all_tasks)
    if requested_task_names:
        requested = upstream_task_closure(graph, requested_task_names)
        if not requested and not allow_partial:
            missing = sorted(set(requested_task_names) - all_tasks)
            msg = f"Requested tasks are not present in the task graph: {missing}."
            raise ValueError(msg)
        active = requested
    if impacted_task_names:
        impacted = downstream_task_closure(graph, impacted_task_names)
        if impacted:
            active = active & impacted if requested_task_names else impacted
            # Ensure dependencies are available even when pruning by impact.
            active = upstream_task_closure(graph, active)
    if not active and not allow_partial:
        msg = "Execution plan resolved to zero active tasks."
        raise ValueError(msg)
    return active


def _prune_task_graph(graph: TaskGraph, *, active_tasks: set[str]) -> TaskGraph:
    if active_tasks == set(graph.task_idx):
        return graph
    node_ids: set[int] = set()
    for name in active_tasks:
        task_idx = graph.task_idx.get(name)
        if task_idx is None:
            continue
        node_ids.add(task_idx)
        node_ids.update(rx.ancestors(graph.graph, task_idx))
        evidence_idx = graph.evidence_idx.get(name)
        if evidence_idx is not None:
            node_ids.add(evidence_idx)
    return task_graph_subgraph(graph, node_ids=node_ids)


def _prune_view_nodes(
    view_nodes: Sequence[ViewNode],
    *,
    active_tasks: set[str],
) -> tuple[ViewNode, ...]:
    return tuple(node for node in view_nodes if node.name in active_tasks)


def _plan_fingerprint_map(inferred: Sequence[InferredDeps]) -> dict[str, str]:
    return {dep.task_name: dep.plan_fingerprint for dep in inferred}


def _plan_snapshot_map(
    view_nodes: Sequence[ViewNode],
    plan_fingerprints: Mapping[str, str],
) -> dict[str, PlanFingerprintSnapshot]:
    snapshots: dict[str, PlanFingerprintSnapshot] = {}
    for node in view_nodes:
        if node.sqlglot_ast is None:
            msg = f"View node {node.name!r} is missing a SQLGlot AST."
            raise ValueError(msg)
        snapshots[node.name] = PlanFingerprintSnapshot(
            plan_fingerprint=plan_fingerprints.get(node.name, ""),
            sqlglot_ast=node.sqlglot_ast,
        )
    return snapshots


def _output_contract_map(
    session: SessionContext,
    view_nodes: Sequence[ViewNode],
    *,
    dataset_spec_map: Mapping[str, DatasetSpec],
) -> dict[str, OutputContract]:
    contracts: dict[str, OutputContract] = dict(dataset_spec_map)
    for node in view_nodes:
        contract_builder = node.contract_builder
        if contract_builder is None:
            continue
        if not session.table_exist(node.name):
            continue
        schema = _arrow_schema_from_session_table(session, node.name)
        contracts[node.name] = contract_builder(schema)
    return contracts


def _arrow_schema_from_session_table(session: SessionContext, name: str) -> pa.Schema:
    schema = session.table(name).schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = f"Failed to resolve Arrow schema for table {name!r}."
    raise TypeError(msg)


__all__ = [
    "ExecutionPlan",
    "ExecutionPlanRequest",
    "bottom_level_costs",
    "compile_execution_plan",
    "dependency_map_from_inferred",
    "downstream_task_closure",
    "priority_for_task",
    "upstream_task_closure",
]
