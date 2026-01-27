"""Canonical execution plan compilation for rustworkx-driven scheduling."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import rustworkx as rx

from incremental.plan_fingerprints import PlanFingerprintSnapshot
from relspec.evidence import (
    EvidenceCatalog,
    initial_evidence_from_views,
    known_dataset_specs,
)
from relspec.inferred_deps import InferredDeps, infer_deps_from_view_nodes
from relspec.rustworkx_graph import (
    GraphDiagnostics,
    TaskDependencyReduction,
    TaskGraph,
    TaskGraphBuildOptions,
    TaskNode,
    build_task_graph_from_inferred_deps,
    task_dependency_critical_path_length,
    task_dependency_critical_path_tasks,
    task_dependency_reduction,
    task_graph_diagnostics,
    task_graph_signature,
    task_graph_snapshot,
    task_graph_subgraph,
)
from relspec.rustworkx_schedule import (
    TaskSchedule,
    schedule_tasks,
    task_schedule_metadata,
)
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

    from datafusion_engine.dataset_registry import DatasetLocation
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.scan_planner import ScanUnit
    from datafusion_engine.schema_contracts import SchemaContract
    from datafusion_engine.view_graph_registry import ViewNode
    from relspec.incremental import IncrementalDiff
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
    task_dependency_graph: rx.PyDiGraph
    reduced_task_dependency_graph: rx.PyDiGraph
    evidence: EvidenceCatalog
    task_schedule: TaskSchedule
    schedule_metadata: Mapping[str, TaskScheduleMetadata]
    plan_fingerprints: Mapping[str, str]
    plan_snapshots: Mapping[str, PlanFingerprintSnapshot]
    output_contracts: Mapping[str, OutputContract]
    plan_signature: str
    task_dependency_signature: str
    reduced_task_dependency_signature: str
    reduction_node_map: Mapping[int, int]
    reduction_edge_count: int
    reduction_removed_edge_count: int
    diagnostics: GraphDiagnostics
    critical_path_task_names: tuple[str, ...]
    critical_path_length_weighted: float | None
    bottom_level_costs: Mapping[str, float]
    dependency_map: Mapping[str, tuple[str, ...]]
    dataset_specs: Mapping[str, DatasetSpec]
    active_tasks: frozenset[str]
    scan_units: tuple[ScanUnit, ...] = ()
    scan_unit_delta_pins: Mapping[str, int] = field(default_factory=dict)
    scan_keys_by_task: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    requested_task_names: tuple[str, ...] = ()
    impacted_task_names: tuple[str, ...] = ()
    allow_partial: bool = False
    incremental_diff: IncrementalDiff | None = None
    incremental_state_dir: str | None = None


@dataclass(frozen=True)
class ExecutionPlanRequest:
    """Describe inputs to the execution plan compiler."""

    view_nodes: Sequence[ViewNode]
    snapshot: Mapping[str, object] | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
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
    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]
    requested_task_names: tuple[str, ...]
    impacted_task_names: tuple[str, ...]
    allow_partial: bool


@dataclass(frozen=True)
class _PruneInputs:
    graph: TaskGraph
    nodes_with_ast: tuple[ViewNode, ...]
    inferred: Sequence[InferredDeps]
    scan_units: Sequence[ScanUnit]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True)
class _PrunedPlanBundle:
    """Pruned graph and node bundle."""

    task_graph: TaskGraph
    active_tasks: set[str]
    view_nodes: tuple[ViewNode, ...]
    inferred: tuple[InferredDeps, ...]
    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True)
class _ContractsEvidence:
    """Contracts and evidence bundle."""

    output_contracts: Mapping[str, OutputContract]
    evidence: EvidenceCatalog


@dataclass(frozen=True)
class _PrunedPlanComponents:
    """Pruned plan components used for recomputation."""

    task_graph: TaskGraph
    view_nodes: tuple[ViewNode, ...]
    plan_fingerprints: Mapping[str, str]
    plan_snapshots: Mapping[str, PlanFingerprintSnapshot]
    output_contracts: Mapping[str, OutputContract]
    dependency_map: Mapping[str, tuple[str, ...]]
    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]
    requested_task_names: tuple[str, ...]
    impacted_task_names: tuple[str, ...]
    incremental_diff: IncrementalDiff | None


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


def _scan_unit_delta_pins(scan_units: Sequence[ScanUnit]) -> dict[str, int]:
    """Extract Delta version pins from scan units for determinism tracking.

    Parameters
    ----------
    scan_units
        Sequence of scan units with optional Delta version pins.

    Returns
    -------
    dict[str, int]
        Mapping of dataset name to pinned Delta version.

    Raises
    ------
    ValueError
        When conflicting Delta versions exist for the same dataset.
    """
    pins: dict[str, int] = {}
    for unit in scan_units:
        if unit.delta_version is not None:
            existing = pins.get(unit.dataset_name)
            if existing is not None and existing != unit.delta_version:
                msg = (
                    f"Conflicting Delta versions for {unit.dataset_name!r}: "
                    f"{existing} vs {unit.delta_version}"
                )
                raise ValueError(msg)
            pins[unit.dataset_name] = unit.delta_version
    return pins


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
    plan_fingerprints = _plan_fingerprint_map(context.inferred)
    reduction = _task_dependency_reduction(context.task_graph, plan_fingerprints)
    output_schema_for = _output_schema_lookup(context.output_contracts)
    schedule = schedule_tasks(
        context.task_graph,
        evidence=context.evidence,
        output_schema_for=output_schema_for,
        allow_partial=request.allow_partial,
        reduced_dependency_graph=reduction.reduced_graph,
    )
    schedule_meta = task_schedule_metadata(schedule)
    plan_snapshots = _plan_snapshot_map(context.view_nodes, plan_fingerprints)
    signature, diagnostics = _plan_signature_and_diagnostics(
        context.task_graph,
        plan_fingerprints,
        reduction=reduction,
    )
    bottom_costs = bottom_level_costs(reduction.reduced_graph)
    dependency_map = dependency_map_from_inferred(
        context.inferred,
        active_tasks=context.active_tasks,
    )
    scan_delta_pins = _scan_unit_delta_pins(context.scan_units)
    return ExecutionPlan(
        view_nodes=context.view_nodes,
        task_graph=context.task_graph,
        task_dependency_graph=reduction.full_graph,
        reduced_task_dependency_graph=reduction.reduced_graph,
        evidence=context.evidence,
        task_schedule=schedule,
        schedule_metadata=schedule_meta,
        plan_fingerprints=plan_fingerprints,
        plan_snapshots=plan_snapshots,
        output_contracts=context.output_contracts,
        plan_signature=signature,
        task_dependency_signature=reduction.full_signature,
        reduced_task_dependency_signature=reduction.reduced_signature,
        reduction_node_map=reduction.node_map,
        reduction_edge_count=reduction.edge_count,
        reduction_removed_edge_count=reduction.removed_edge_count,
        diagnostics=diagnostics,
        critical_path_task_names=diagnostics.critical_path_task_names,
        critical_path_length_weighted=diagnostics.critical_path_length_weighted,
        bottom_level_costs=bottom_costs,
        dependency_map=dependency_map,
        dataset_specs=context.dataset_spec_map,
        active_tasks=frozenset(context.active_tasks),
        scan_units=context.scan_units,
        scan_unit_delta_pins=scan_delta_pins,
        scan_keys_by_task=context.scan_keys_by_task,
        requested_task_names=context.requested_task_names,
        impacted_task_names=context.impacted_task_names,
        allow_partial=context.allow_partial,
    )


def _validated_view_nodes(
    view_nodes: Sequence[ViewNode],
    *,
    requested: set[str],
) -> tuple[ViewNode, ...]:
    nodes_with_plan, missing_plan = _partition_view_nodes(view_nodes)
    if requested and missing_plan & requested:
        missing = sorted(missing_plan & requested)
        msg = (
            f"Requested tasks are missing plan_bundle: {missing}. "
            "Plan bundles are required for dependency inference."
        )
        raise ValueError(msg)
    if not nodes_with_plan:
        msg = (
            "Execution plan requires view nodes with plan_bundle. "
            "Plan bundles are required for DataFusion-native lineage extraction."
        )
        raise ValueError(msg)
    return nodes_with_plan


def _pruned_plan_bundle(
    *,
    inputs: _PruneInputs,
    request: ExecutionPlanRequest,
    requested: set[str],
) -> _PrunedPlanBundle:
    active_tasks = _resolve_active_tasks(
        inputs.graph,
        requested_task_names=requested,
        impacted_task_names=request.impacted_task_names,
        allow_partial=request.allow_partial,
    )
    pruned_graph = _prune_task_graph(inputs.graph, active_tasks=active_tasks)
    pruned_nodes = _prune_view_nodes(inputs.nodes_with_ast, active_tasks=active_tasks)
    pruned_inferred = tuple(dep for dep in inputs.inferred if dep.task_name in active_tasks)
    pruned_scan_units, pruned_scan_keys_by_task = _prune_scan_units(
        scan_units=inputs.scan_units,
        scan_keys_by_task=inputs.scan_keys_by_task,
        active_tasks=active_tasks,
    )
    return _PrunedPlanBundle(
        task_graph=pruned_graph,
        active_tasks=active_tasks,
        view_nodes=pruned_nodes,
        inferred=pruned_inferred,
        scan_units=pruned_scan_units,
        scan_keys_by_task=pruned_scan_keys_by_task,
    )


def _contracts_and_evidence(
    *,
    session: SessionContext,
    pruned: _PrunedPlanBundle,
    dataset_spec_map: Mapping[str, DatasetSpec],
    scan_keys: Iterable[str] = (),
) -> _ContractsEvidence:
    output_contracts = _output_contract_map(
        session,
        pruned.view_nodes,
        dataset_spec_map=dataset_spec_map,
    )
    dataset_specs = tuple(dataset_spec_map[name] for name in sorted(dataset_spec_map))
    evidence = initial_evidence_from_views(
        pruned.view_nodes,
        ctx=session,
        task_names=set(pruned.active_tasks),
        dataset_specs=dataset_specs,
    )
    for scan_key in scan_keys:
        if scan_key:
            evidence.sources.add(scan_key)
    return _ContractsEvidence(output_contracts=output_contracts, evidence=evidence)


def _prepare_plan_context(
    session: SessionContext,
    request: ExecutionPlanRequest,
) -> _PlanBuildContext:
    requested = set(request.requested_task_names or ())
    nodes_with_ast = _validated_view_nodes(request.view_nodes, requested=requested)
    inferred = infer_deps_from_view_nodes(nodes_with_ast, snapshot=request.snapshot)
    priorities = _priority_map(inferred)
    dataset_spec_map = _dataset_spec_map(session)
    scan_units, scan_keys_by_task = _scan_units_for_inferred(
        session,
        inferred,
        runtime_profile=request.runtime_profile,
    )
    graph = build_task_graph_from_inferred_deps(
        inferred,
        options=TaskGraphBuildOptions(
            priorities=priorities,
            extra_evidence=tuple(sorted(dataset_spec_map)),
            scan_units=scan_units,
            scan_keys_by_task=scan_keys_by_task,
        ),
    )
    pruned = _pruned_plan_bundle(
        inputs=_PruneInputs(
            graph=graph,
            nodes_with_ast=nodes_with_ast,
            inferred=inferred,
            scan_units=scan_units,
            scan_keys_by_task=scan_keys_by_task,
        ),
        request=request,
        requested=requested,
    )
    scan_keys = _scan_keys_from_mapping(pruned.scan_keys_by_task)
    if request.runtime_profile is not None:
            from datafusion_engine.plan_artifact_store import (
                PlanArtifactsForViewsRequest,
                persist_plan_artifacts_for_views,
            )

        try:
            persist_plan_artifacts_for_views(
                session,
                request.runtime_profile,
                request=PlanArtifactsForViewsRequest(
                    view_nodes=pruned.view_nodes,
                    scan_units=pruned.scan_units,
                    scan_keys_by_view=pruned.scan_keys_by_task,
                ),
            )
        except (RuntimeError, ValueError, OSError, KeyError, ImportError, TypeError) as exc:
            from datafusion_engine.diagnostics import record_artifact

            record_artifact(
                request.runtime_profile,
                "plan_artifacts_store_failed_v1",
                {
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "view_count": len(pruned.view_nodes),
                    "scan_unit_count": len(pruned.scan_units),
                },
            )
    contracts_evidence = _contracts_and_evidence(
        session=session,
        pruned=pruned,
        dataset_spec_map=dataset_spec_map,
        scan_keys=scan_keys,
    )
    requested_task_names = tuple(sorted(requested))
    impacted_task_names = tuple(sorted(set(request.impacted_task_names or ())))
    return _PlanBuildContext(
        view_nodes=pruned.view_nodes,
        inferred=pruned.inferred,
        task_graph=pruned.task_graph,
        active_tasks=pruned.active_tasks,
        scan_units=pruned.scan_units,
        scan_keys_by_task=pruned.scan_keys_by_task,
        output_contracts=contracts_evidence.output_contracts,
        evidence=contracts_evidence.evidence,
        dataset_spec_map=dataset_spec_map,
        requested_task_names=requested_task_names,
        impacted_task_names=impacted_task_names,
        allow_partial=bool(request.allow_partial),
    )


def _dataset_spec_map(session: SessionContext) -> Mapping[str, DatasetSpec]:
    dataset_specs = known_dataset_specs(ctx=session)
    return {spec.name: spec for spec in dataset_specs}


def _dataset_location_map(
    profile: DataFusionRuntimeProfile | None,
) -> dict[str, DatasetLocation]:
    if profile is None:
        return {}
    locations: dict[str, DatasetLocation] = {}
    for name, location in profile.extract_dataset_locations.items():
        locations.setdefault(name, location)
    for name, location in profile.scip_dataset_locations.items():
        locations.setdefault(name, location)
    for catalog in profile.registry_catalogs.values():
        for name in catalog.names():
            if name in locations:
                continue
            try:
                locations[name] = catalog.get(name)
            except KeyError:
                continue
    return locations


def _scan_units_for_inferred(
    session: SessionContext,
    inferred: Sequence[InferredDeps],
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> tuple[tuple[ScanUnit, ...], dict[str, tuple[str, ...]]]:
    scans_by_task = {dep.task_name: dep.scans for dep in inferred if dep.scans}
    if not scans_by_task:
        return (), {}
    dataset_locations = _dataset_location_map(runtime_profile)
    from datafusion_engine.scan_planner import plan_scan_units

    return plan_scan_units(
        session,
        dataset_locations=dataset_locations,
        scans_by_task=scans_by_task,
    )


def _prune_scan_units(
    *,
    scan_units: Sequence[ScanUnit],
    scan_keys_by_task: Mapping[str, tuple[str, ...]],
    active_tasks: set[str],
) -> tuple[tuple[ScanUnit, ...], dict[str, tuple[str, ...]]]:
    active_scan_keys: set[str] = set()
    pruned_scan_keys_by_task: dict[str, tuple[str, ...]] = {}
    for task_name in sorted(active_tasks):
        keys = scan_keys_by_task.get(task_name, ())
        if not keys:
            continue
        deduped_keys = tuple(dict.fromkeys(keys))
        pruned_scan_keys_by_task[task_name] = deduped_keys
        active_scan_keys.update(deduped_keys)
    pruned_units = tuple(
        sorted(
            (unit for unit in scan_units if unit.key in active_scan_keys),
            key=lambda unit: unit.key,
        )
    )
    return pruned_units, pruned_scan_keys_by_task


def _scan_keys_from_mapping(
    scan_keys_by_task: Mapping[str, tuple[str, ...]],
) -> tuple[str, ...]:
    keys: list[str] = []
    for task_name in sorted(scan_keys_by_task):
        keys.extend(scan_keys_by_task[task_name])
    return tuple(dict.fromkeys(keys))


def _task_dependency_reduction(
    graph: TaskGraph,
    plan_fingerprints: Mapping[str, str],
) -> TaskDependencyReduction:
    return task_dependency_reduction(graph, task_signatures=plan_fingerprints)


def _plan_signature_and_diagnostics(
    graph: TaskGraph,
    plan_fingerprints: Mapping[str, str],
    *,
    reduction: TaskDependencyReduction,
) -> tuple[str, GraphDiagnostics]:
    snapshot_payload = task_graph_snapshot(
        graph,
        label="execution_plan",
        task_signatures=plan_fingerprints,
    )
    signature = task_graph_signature(snapshot_payload)
    diagnostics = task_graph_diagnostics(graph, include_node_link=True)
    critical_path_task_names = task_dependency_critical_path_tasks(reduction.reduced_graph)
    critical_path_length_weighted = (
        task_dependency_critical_path_length(reduction.reduced_graph)
        if reduction.reduced_graph.num_nodes() > 0
        else None
    )
    enriched = replace(
        diagnostics,
        full_graph_signature=signature,
        reduced_graph_signature=reduction.reduced_signature,
        reduction_node_map=dict(reduction.node_map),
        critical_path_task_names=critical_path_task_names,
        critical_path_length_weighted=critical_path_length_weighted,
        dependency_edge_count=reduction.edge_count,
        reduced_dependency_edge_count=reduction.reduced_edge_count,
        dependency_removed_edge_count=reduction.removed_edge_count,
    )
    return signature, enriched


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


def _output_schema_lookup(
    contracts: Mapping[str, OutputContract],
) -> Callable[[str], SchemaContract | DatasetSpec | ContractSpec | None]:
    def _lookup(name: str) -> SchemaContract | DatasetSpec | ContractSpec | None:
        return cast(
            "SchemaContract | DatasetSpec | ContractSpec | None",
            contracts.get(name),
        )

    return _lookup


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
        task_name, priority = _task_name_priority(node)
        cost = float(max(priority, 1)) if task_name is not None else 1.0
        bottom[node_idx] = cost + best_succ
    out: dict[str, float] = {}
    for node_idx, score in bottom.items():
        node = graph[node_idx]
        task_name, _priority = _task_name_priority(node)
        if task_name is not None:
            out[task_name] = score
    return out


def _task_name_priority(node: object) -> tuple[str | None, int]:
    if isinstance(node, TaskNode):
        return node.name, node.priority
    kind = getattr(node, "kind", None)
    payload = getattr(node, "payload", None)
    if kind != "task":
        return None, 1
    if isinstance(payload, TaskNode):
        return payload.name, payload.priority
    name = getattr(payload, "name", None)
    priority = getattr(payload, "priority", 1)
    if isinstance(name, str) and isinstance(priority, int):
        return name, priority
    return name if isinstance(name, str) else None, 1


def _partition_view_nodes(
    view_nodes: Sequence[ViewNode],
) -> tuple[tuple[ViewNode, ...], set[str]]:
    nodes_with_plan: list[ViewNode] = []
    missing: set[str] = set()
    for node in view_nodes:
        if node.plan_bundle is None:
            missing.add(node.name)
            continue
        nodes_with_plan.append(node)
    return tuple(nodes_with_plan), missing


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
        substrait_bytes = None
        if node.plan_bundle is not None:
            substrait_bytes = node.plan_bundle.substrait_bytes
        snapshots[node.name] = PlanFingerprintSnapshot(
            plan_fingerprint=plan_fingerprints.get(node.name, ""),
            substrait_bytes=substrait_bytes,
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


def _pruned_plan_components(
    *,
    plan: ExecutionPlan,
    active_tasks: set[str],
) -> _PrunedPlanComponents:
    pruned_graph = _prune_task_graph(plan.task_graph, active_tasks=active_tasks)
    pruned_view_nodes = _prune_view_nodes(plan.view_nodes, active_tasks=active_tasks)
    pruned_scan_units, pruned_scan_keys_by_task = _prune_scan_units(
        scan_units=plan.scan_units,
        scan_keys_by_task=plan.scan_keys_by_task,
        active_tasks=active_tasks,
    )
    pruned_fingerprints = {
        name: plan.plan_fingerprints[name]
        for name in sorted(active_tasks)
        if name in plan.plan_fingerprints
    }
    pruned_snapshots = {
        name: plan.plan_snapshots[name]
        for name in sorted(active_tasks)
        if name in plan.plan_snapshots
    }
    pruned_contracts = {
        name: plan.output_contracts[name]
        for name in sorted(active_tasks)
        if name in plan.output_contracts
    }
    pruned_dependency_map = {
        name: tuple(dep for dep in deps if dep in active_tasks)
        for name, deps in plan.dependency_map.items()
        if name in active_tasks
    }
    pruned_requested = tuple(name for name in plan.requested_task_names if name in active_tasks)
    pruned_impacted = tuple(name for name in plan.impacted_task_names if name in active_tasks)
    pruned_diff = _prune_incremental_diff(
        plan.incremental_diff,
        active_tasks=active_tasks,
    )
    return _PrunedPlanComponents(
        task_graph=pruned_graph,
        view_nodes=pruned_view_nodes,
        plan_fingerprints=pruned_fingerprints,
        plan_snapshots=pruned_snapshots,
        output_contracts=pruned_contracts,
        dependency_map=pruned_dependency_map,
        scan_units=pruned_scan_units,
        scan_keys_by_task=pruned_scan_keys_by_task,
        requested_task_names=pruned_requested,
        impacted_task_names=pruned_impacted,
        incremental_diff=pruned_diff,
    )


def prune_execution_plan(
    plan: ExecutionPlan,
    *,
    active_tasks: Iterable[str],
    allow_partial: bool | None = None,
) -> ExecutionPlan:
    """Return a plan pruned to the provided active task set.

    Returns
    -------
    ExecutionPlan
        Pruned plan with recomputed scheduling and diagnostics.

    Raises
    ------
    ValueError
        Raised when pruning resolves to zero active tasks.
    """
    target_active = set(active_tasks) & set(plan.active_tasks)
    if not target_active:
        msg = "Pruned execution plan resolved to zero active tasks."
        raise ValueError(msg)
    if target_active == set(plan.active_tasks):
        return plan
    components = _pruned_plan_components(plan=plan, active_tasks=target_active)
    reduction = _task_dependency_reduction(
        components.task_graph,
        components.plan_fingerprints,
    )
    output_schema_for = _output_schema_lookup(components.output_contracts)
    scan_keys = _scan_keys_from_mapping(components.scan_keys_by_task)
    evidence_for_schedule = plan.evidence.clone()
    for scan_key in scan_keys:
        if scan_key:
            evidence_for_schedule.sources.add(scan_key)
    schedule = schedule_tasks(
        components.task_graph,
        evidence=evidence_for_schedule,
        output_schema_for=output_schema_for,
        allow_partial=plan.allow_partial if allow_partial is None else allow_partial,
        reduced_dependency_graph=reduction.reduced_graph,
    )
    schedule_meta = task_schedule_metadata(schedule)
    signature, diagnostics = _plan_signature_and_diagnostics(
        components.task_graph,
        components.plan_fingerprints,
        reduction=reduction,
    )
    bottom_costs = bottom_level_costs(reduction.reduced_graph)
    scan_delta_pins = _scan_unit_delta_pins(components.scan_units)
    return ExecutionPlan(
        view_nodes=components.view_nodes,
        task_graph=components.task_graph,
        task_dependency_graph=reduction.full_graph,
        reduced_task_dependency_graph=reduction.reduced_graph,
        evidence=plan.evidence,
        task_schedule=schedule,
        schedule_metadata=schedule_meta,
        plan_fingerprints=components.plan_fingerprints,
        plan_snapshots=components.plan_snapshots,
        output_contracts=components.output_contracts,
        plan_signature=signature,
        task_dependency_signature=reduction.full_signature,
        reduced_task_dependency_signature=reduction.reduced_signature,
        reduction_node_map=reduction.node_map,
        reduction_edge_count=reduction.edge_count,
        reduction_removed_edge_count=reduction.removed_edge_count,
        diagnostics=diagnostics,
        critical_path_task_names=diagnostics.critical_path_task_names,
        critical_path_length_weighted=diagnostics.critical_path_length_weighted,
        bottom_level_costs=bottom_costs,
        dependency_map=components.dependency_map,
        dataset_specs=plan.dataset_specs,
        active_tasks=frozenset(target_active),
        scan_units=components.scan_units,
        scan_unit_delta_pins=scan_delta_pins,
        scan_keys_by_task=components.scan_keys_by_task,
        requested_task_names=components.requested_task_names,
        impacted_task_names=components.impacted_task_names,
        allow_partial=plan.allow_partial if allow_partial is None else allow_partial,
        incremental_diff=components.incremental_diff,
        incremental_state_dir=plan.incremental_state_dir,
    )


def _prune_incremental_diff(
    diff: IncrementalDiff | None,
    *,
    active_tasks: set[str],
) -> IncrementalDiff | None:
    if diff is None:
        return None
    changed = tuple(name for name in diff.changed_tasks if name in active_tasks)
    added = tuple(name for name in diff.added_tasks if name in active_tasks)
    removed = tuple(name for name in diff.removed_tasks if name in active_tasks)
    unchanged = tuple(name for name in diff.unchanged_tasks if name in active_tasks)
    semantic = {
        name: diff.semantic_changes[name] for name in changed if name in diff.semantic_changes
    }
    from relspec.incremental import IncrementalDiff as _IncrementalDiff

    return _IncrementalDiff(
        changed_tasks=changed,
        added_tasks=added,
        removed_tasks=removed,
        unchanged_tasks=unchanged,
        semantic_changes=semantic,
    )


__all__ = [
    "ExecutionPlan",
    "ExecutionPlanRequest",
    "bottom_level_costs",
    "compile_execution_plan",
    "dependency_map_from_inferred",
    "downstream_task_closure",
    "priority_for_task",
    "prune_execution_plan",
    "upstream_task_closure",
]
