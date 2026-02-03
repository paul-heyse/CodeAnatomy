"""Canonical execution plan compilation for rustworkx-driven scheduling."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import rustworkx as rx

from datafusion_engine.dataset.registry import dataset_catalog_from_profile
from datafusion_engine.delta.protocol import DeltaProtocolSnapshot
from datafusion_engine.plan.pipeline import plan_with_delta_pins
from relspec.evidence import (
    EvidenceCatalog,
    initial_evidence_from_views,
    known_dataset_specs,
)
from relspec.extract_plan import extract_inferred_deps, extract_task_kind_map
from relspec.inferred_deps import InferredDeps
from relspec.rustworkx_graph import (
    GraphDiagnostics,
    TaskDependencyReduction,
    TaskGraph,
    TaskGraphBuildOptions,
    TaskNode,
    build_task_graph_from_inferred_deps,
    task_dependency_articulation_tasks,
    task_dependency_betweenness_centrality,
    task_dependency_bridge_edges,
    task_dependency_critical_path_length,
    task_dependency_critical_path_tasks,
    task_dependency_immediate_dominators,
    task_dependency_reduction,
    task_graph_diagnostics,
    task_graph_signature,
    task_graph_snapshot,
    task_graph_subgraph,
)
from relspec.rustworkx_schedule import (
    ScheduleCostContext,
    ScheduleOptions,
    TaskSchedule,
    schedule_tasks,
    task_schedule_metadata,
)
from relspec.schedule_events import TaskScheduleMetadata
from relspec.view_defs import (
    DEFAULT_REL_TASK_PRIORITY,
    RELATION_VIEW_NAMES,
    SEMANTIC_INTERMEDIATE_VIEWS,
)
from semantics.incremental.plan_fingerprints import PlanFingerprintSnapshot
from serde_msgspec import to_builtins
from utils.hashing import hash_msgpack_canonical, hash_settings

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.delta.protocol import DeltaProtocolSnapshot
    from datafusion_engine.lineage.datafusion import LineageReport
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.plan.bundle import DataFusionPlanBundle
    from datafusion_engine.schema.contracts import SchemaContract
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
    from datafusion_engine.views.graph import ViewNode
    from schema_spec.system import ContractSpec, DatasetSpec

    OutputContract = ContractSpec | DatasetSpec | object
else:
    OutputContract = object

_RELSPEC_OUTPUTS: frozenset[str] = frozenset(
    {
        *RELATION_VIEW_NAMES,
        *SEMANTIC_INTERMEDIATE_VIEWS,
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
    plan_task_signatures: Mapping[str, str]
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
    slack_by_task: Mapping[str, float]
    task_plan_metrics: Mapping[str, TaskPlanMetrics]
    task_costs: Mapping[str, float]
    dependency_map: Mapping[str, tuple[str, ...]]
    dataset_specs: Mapping[str, DatasetSpec]
    active_tasks: frozenset[str]
    runtime_profile: DataFusionRuntimeProfile | None = None
    session_runtime_hash: str | None = None
    scan_units: tuple[ScanUnit, ...] = ()
    scan_unit_delta_pins: Mapping[str, tuple[int | None, str | None]] = field(default_factory=dict)
    scan_keys_by_task: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    scan_task_units_by_name: Mapping[str, ScanUnit] = field(default_factory=dict)
    scan_task_name_by_key: Mapping[str, str] = field(default_factory=dict)
    scan_task_names_by_task: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    lineage_by_view: Mapping[str, LineageReport] = field(default_factory=dict)
    requested_task_names: tuple[str, ...] = ()
    impacted_task_names: tuple[str, ...] = ()
    allow_partial: bool = False


@dataclass(frozen=True)
class ExecutionPlanRequest:
    """Describe inputs to the execution plan compiler."""

    view_nodes: Sequence[ViewNode]
    snapshot: Mapping[str, object] | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    requested_task_names: Iterable[str] | None = None
    impacted_task_names: Iterable[str] | None = None
    allow_partial: bool = False
    enable_metric_scheduling: bool = True


@dataclass(frozen=True)
class TaskPlanMetrics:
    """Captured planning metrics for a task."""

    duration_ms: float | None = None
    output_rows: int | None = None
    partition_count: int | None = None
    repartition_count: int | None = None
    stats_row_count: int | None = None
    stats_total_bytes: int | None = None
    stats_available: bool = False


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
    runtime_profile: DataFusionRuntimeProfile
    session_runtime: SessionRuntime
    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]
    scan_task_units_by_name: Mapping[str, ScanUnit]
    scan_task_name_by_key: Mapping[str, str]
    scan_task_names_by_task: Mapping[str, tuple[str, ...]]
    lineage_by_view: Mapping[str, LineageReport]
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
    scan_task_units_by_name: Mapping[str, ScanUnit]
    scan_task_name_by_key: Mapping[str, str]
    scan_task_names_by_task: Mapping[str, tuple[str, ...]]
    lineage_by_view: Mapping[str, LineageReport]


@dataclass(frozen=True)
class _PrunedPlanBundle:
    """Pruned graph and node bundle."""

    task_graph: TaskGraph
    active_tasks: set[str]
    view_nodes: tuple[ViewNode, ...]
    inferred: tuple[InferredDeps, ...]
    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]
    scan_task_units_by_name: Mapping[str, ScanUnit]
    scan_task_name_by_key: Mapping[str, str]
    scan_task_names_by_task: Mapping[str, tuple[str, ...]]
    lineage_by_view: Mapping[str, LineageReport]


@dataclass(frozen=True)
class _ContractsEvidence:
    """Contracts and evidence bundle."""

    output_contracts: Mapping[str, OutputContract]
    evidence: EvidenceCatalog


@dataclass(frozen=True)
class _PlanAssembly:
    """Compiled plan pieces used for execution plan assembly."""

    schedule: TaskSchedule
    schedule_metadata: Mapping[str, TaskScheduleMetadata]
    plan_snapshots: Mapping[str, PlanFingerprintSnapshot]
    task_plan_metrics: Mapping[str, TaskPlanMetrics]
    task_costs: Mapping[str, float]
    signature: str
    diagnostics: GraphDiagnostics
    bottom_costs: Mapping[str, float]
    slack_by_task: Mapping[str, float]
    dependency_map: Mapping[str, tuple[str, ...]]
    scan_delta_pins: Mapping[str, tuple[int | None, str | None]]
    session_runtime_hash: str | None


@dataclass(frozen=True)
class _PrunedScanComponents:
    """Scan-unit related plan components after pruning."""

    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]
    scan_task_units_by_name: Mapping[str, ScanUnit]
    scan_task_name_by_key: Mapping[str, str]
    scan_task_names_by_task: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True)
class _PrunedPlanMaps:
    """Plan mappings that are filtered to active tasks."""

    plan_fingerprints: Mapping[str, str]
    plan_task_signatures: Mapping[str, str]
    plan_snapshots: Mapping[str, PlanFingerprintSnapshot]
    output_contracts: Mapping[str, OutputContract]
    dependency_map: Mapping[str, tuple[str, ...]]
    task_plan_metrics: Mapping[str, TaskPlanMetrics]
    task_costs: Mapping[str, float]
    lineage_by_view: Mapping[str, LineageReport]


@dataclass(frozen=True)
class _PrunedPlanComponents:
    """Pruned plan components used for recomputation."""

    task_graph: TaskGraph
    view_nodes: tuple[ViewNode, ...]
    plan_fingerprints: Mapping[str, str]
    plan_task_signatures: Mapping[str, str]
    plan_snapshots: Mapping[str, PlanFingerprintSnapshot]
    output_contracts: Mapping[str, OutputContract]
    dependency_map: Mapping[str, tuple[str, ...]]
    task_plan_metrics: Mapping[str, TaskPlanMetrics]
    task_costs: Mapping[str, float]
    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]
    scan_task_units_by_name: Mapping[str, ScanUnit]
    scan_task_name_by_key: Mapping[str, str]
    scan_task_names_by_task: Mapping[str, tuple[str, ...]]
    lineage_by_view: Mapping[str, LineageReport]
    requested_task_names: tuple[str, ...]
    impacted_task_names: tuple[str, ...]


def priority_for_task(task_name: str) -> int:
    """Return a deterministic scheduling priority for a task name.

    Returns
    -------
    int
        Priority value used by the scheduler.
    """
    if task_name.startswith("scan_unit_"):
        return 10
    if task_name in _RELSPEC_OUTPUTS:
        return DEFAULT_REL_TASK_PRIORITY
    if task_name.startswith("cpg_"):
        return 100
    return 100


def _scan_unit_delta_pins(
    scan_units: Sequence[ScanUnit],
) -> dict[str, tuple[int | None, str | None]]:
    """Extract Delta pins from scan units for determinism tracking.

    Parameters
    ----------
    scan_units
        Sequence of scan units with optional Delta version pins.

    Returns
    -------
    dict[str, tuple[int | None, str | None]]
        Mapping of dataset name to pinned Delta version and timestamp.

    Raises
    ------
    ValueError
        When conflicting Delta versions exist for the same dataset.
    """
    pins: dict[str, tuple[int | None, str | None]] = {}
    for unit in scan_units:
        timestamp = unit.delta_timestamp
        if timestamp is None and unit.snapshot_timestamp is not None:
            timestamp = str(unit.snapshot_timestamp)
        if unit.delta_version is None and timestamp is None:
            continue
        existing = pins.get(unit.dataset_name)
        candidate = (unit.delta_version, timestamp)
        if existing is not None and existing != candidate:
            msg = f"Conflicting Delta pins for {unit.dataset_name!r}: {existing} vs {candidate}"
            raise ValueError(msg)
        pins[unit.dataset_name] = candidate
    return pins


def _assemble_plan_components(
    *,
    context: _PlanBuildContext,
    request: ExecutionPlanRequest,
    plan_fingerprints: Mapping[str, str],
    plan_task_signatures: Mapping[str, str],
    reduction: TaskDependencyReduction,
) -> _PlanAssembly:
    """Assemble plan components for an execution plan.

    Returns
    -------
    _PlanAssembly
        Plan components used to build the final execution plan.
    """
    output_schema_for = _output_schema_lookup(context.output_contracts)
    plan_snapshots = _plan_snapshot_map(
        context.view_nodes,
        plan_fingerprints,
        plan_task_signatures,
    )
    cost_context, plan_metrics, task_costs = _plan_cost_context(
        view_nodes=context.view_nodes,
        scan_units_by_task=context.scan_task_units_by_name,
        reduction=reduction,
        enable_metric_scheduling=request.enable_metric_scheduling,
    )
    schedule = schedule_tasks(
        context.task_graph,
        evidence=context.evidence,
        options=ScheduleOptions(
            output_schema_for=output_schema_for,
            allow_partial=request.allow_partial,
            reduced_dependency_graph=reduction.reduced_graph,
            cost_context=cost_context,
        ),
    )
    schedule_meta = task_schedule_metadata(schedule)
    signature, diagnostics = _plan_signature_and_diagnostics(
        context.task_graph,
        plan_task_signatures,
        reduction=reduction,
        task_costs=task_costs,
    )
    dependency_map = dependency_map_from_inferred(
        context.inferred,
        active_tasks=context.active_tasks,
        scan_task_names_by_task=context.scan_task_names_by_task,
    )
    scan_delta_pins = _scan_unit_delta_pins(context.scan_units)
    session_runtime_hash = _session_runtime_hash(context.session_runtime)
    bottom_costs = cost_context.bottom_level_costs or {}
    slack_by_task = cost_context.slack_by_task or {}
    return _PlanAssembly(
        schedule=schedule,
        schedule_metadata=schedule_meta,
        plan_snapshots=plan_snapshots,
        task_plan_metrics=plan_metrics,
        task_costs=task_costs,
        signature=signature,
        diagnostics=diagnostics,
        bottom_costs=bottom_costs,
        slack_by_task=slack_by_task,
        dependency_map=dependency_map,
        scan_delta_pins=scan_delta_pins,
        session_runtime_hash=session_runtime_hash,
    )


def _plan_cost_context(
    *,
    view_nodes: Sequence[ViewNode],
    scan_units_by_task: Mapping[str, ScanUnit],
    reduction: TaskDependencyReduction,
    enable_metric_scheduling: bool,
) -> tuple[ScheduleCostContext, dict[str, TaskPlanMetrics], dict[str, float]]:
    """Build scheduling cost context and metrics.

    Parameters
    ----------
    view_nodes
        View nodes used to derive task metrics.
    scan_units_by_task
        Mapping of task names to scan units for cost derivation.
    reduction
        Reduced dependency graph used for scheduling analytics.
    enable_metric_scheduling
        Flag indicating whether metric-based scheduling is enabled.

    Returns
    -------
    tuple[ScheduleCostContext, dict[str, TaskPlanMetrics], dict[str, float]]
        Cost context, plan metrics, and task cost mapping.
    """
    if enable_metric_scheduling:
        plan_metrics = _task_plan_metrics(view_nodes)
        task_costs = _task_costs_from_metrics(
            plan_metrics,
            scan_units_by_task=scan_units_by_task,
        )
    else:
        plan_metrics: dict[str, TaskPlanMetrics] = {}
        task_costs: dict[str, float] = {}
    bottom_costs = bottom_level_costs(reduction.reduced_graph, task_costs=task_costs)
    slack_by_task = task_slack_by_task(
        reduction.reduced_graph,
        task_costs=task_costs,
    )
    centrality = task_dependency_betweenness_centrality(reduction.reduced_graph)
    bridge_edges = task_dependency_bridge_edges(reduction.reduced_graph)
    articulation_tasks = task_dependency_articulation_tasks(reduction.reduced_graph)
    bridge_tasks = _bridge_task_names(bridge_edges)
    cost_context = ScheduleCostContext(
        task_costs=task_costs,
        bottom_level_costs=bottom_costs,
        slack_by_task=slack_by_task,
        betweenness_centrality=centrality,
        articulation_tasks=frozenset(articulation_tasks),
        bridge_tasks=bridge_tasks,
    )
    return cost_context, plan_metrics, task_costs


def compile_execution_plan(
    *,
    session_runtime: SessionRuntime,
    request: ExecutionPlanRequest,
) -> ExecutionPlan:
    """Compile the canonical execution plan for the current session.

    Parameters
    ----------
    session_runtime : SessionRuntime
        Active SessionRuntime used for schema discovery and planning.
    request : ExecutionPlanRequest
        Inputs that shape plan compilation and pruning.

    Returns
    -------
    ExecutionPlan
        Fully compiled execution plan for this session.

    """
    context = _prepare_plan_context(session_runtime, request)
    plan_fingerprints = _plan_fingerprint_map(context.inferred)
    plan_task_signatures = _plan_task_signature_map(
        context=context,
        plan_fingerprints=plan_fingerprints,
    )
    reduction = _task_dependency_reduction(context.task_graph, plan_task_signatures)
    assembly = _assemble_plan_components(
        context=context,
        request=request,
        plan_fingerprints=plan_fingerprints,
        plan_task_signatures=plan_task_signatures,
        reduction=reduction,
    )
    return ExecutionPlan(
        view_nodes=context.view_nodes,
        task_graph=context.task_graph,
        task_dependency_graph=reduction.full_graph,
        reduced_task_dependency_graph=reduction.reduced_graph,
        evidence=context.evidence,
        task_schedule=assembly.schedule,
        schedule_metadata=assembly.schedule_metadata,
        plan_fingerprints=plan_fingerprints,
        plan_task_signatures=plan_task_signatures,
        plan_snapshots=assembly.plan_snapshots,
        output_contracts=context.output_contracts,
        plan_signature=assembly.signature,
        task_dependency_signature=reduction.full_signature,
        reduced_task_dependency_signature=reduction.reduced_signature,
        reduction_node_map=reduction.node_map,
        reduction_edge_count=reduction.edge_count,
        reduction_removed_edge_count=reduction.removed_edge_count,
        diagnostics=assembly.diagnostics,
        critical_path_task_names=assembly.diagnostics.critical_path_task_names,
        critical_path_length_weighted=assembly.diagnostics.critical_path_length_weighted,
        bottom_level_costs=assembly.bottom_costs,
        slack_by_task=assembly.slack_by_task,
        task_plan_metrics=assembly.task_plan_metrics,
        task_costs=assembly.task_costs,
        dependency_map=assembly.dependency_map,
        dataset_specs=context.dataset_spec_map,
        active_tasks=frozenset(context.active_tasks),
        runtime_profile=context.runtime_profile,
        session_runtime_hash=assembly.session_runtime_hash,
        scan_units=context.scan_units,
        scan_unit_delta_pins=assembly.scan_delta_pins,
        scan_keys_by_task=context.scan_keys_by_task,
        scan_task_units_by_name=context.scan_task_units_by_name,
        scan_task_name_by_key=context.scan_task_name_by_key,
        scan_task_names_by_task=context.scan_task_names_by_task,
        lineage_by_view=context.lineage_by_view,
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
    pruned_scan_task_units = {
        name: unit for name, unit in inputs.scan_task_units_by_name.items() if name in active_tasks
    }
    pruned_scan_task_name_by_key = {
        key: name
        for key, name in inputs.scan_task_name_by_key.items()
        if name in pruned_scan_task_units
    }
    pruned_scan_task_names_by_task = {
        task: tuple(name for name in names if name in active_tasks)
        for task, names in inputs.scan_task_names_by_task.items()
        if task in active_tasks
    }
    pruned_lineage_by_view = {
        node.name: inputs.lineage_by_view[node.name]
        for node in pruned_nodes
        if node.name in inputs.lineage_by_view
    }
    return _PrunedPlanBundle(
        task_graph=pruned_graph,
        active_tasks=active_tasks,
        view_nodes=pruned_nodes,
        inferred=pruned_inferred,
        scan_units=pruned_scan_units,
        scan_keys_by_task=pruned_scan_keys_by_task,
        scan_task_units_by_name=pruned_scan_task_units,
        scan_task_name_by_key=pruned_scan_task_name_by_key,
        scan_task_names_by_task=pruned_scan_task_names_by_task,
        lineage_by_view=pruned_lineage_by_view,
    )


def _contracts_and_evidence(
    *,
    session: SessionContext,
    pruned: _PrunedPlanBundle,
    dataset_spec_map: Mapping[str, DatasetSpec],
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
    return _ContractsEvidence(output_contracts=output_contracts, evidence=evidence)


def _prepare_plan_context(
    session_runtime: SessionRuntime,
    request: ExecutionPlanRequest,
) -> _PlanBuildContext:
    session = session_runtime.ctx
    requested = set(request.requested_task_names or ())
    nodes_with_ast = _validated_view_nodes(request.view_nodes, requested=requested)
    dataset_spec_map = _dataset_spec_map(session)
    runtime_profile = request.runtime_profile or session_runtime.profile
    if (
        request.runtime_profile is not None
        and request.runtime_profile is not session_runtime.profile
    ):
        msg = "ExecutionPlanRequest runtime_profile must match the SessionRuntime profile."
        raise ValueError(msg)
    planned = plan_with_delta_pins(
        session,
        view_nodes=nodes_with_ast,
        runtime_profile=runtime_profile,
        snapshot=request.snapshot,
    )
    _validate_plan_bundle_compatibility(
        view_nodes=planned.view_nodes,
        inferred=planned.inferred,
        scan_units=planned.scan_units,
        scan_keys_by_task=planned.scan_keys_by_task,
    )
    inferred_all = (*planned.inferred, *extract_inferred_deps())
    priorities = _priority_map(inferred_all)
    graph = build_task_graph_from_inferred_deps(
        inferred_all,
        options=TaskGraphBuildOptions(
            priorities=priorities,
            extra_evidence=tuple(sorted(dataset_spec_map)),
            scan_units_by_evidence_name=planned.scan_units_by_evidence_name,
            scan_task_names_by_task=planned.scan_task_names_by_task,
            task_kinds=extract_task_kind_map(),
        ),
    )
    pruned = _pruned_plan_bundle(
        inputs=_PruneInputs(
            graph=graph,
            nodes_with_ast=planned.view_nodes,
            inferred=inferred_all,
            scan_units=planned.scan_units,
            scan_keys_by_task=planned.scan_keys_by_task,
            scan_task_units_by_name=planned.scan_task_units_by_name,
            scan_task_name_by_key=planned.scan_task_name_by_key,
            scan_task_names_by_task=planned.scan_task_names_by_task,
            lineage_by_view=planned.lineage_by_view,
        ),
        request=request,
        requested=requested,
    )
    if request.runtime_profile is not None:
        from datafusion_engine.plan.artifact_store import (
            PlanArtifactsForViewsRequest,
            persist_plan_artifacts_for_views,
        )

        try:
            persist_plan_artifacts_for_views(
                session,
                runtime_profile,
                request=PlanArtifactsForViewsRequest(
                    view_nodes=pruned.view_nodes,
                    scan_units=pruned.scan_units,
                    scan_keys_by_view=pruned.scan_keys_by_task,
                    lineage_by_view=pruned.lineage_by_view,
                ),
            )
        except (RuntimeError, ValueError, OSError, KeyError, ImportError, TypeError) as exc:
            from datafusion_engine.lineage.diagnostics import record_artifact

            record_artifact(
                runtime_profile,
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
        scan_task_units_by_name=pruned.scan_task_units_by_name,
        scan_task_name_by_key=pruned.scan_task_name_by_key,
        scan_task_names_by_task=pruned.scan_task_names_by_task,
        lineage_by_view=pruned.lineage_by_view,
        output_contracts=contracts_evidence.output_contracts,
        evidence=contracts_evidence.evidence,
        dataset_spec_map=dataset_spec_map,
        runtime_profile=runtime_profile,
        session_runtime=session_runtime,
        requested_task_names=requested_task_names,
        impacted_task_names=impacted_task_names,
        allow_partial=bool(request.allow_partial),
    )


def _dataset_spec_map(session: SessionContext) -> Mapping[str, DatasetSpec]:
    dataset_specs = known_dataset_specs(ctx=session)
    semantic_outputs = _semantic_output_names()
    return {spec.name: spec for spec in dataset_specs if spec.name not in semantic_outputs}


def _semantic_output_names() -> set[str]:
    try:
        from semantics.registry import SEMANTIC_MODEL
    except (ImportError, RuntimeError, TypeError, ValueError):
        return set()
    return {spec.name for spec in SEMANTIC_MODEL.outputs}


def _dataset_location_map(
    profile: DataFusionRuntimeProfile | None,
) -> dict[str, DatasetLocation]:
    catalog = dataset_catalog_from_profile(profile)
    return {name: catalog.get(name) for name in catalog.names()}


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
    from datafusion_engine.lineage.scan import plan_scan_units

    return plan_scan_units(
        session,
        dataset_locations=dataset_locations,
        scans_by_task=scans_by_task,
        runtime_profile=runtime_profile,
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
    plan_task_signatures: Mapping[str, str],
) -> TaskDependencyReduction:
    return task_dependency_reduction(graph, task_signatures=plan_task_signatures)


def _plan_signature_and_diagnostics(
    graph: TaskGraph,
    plan_task_signatures: Mapping[str, str],
    *,
    reduction: TaskDependencyReduction,
    task_costs: Mapping[str, float] | None,
) -> tuple[str, GraphDiagnostics]:
    snapshot_payload = task_graph_snapshot(
        graph,
        label="execution_plan",
        task_signatures=plan_task_signatures,
    )
    signature = task_graph_signature(snapshot_payload)
    diagnostics = task_graph_diagnostics(graph, include_node_link=True)
    critical_path_task_names = task_dependency_critical_path_tasks(
        reduction.reduced_graph,
        task_costs=task_costs,
    )
    critical_path_length_weighted = (
        task_dependency_critical_path_length(
            reduction.reduced_graph,
            task_costs=task_costs,
        )
        if reduction.reduced_graph.num_nodes() > 0
        else None
    )
    dominators = task_dependency_immediate_dominators(reduction.reduced_graph)
    centrality = task_dependency_betweenness_centrality(reduction.reduced_graph)
    bridge_edges = task_dependency_bridge_edges(reduction.reduced_graph)
    articulation_tasks = task_dependency_articulation_tasks(reduction.reduced_graph)
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
        dominators=dominators,
        betweenness_centrality=centrality,
        bridge_edges=bridge_edges,
        articulation_tasks=articulation_tasks,
    )
    return signature, enriched


def _bridge_task_names(
    bridge_edges: Sequence[tuple[str, str]],
) -> frozenset[str]:
    tasks: set[str] = set()
    for left, right in bridge_edges:
        tasks.add(left)
        tasks.add(right)
    return frozenset(tasks)


def dependency_map_from_inferred(
    inferred: Sequence[InferredDeps],
    *,
    active_tasks: Iterable[str],
    scan_task_names_by_task: Mapping[str, tuple[str, ...]],
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
        inputs_from_outputs = {name for name in dep.inputs if name in outputs}
        scan_dependencies = set(scan_task_names_by_task.get(dep.task_name, ()))
        combined = tuple(sorted(inputs_from_outputs | scan_dependencies))
        dependency_map[dep.output] = combined
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


def bottom_level_costs(
    graph: rx.PyDiGraph, *, task_costs: Mapping[str, float] | None = None
) -> dict[str, float]:
    """Compute bottom-level costs to prioritize critical-path tasks.

    Returns
    -------
    dict[str, float]
        Mapping of task name to critical-path priority score.
    """
    topo = list(rx.topological_sort(graph))
    bottom: dict[int, float] = {}
    costs = dict(task_costs or {})
    for node_idx in reversed(topo):
        succs = graph.successor_indices(node_idx)
        best_succ = max((bottom[s] for s in succs), default=0.0)
        node = graph[node_idx]
        task_name, priority = _task_name_priority(node)
        if task_name is not None and task_name in costs:
            cost = float(max(costs[task_name], 1.0))
        else:
            cost = float(max(priority, 1)) if task_name is not None else 1.0
        bottom[node_idx] = cost + best_succ
    out: dict[str, float] = {}
    for node_idx, score in bottom.items():
        node = graph[node_idx]
        task_name, _priority = _task_name_priority(node)
        if task_name is not None:
            out[task_name] = score
    return out


def task_slack_by_task(
    graph: rx.PyDiGraph,
    *,
    task_costs: Mapping[str, float] | None = None,
) -> dict[str, float]:
    """Return per-task slack values based on weighted scheduling costs.

    Returns
    -------
    dict[str, float]
        Mapping of task name to slack (latest start - earliest start).
    """
    topo = list(rx.topological_sort(graph))
    if not topo:
        return {}
    node_costs = _node_costs_from_topo(graph, topo, task_costs=task_costs)
    earliest_start, earliest_finish = _earliest_times(graph, topo, node_costs=node_costs)
    critical_length = max(earliest_finish.values(), default=0.0)
    latest_start = _latest_start_times(
        graph,
        topo,
        node_costs=node_costs,
        critical_length=critical_length,
    )
    return _slack_by_task_mapping(
        graph,
        earliest_start=earliest_start,
        latest_start=latest_start,
    )


def _node_costs_from_topo(
    graph: rx.PyDiGraph,
    topo: Sequence[int],
    *,
    task_costs: Mapping[str, float] | None,
) -> dict[int, float]:
    costs = dict(task_costs or {})
    node_costs: dict[int, float] = {}
    for node_idx in topo:
        node = graph[node_idx]
        task_name, priority = _task_name_priority(node)
        if task_name is not None and task_name in costs:
            node_costs[node_idx] = float(max(costs[task_name], 1.0))
        else:
            node_costs[node_idx] = float(max(priority, 1))
    return node_costs


def _earliest_times(
    graph: rx.PyDiGraph,
    topo: Sequence[int],
    *,
    node_costs: Mapping[int, float],
) -> tuple[dict[int, float], dict[int, float]]:
    earliest_start: dict[int, float] = {}
    earliest_finish: dict[int, float] = {}
    for node_idx in topo:
        preds = graph.predecessor_indices(node_idx)
        start = max((earliest_finish[pred] for pred in preds), default=0.0)
        earliest_start[node_idx] = start
        earliest_finish[node_idx] = start + node_costs[node_idx]
    return earliest_start, earliest_finish


def _latest_start_times(
    graph: rx.PyDiGraph,
    topo: Sequence[int],
    *,
    node_costs: Mapping[int, float],
    critical_length: float,
) -> dict[int, float]:
    latest_start: dict[int, float] = {}
    for node_idx in reversed(topo):
        succs = graph.successor_indices(node_idx)
        finish = (
            min((latest_start[succ] for succ in succs), default=critical_length)
            if succs
            else critical_length
        )
        latest_start[node_idx] = finish - node_costs[node_idx]
    return latest_start


def _slack_by_task_mapping(
    graph: rx.PyDiGraph,
    *,
    earliest_start: Mapping[int, float],
    latest_start: Mapping[int, float],
) -> dict[str, float]:
    slack_by_task: dict[str, float] = {}
    for node_idx, start in earliest_start.items():
        task_name, _priority = _task_name_priority(graph[node_idx])
        if task_name is None:
            continue
        slack_value = latest_start.get(node_idx, start) - start
        slack_by_task[task_name] = float(max(slack_value, 0.0))
    return slack_by_task


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


_PLAN_TASK_SIGNATURE_VERSION = 1
_SCAN_UNIT_SIGNATURE_VERSION = 1


def _session_runtime_hash(runtime: SessionRuntime | None) -> str | None:
    if runtime is None:
        return None
    from datafusion_engine.session.runtime import session_runtime_hash

    return session_runtime_hash(runtime)


DeltaInputPayload = tuple[
    str,
    int | None,
    str | None,
    tuple[tuple[str, object], ...] | None,
]


def _delta_inputs_payload(
    bundle: object,
) -> tuple[
    DeltaInputPayload,
    ...,
]:
    delta_inputs = getattr(bundle, "delta_inputs", ())
    payload: list[DeltaInputPayload] = []
    for item in delta_inputs:
        dataset_name = getattr(item, "dataset_name", None)
        version = getattr(item, "version", None)
        timestamp = getattr(item, "timestamp", None)
        protocol = getattr(item, "protocol", None)
        if not isinstance(dataset_name, str) or not dataset_name:
            continue
        version_value = int(version) if isinstance(version, int) else None
        if isinstance(timestamp, str):
            timestamp_value = timestamp
        elif isinstance(timestamp, (int, float)):
            timestamp_value = str(int(timestamp))
        else:
            timestamp_value = None
        payload.append(
            (
                dataset_name,
                version_value,
                timestamp_value,
                _protocol_payload(protocol),
            )
        )
    return tuple(sorted(payload, key=lambda entry: entry[0]))


def _scan_unit_signature(scan_unit: ScanUnit, *, runtime_hash: str | None) -> str:
    candidate_files = tuple(sorted(str(path) for path in scan_unit.candidate_files))
    payload = (
        ("version", _SCAN_UNIT_SIGNATURE_VERSION),
        ("runtime_hash", runtime_hash or ""),
        ("scan_key", scan_unit.key),
        ("dataset_name", scan_unit.dataset_name),
        ("delta_version", scan_unit.delta_version),
        ("delta_timestamp", scan_unit.delta_timestamp),
        ("snapshot_timestamp", scan_unit.snapshot_timestamp),
        ("delta_protocol", _protocol_payload(scan_unit.delta_protocol)),
        ("total_files", scan_unit.total_files),
        ("candidate_file_count", scan_unit.candidate_file_count),
        ("pruned_file_count", scan_unit.pruned_file_count),
        ("candidate_files", candidate_files),
        ("pushed_filters", tuple(sorted(scan_unit.pushed_filters))),
        ("projected_columns", tuple(sorted(scan_unit.projected_columns))),
    )
    return hash_msgpack_canonical(payload)


def _protocol_payload(
    protocol: DeltaProtocolSnapshot | Mapping[str, object] | None,
) -> tuple[tuple[str, object], ...] | None:
    if protocol is None:
        return None
    payload = protocol if isinstance(protocol, Mapping) else to_builtins(protocol, str_keys=True)
    if not isinstance(payload, Mapping):
        return None
    normalized = cast("Mapping[str, object]", payload)
    items: list[tuple[str, object]] = []
    for key, value in normalized.items():
        if isinstance(value, (str, int, float)) or value is None:
            items.append((str(key), value))
            continue
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            items.append((str(key), tuple(str(item) for item in value)))
            continue
        items.append((str(key), str(value)))
    return tuple(sorted(items, key=lambda item: item[0]))


def _scan_unit_delta_inputs_payload(
    scan_units: Sequence[ScanUnit],
) -> tuple[DeltaInputPayload, ...]:
    payload: list[DeltaInputPayload] = []
    for unit in scan_units:
        timestamp = unit.delta_timestamp
        if timestamp is None and unit.snapshot_timestamp is not None:
            timestamp = str(unit.snapshot_timestamp)
        if unit.delta_version is None and timestamp is None:
            continue
        payload.append(
            (
                unit.dataset_name,
                unit.delta_version,
                timestamp,
                _protocol_payload(unit.delta_protocol),
            )
        )
    return tuple(sorted(payload, key=lambda entry: entry[0]))


def _validate_plan_bundle_compatibility(
    *,
    view_nodes: Sequence[ViewNode],
    inferred: Sequence[InferredDeps],
    scan_units: Sequence[ScanUnit],
    scan_keys_by_task: Mapping[str, tuple[str, ...]],
) -> None:
    inferred_by_task = {item.task_name: item for item in inferred}
    scan_units_by_key = {unit.key: unit for unit in scan_units}
    for node in view_nodes:
        bundle = node.plan_bundle
        if bundle is None:
            continue
        deps = inferred_by_task.get(node.name)
        if deps is None:
            continue
        _validate_bundle_udfs(node.name, bundle=bundle, inferred=deps)
        scan_keys = scan_keys_by_task.get(node.name, ())
        if not scan_keys:
            continue
        task_units = [scan_units_by_key[key] for key in scan_keys if key in scan_units_by_key]
        _validate_bundle_delta_inputs(node.name, bundle=bundle, scan_units=task_units)


def _validate_bundle_udfs(
    task_name: str,
    *,
    bundle: DataFusionPlanBundle,
    inferred: InferredDeps,
) -> None:
    from datafusion_engine.udf.runtime import udf_names_from_snapshot
    from datafusion_engine.views.bundle_extraction import (
        extract_lineage_from_bundle,
        resolve_required_udfs_from_bundle,
    )

    bundle_snapshot = getattr(getattr(bundle, "artifacts", None), "udf_snapshot", None)
    snapshot = bundle_snapshot if isinstance(bundle_snapshot, Mapping) else {}
    bundle_udfs = resolve_required_udfs_from_bundle(bundle, snapshot=snapshot)
    snapshot_names = udf_names_from_snapshot(snapshot)
    lookup = {name.lower(): name for name in snapshot_names}
    inferred_udfs = tuple(
        sorted(
            lookup[name.lower()]
            for name in inferred.required_udfs
            if isinstance(name, str) and name.lower() in lookup
        )
    )
    bundle_tags = tuple(sorted(getattr(bundle, "required_rewrite_tags", ())))
    if not bundle_tags:
        lineage = extract_lineage_from_bundle(bundle)
        bundle_tags = tuple(sorted(lineage.required_rewrite_tags))
    inferred_tags = tuple(sorted(inferred.required_rewrite_tags))
    if bundle_udfs != inferred_udfs:
        msg = (
            f"UDF requirements mismatch for {task_name!r}: "
            f"bundle={bundle_udfs} inferred={inferred_udfs}"
        )
        raise ValueError(msg)
    if bundle_tags != inferred_tags:
        msg = (
            f"Rewrite tag requirements mismatch for {task_name!r}: "
            f"bundle={bundle_tags} inferred={inferred_tags}"
        )
        raise ValueError(msg)


def _validate_bundle_delta_inputs(
    task_name: str,
    *,
    bundle: object,
    scan_units: Sequence[ScanUnit],
) -> None:
    expected = _scan_unit_delta_inputs_payload(scan_units)
    if not expected:
        return
    expected_by_dataset = {entry[0]: entry for entry in expected}
    actual = _delta_inputs_payload(bundle)
    actual_by_dataset = {entry[0]: entry for entry in actual}
    mismatches: dict[str, tuple[DeltaInputPayload, DeltaInputPayload | None]] = {}
    for dataset_name, expected_entry in expected_by_dataset.items():
        actual_entry = actual_by_dataset.get(dataset_name)
        if actual_entry != expected_entry:
            mismatches[dataset_name] = (expected_entry, actual_entry)
    if mismatches:
        msg = f"Delta input pins do not match scan units for {task_name!r}: {mismatches}"
        raise ValueError(msg)


def _plan_bundle_task_signature(
    *,
    bundle: object,
    runtime_hash: str | None,
    scan_signatures: Sequence[str],
) -> str:
    artifacts = getattr(bundle, "artifacts", None)
    if artifacts is None:
        return ""
    plan_fingerprint = getattr(bundle, "plan_fingerprint", "")
    function_registry_hash = getattr(artifacts, "function_registry_hash", "")
    udf_snapshot_hash = getattr(artifacts, "udf_snapshot_hash", "")
    rewrite_tags = tuple(sorted(getattr(artifacts, "rewrite_tags", ())))
    domain_planner_names = tuple(sorted(getattr(artifacts, "domain_planner_names", ())))
    df_settings = getattr(artifacts, "df_settings", {})
    df_settings_hash = hash_settings(df_settings) if isinstance(df_settings, Mapping) else ""
    info_schema_hash = getattr(artifacts, "information_schema_hash", "")
    required_udfs = tuple(sorted(getattr(bundle, "required_udfs", ())))
    required_rewrite_tags = tuple(sorted(getattr(bundle, "required_rewrite_tags", ())))
    delta_inputs_payload = _delta_inputs_payload(bundle)
    payload = (
        ("version", _PLAN_TASK_SIGNATURE_VERSION),
        ("runtime_hash", runtime_hash or ""),
        ("plan_fingerprint", str(plan_fingerprint)),
        ("function_registry_hash", str(function_registry_hash)),
        ("udf_snapshot_hash", str(udf_snapshot_hash)),
        ("information_schema_hash", str(info_schema_hash)),
        ("rewrite_tags", rewrite_tags),
        ("domain_planner_names", domain_planner_names),
        ("df_settings_hash", df_settings_hash),
        ("required_udfs", required_udfs),
        ("required_rewrite_tags", required_rewrite_tags),
        ("delta_inputs", delta_inputs_payload),
        ("scan_signatures", tuple(sorted(scan_signatures))),
    )
    return hash_msgpack_canonical(payload)


def _plan_task_signature_map(
    *,
    context: _PlanBuildContext,
    plan_fingerprints: Mapping[str, str],
) -> dict[str, str]:
    runtime_hash = _session_runtime_hash(context.session_runtime)
    scan_units_by_key = {unit.key: unit for unit in context.scan_units}
    scan_signatures_by_key = {
        key: _scan_unit_signature(unit, runtime_hash=runtime_hash)
        for key, unit in scan_units_by_key.items()
    }
    view_nodes_by_name = {node.name: node for node in context.view_nodes}
    signatures: dict[str, str] = {}
    for dep in context.inferred:
        task_name = dep.task_name
        if task_name in context.scan_task_units_by_name:
            scan_unit = context.scan_task_units_by_name[task_name]
            signatures[task_name] = _scan_unit_signature(scan_unit, runtime_hash=runtime_hash)
            continue
        view_node = view_nodes_by_name.get(task_name)
        if view_node is not None and view_node.plan_bundle is not None:
            scan_keys = context.scan_keys_by_task.get(task_name, ())
            scan_signatures = [
                scan_signatures_by_key[key] for key in scan_keys if key in scan_signatures_by_key
            ]
            bundle_signature = _plan_bundle_task_signature(
                bundle=view_node.plan_bundle,
                runtime_hash=runtime_hash,
                scan_signatures=scan_signatures,
            )
            if bundle_signature:
                signatures[task_name] = bundle_signature
                continue
        signatures[task_name] = plan_fingerprints.get(task_name, "")
    return signatures


def _plan_snapshot_map(
    view_nodes: Sequence[ViewNode],
    plan_fingerprints: Mapping[str, str],
    plan_task_signatures: Mapping[str, str],
) -> dict[str, PlanFingerprintSnapshot]:
    snapshots: dict[str, PlanFingerprintSnapshot] = {}
    for node in view_nodes:
        snapshots[node.name] = PlanFingerprintSnapshot(
            plan_fingerprint=plan_fingerprints.get(node.name, ""),
            plan_task_signature=plan_task_signatures.get(node.name, ""),
        )
    return snapshots


def _task_plan_metrics(view_nodes: Sequence[ViewNode]) -> dict[str, TaskPlanMetrics]:
    metrics: dict[str, TaskPlanMetrics] = {}
    for node in view_nodes:
        bundle = node.plan_bundle
        if bundle is None:
            continue
        details = getattr(bundle, "plan_details", {})
        if not isinstance(details, Mapping):
            continue
        duration_ms = _float_from_value(details.get("explain_analyze_duration_ms"))
        output_rows = _int_from_value(details.get("explain_analyze_output_rows"))
        partition_count = _int_from_value(details.get("partition_count"))
        repartition_count = _int_from_value(details.get("repartition_count"))
        stats_row_count, stats_total_bytes, stats_available = _stats_from_payload(
            details.get("statistics")
        )
        metrics[node.name] = TaskPlanMetrics(
            duration_ms=duration_ms,
            output_rows=output_rows,
            partition_count=partition_count,
            repartition_count=repartition_count,
            stats_row_count=stats_row_count,
            stats_total_bytes=stats_total_bytes,
            stats_available=stats_available,
        )
    return metrics


def _task_costs_from_metrics(
    metrics: Mapping[str, TaskPlanMetrics],
    *,
    scan_units_by_task: Mapping[str, ScanUnit],
) -> dict[str, float]:
    costs: dict[str, float] = {}
    for name, metric in metrics.items():
        base_cost = _base_cost_from_metrics(metric)
        if base_cost is not None:
            costs[name] = _apply_physical_adjustments(base_cost, metric)
    for name, unit in scan_units_by_task.items():
        if name in costs:
            continue
        candidate = unit.candidate_file_count
        total_files = unit.total_files
        file_cost = candidate if candidate > 0 else total_files
        costs[name] = float(max(file_cost, 1))
    return costs


def _base_cost_from_metrics(metric: TaskPlanMetrics) -> float | None:
    """Return the base cost derived from EXPLAIN ANALYZE metrics.

    Parameters
    ----------
    metric
        Task plan metrics captured from DataFusion explains.

    Returns
    -------
    float | None
        Base cost derived from duration, output rows, or partition count.
    """
    base_cost: float | None = None
    if metric.duration_ms is not None:
        base_cost = max(metric.duration_ms, 1.0)
    elif metric.output_rows is not None:
        base_cost = float(max(metric.output_rows, 1))
    elif metric.stats_row_count is not None:
        base_cost = float(max(metric.stats_row_count, 1))
    elif metric.stats_total_bytes is not None:
        base_cost = max(metric.stats_total_bytes / 1048576.0, 1.0)
    elif metric.partition_count is not None:
        base_cost = float(max(metric.partition_count, 1))
    elif metric.repartition_count is not None:
        base_cost = float(max(metric.repartition_count, 1))
    return base_cost


def _apply_physical_adjustments(base_cost: float, metric: TaskPlanMetrics) -> float:
    """Adjust the cost based on physical plan signals.

    Parameters
    ----------
    base_cost
        Base cost derived from plan metrics.
    metric
        Task plan metrics captured from DataFusion explains.

    Returns
    -------
    float
        Adjusted cost incorporating partition and repartition signals.
    """
    adjusted = float(base_cost)
    stats_cost = _stats_cost_from_metrics(metric)
    if metric.stats_available:
        if stats_cost is not None:
            adjusted = max(adjusted, stats_cost)
        else:
            adjusted *= 1.05
    if metric.partition_count is not None:
        adjusted += float(max(metric.partition_count, 1))
    if metric.repartition_count is not None:
        adjusted += float(max(metric.repartition_count, 0)) * 10.0
    return adjusted


def _stats_cost_from_metrics(metric: TaskPlanMetrics) -> float | None:
    if metric.stats_row_count is not None:
        return float(max(metric.stats_row_count, 1))
    if metric.stats_total_bytes is not None:
        return max(metric.stats_total_bytes / 1048576.0, 1.0)
    return None


def _stats_from_payload(
    payload: object,
) -> tuple[int | None, int | None, bool]:
    if not isinstance(payload, Mapping):
        return None, None, False
    row_value = payload.get("num_rows") or payload.get("row_count")
    byte_value = payload.get("total_byte_size") or payload.get("total_bytes")
    row_count = _int_from_value(row_value)
    total_bytes = _int_from_value(byte_value)
    stats_available = row_count is not None or total_bytes is not None
    column_present = payload.get("column_statistics_present")
    if isinstance(column_present, bool):
        stats_available = stats_available or column_present
    if not stats_available:
        column_stats = payload.get("column_statistics")
        if (isinstance(column_stats, Mapping) and column_stats) or (
            isinstance(column_stats, Sequence)
            and not isinstance(column_stats, (str, bytes, bytearray))
            and column_stats
        ):
            stats_available = True
    return row_count, total_bytes, stats_available


def _float_from_value(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _int_from_value(value: object) -> int | None:
    result: int | None = None
    if value is None or isinstance(value, bool):
        result = None
    elif isinstance(value, int):
        result = value
    elif isinstance(value, float):
        result = int(value)
    elif isinstance(value, str):
        try:
            result = int(value)
        except ValueError:
            result = None
    return result


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


def _pruned_scan_components(
    *,
    plan: ExecutionPlan,
    active_tasks: set[str],
) -> _PrunedScanComponents:
    """Prune scan-unit related components to active tasks.

    Returns
    -------
    _PrunedScanComponents
        Pruned scan-unit components for active tasks.
    """
    pruned_scan_units, pruned_scan_keys_by_task = _prune_scan_units(
        scan_units=plan.scan_units,
        scan_keys_by_task=plan.scan_keys_by_task,
        active_tasks=active_tasks,
    )
    pruned_scan_task_units = {
        name: unit for name, unit in plan.scan_task_units_by_name.items() if name in active_tasks
    }
    pruned_scan_task_name_by_key = {
        key: name
        for key, name in plan.scan_task_name_by_key.items()
        if name in pruned_scan_task_units
    }
    pruned_scan_task_names_by_task = {
        task: tuple(name for name in names if name in active_tasks)
        for task, names in plan.scan_task_names_by_task.items()
        if task in active_tasks
    }
    return _PrunedScanComponents(
        scan_units=pruned_scan_units,
        scan_keys_by_task=pruned_scan_keys_by_task,
        scan_task_units_by_name=pruned_scan_task_units,
        scan_task_name_by_key=pruned_scan_task_name_by_key,
        scan_task_names_by_task=pruned_scan_task_names_by_task,
    )


def _pruned_plan_maps(
    *,
    plan: ExecutionPlan,
    active_tasks: set[str],
    pruned_view_nodes: tuple[ViewNode, ...],
) -> _PrunedPlanMaps:
    """Prune plan mappings to active tasks.

    Returns
    -------
    _PrunedPlanMaps
        Pruned plan mappings for active tasks.
    """
    pruned_lineage_by_view = {
        node.name: plan.lineage_by_view[node.name]
        for node in pruned_view_nodes
        if node.name in plan.lineage_by_view
    }
    pruned_fingerprints = {
        name: plan.plan_fingerprints[name]
        for name in sorted(active_tasks)
        if name in plan.plan_fingerprints
    }
    pruned_task_signatures = {
        name: plan.plan_task_signatures[name]
        for name in sorted(active_tasks)
        if name in plan.plan_task_signatures
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
    pruned_task_metrics = {
        name: plan.task_plan_metrics[name]
        for name in sorted(active_tasks)
        if name in plan.task_plan_metrics
    }
    pruned_task_costs = {
        name: plan.task_costs[name] for name in sorted(active_tasks) if name in plan.task_costs
    }
    return _PrunedPlanMaps(
        plan_fingerprints=pruned_fingerprints,
        plan_task_signatures=pruned_task_signatures,
        plan_snapshots=pruned_snapshots,
        output_contracts=pruned_contracts,
        dependency_map=pruned_dependency_map,
        task_plan_metrics=pruned_task_metrics,
        task_costs=pruned_task_costs,
        lineage_by_view=pruned_lineage_by_view,
    )


def _pruned_plan_components(
    *,
    plan: ExecutionPlan,
    active_tasks: set[str],
) -> _PrunedPlanComponents:
    pruned_graph = _prune_task_graph(plan.task_graph, active_tasks=active_tasks)
    pruned_view_nodes = _prune_view_nodes(plan.view_nodes, active_tasks=active_tasks)
    pruned_scan = _pruned_scan_components(plan=plan, active_tasks=active_tasks)
    pruned_maps = _pruned_plan_maps(
        plan=plan,
        active_tasks=active_tasks,
        pruned_view_nodes=pruned_view_nodes,
    )
    return _PrunedPlanComponents(
        task_graph=pruned_graph,
        view_nodes=pruned_view_nodes,
        plan_fingerprints=pruned_maps.plan_fingerprints,
        plan_task_signatures=pruned_maps.plan_task_signatures,
        plan_snapshots=pruned_maps.plan_snapshots,
        output_contracts=pruned_maps.output_contracts,
        dependency_map=pruned_maps.dependency_map,
        task_plan_metrics=pruned_maps.task_plan_metrics,
        task_costs=pruned_maps.task_costs,
        scan_units=pruned_scan.scan_units,
        scan_keys_by_task=pruned_scan.scan_keys_by_task,
        scan_task_units_by_name=pruned_scan.scan_task_units_by_name,
        scan_task_name_by_key=pruned_scan.scan_task_name_by_key,
        scan_task_names_by_task=pruned_scan.scan_task_names_by_task,
        lineage_by_view=pruned_maps.lineage_by_view,
        requested_task_names=tuple(
            name for name in plan.requested_task_names if name in active_tasks
        ),
        impacted_task_names=tuple(
            name for name in plan.impacted_task_names if name in active_tasks
        ),
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
        components.plan_task_signatures,
    )
    output_schema_for = _output_schema_lookup(components.output_contracts)
    evidence_for_schedule = plan.evidence.clone()
    bottom_costs = bottom_level_costs(reduction.reduced_graph, task_costs=components.task_costs)
    slack_by_task = task_slack_by_task(
        reduction.reduced_graph,
        task_costs=components.task_costs,
    )
    schedule = schedule_tasks(
        components.task_graph,
        evidence=evidence_for_schedule,
        options=ScheduleOptions(
            output_schema_for=output_schema_for,
            allow_partial=plan.allow_partial if allow_partial is None else allow_partial,
            reduced_dependency_graph=reduction.reduced_graph,
            cost_context=ScheduleCostContext(
                task_costs=components.task_costs,
                bottom_level_costs=bottom_costs,
                slack_by_task=slack_by_task,
            ),
        ),
    )
    schedule_meta = task_schedule_metadata(schedule)
    signature, diagnostics = _plan_signature_and_diagnostics(
        components.task_graph,
        components.plan_task_signatures,
        reduction=reduction,
        task_costs=components.task_costs,
    )
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
        plan_task_signatures=components.plan_task_signatures,
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
        slack_by_task=slack_by_task,
        task_plan_metrics=components.task_plan_metrics,
        task_costs=components.task_costs,
        dependency_map=components.dependency_map,
        dataset_specs=plan.dataset_specs,
        active_tasks=frozenset(target_active),
        runtime_profile=plan.runtime_profile,
        session_runtime_hash=plan.session_runtime_hash,
        scan_units=components.scan_units,
        scan_unit_delta_pins=scan_delta_pins,
        scan_keys_by_task=components.scan_keys_by_task,
        scan_task_units_by_name=components.scan_task_units_by_name,
        scan_task_name_by_key=components.scan_task_name_by_key,
        scan_task_names_by_task=components.scan_task_names_by_task,
        lineage_by_view=components.lineage_by_view,
        requested_task_names=components.requested_task_names,
        impacted_task_names=components.impacted_task_names,
        allow_partial=plan.allow_partial if allow_partial is None else allow_partial,
    )


__all__ = [
    "ExecutionPlan",
    "ExecutionPlanRequest",
    "TaskPlanMetrics",
    "bottom_level_costs",
    "compile_execution_plan",
    "dependency_map_from_inferred",
    "downstream_task_closure",
    "priority_for_task",
    "prune_execution_plan",
    "task_slack_by_task",
    "upstream_task_closure",
]
