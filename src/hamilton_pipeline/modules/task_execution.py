"""Hamilton nodes for executing tasks from inferred plans."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
from hamilton.function_modifiers import pipe_input, source, step, tag

from core_types import JsonDict
from datafusion_engine.arrow_interop import TableLike as ArrowTableLike
from datafusion_engine.arrow_schema.abi import schema_fingerprint
from datafusion_engine.arrow_schema.build import empty_table
from datafusion_engine.execution_facade import ExecutionResult
from datafusion_engine.finalize import Contract, FinalizeOptions, normalize_only
from datafusion_engine.view_registry import ensure_view_graph
from engine.runtime_profile import RuntimeProfileSpec
from relspec.evidence import EvidenceCatalog
from relspec.runtime_artifacts import ExecutionArtifactSpec, RuntimeArtifacts, TableLike

if TYPE_CHECKING:
    from datafusion_engine.plan_bundle import DataFusionPlanBundle
    from datafusion_engine.runtime import DataFusionRuntimeProfile, SessionRuntime
    from datafusion_engine.scan_planner import ScanUnit
    from engine.session import EngineSession
else:
    DataFusionPlanBundle = object
    DataFusionRuntimeProfile = object
    SessionRuntime = object
    ScanUnit = object
    EngineSession = object


@dataclass(frozen=True)
class TaskExecutionInputs:
    """Shared inputs for task execution."""

    runtime: RuntimeArtifacts
    evidence: EvidenceCatalog
    plan_signature: str
    active_task_names: frozenset[str]
    plan_bundles_by_task: Mapping[str, DataFusionPlanBundle]
    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]
    scan_units_by_task_name: Mapping[str, ScanUnit]
    scan_units_hash: str | None


@dataclass(frozen=True)
class PlanScanInputs:
    """Scan-unit context for deterministic execution."""

    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]
    scan_units_by_task_name: Mapping[str, ScanUnit]
    scan_units_hash: str | None


@dataclass(frozen=True)
class PlanExecutionContext:
    """Bundle execution plan context for task execution."""

    plan_signature: str
    active_task_names: frozenset[str]
    plan_bundles_by_task: Mapping[str, DataFusionPlanBundle]
    plan_scan_inputs: PlanScanInputs


@dataclass(frozen=True)
class TaskExecutionSpec:
    """Describe a task execution request."""

    task_name: str
    task_output: str
    plan_fingerprint: str
    plan_task_signature: str
    task_kind: Literal["view", "scan"]
    scan_unit_key: str | None = None


def _finalize_cpg_table(
    table: TableLike,
    *,
    name: str,
    runtime_profile_spec: RuntimeProfileSpec,
) -> TableLike:
    schema_names = getattr(table.schema, "names", [])
    if not schema_names:
        return empty_table(table.schema)
    contract = Contract(name=name, schema=table.schema)
    return normalize_only(
        cast("ArrowTableLike", table),
        contract=contract,
        options=FinalizeOptions(
            runtime_profile=runtime_profile_spec.datafusion,
            determinism_tier=runtime_profile_spec.determinism_tier,
        ),
    )


@tag(layer="execution", artifact="cpg_nodes_finalize", kind="stage")
def _finalize_cpg_nodes_stage(
    table: TableLike,
    *,
    runtime_profile_spec: RuntimeProfileSpec,
) -> TableLike:
    return _finalize_cpg_table(
        table,
        name="cpg_nodes_v1",
        runtime_profile_spec=runtime_profile_spec,
    )


@tag(layer="execution", artifact="cpg_edges_finalize", kind="stage")
def _finalize_cpg_edges_stage(
    table: TableLike,
    *,
    runtime_profile_spec: RuntimeProfileSpec,
) -> TableLike:
    return _finalize_cpg_table(
        table,
        name="cpg_edges_v1",
        runtime_profile_spec=runtime_profile_spec,
    )


@tag(layer="execution", artifact="cpg_props_finalize", kind="stage")
def _finalize_cpg_props_stage(
    table: TableLike,
    *,
    runtime_profile_spec: RuntimeProfileSpec,
) -> TableLike:
    return _finalize_cpg_table(
        table,
        name="cpg_props_v1",
        runtime_profile_spec=runtime_profile_spec,
    )


def _record_output(
    *,
    inputs: TaskExecutionInputs,
    spec: TaskExecutionSpec,
    plan_signature: str,
    table: TableLike,
) -> None:
    evidence = inputs.evidence
    runtime = inputs.runtime
    schema = table.schema
    evidence.register_schema(spec.task_output, schema)
    if spec.task_output in runtime.execution_artifacts:
        return
    runtime.register_execution(
        spec.task_output,
        ExecutionResult.from_table(cast("ArrowTableLike", table)),
        spec=ExecutionArtifactSpec(
            source_task=spec.task_name,
            schema_fingerprint=schema_fingerprint(schema),
            plan_fingerprint=spec.plan_fingerprint,
            plan_task_signature=spec.plan_task_signature,
            plan_signature=plan_signature,
        ),
    )


@tag(layer="execution", artifact="runtime_artifacts", kind="context")
def runtime_artifacts(
    engine_session: EngineSession,
    relspec_param_values: JsonDict,
) -> RuntimeArtifacts:
    """Return the runtime artifacts container for task execution.

    Returns
    -------
    RuntimeArtifacts
        Runtime artifacts container.
    """
    return RuntimeArtifacts(
        execution=engine_session.df_runtime(),
        determinism_tier=engine_session.surface_policy.determinism_tier,
        rulepack_param_values=relspec_param_values,
    )


@tag(layer="execution", artifact="plan_scan_inputs", kind="context")
def plan_scan_inputs(
    plan_scan_units: tuple[ScanUnit, ...],
    plan_scan_keys_by_task: Mapping[str, tuple[str, ...]],
    plan_scan_units_by_task_name: Mapping[str, ScanUnit],
) -> PlanScanInputs:
    """Bundle scan-unit context for deterministic task execution.

    Returns
    -------
    PlanScanInputs
        Scan-unit context and hash for the execution run.
    """
    mapping_units = tuple(plan_scan_units_by_task_name.values())
    units = (
        plan_scan_units
        if plan_scan_units
        else tuple(sorted(mapping_units, key=lambda unit: unit.key))
    )
    scan_hash: str | None = None
    if units:
        from datafusion_engine.scan_overrides import scan_units_hash

        scan_hash = scan_units_hash(units)
    return PlanScanInputs(
        scan_units=units,
        scan_keys_by_task=plan_scan_keys_by_task,
        scan_units_by_task_name=dict(plan_scan_units_by_task_name),
        scan_units_hash=scan_hash,
    )


@tag(layer="execution", artifact="task_execution_inputs", kind="context")
def task_execution_inputs(
    runtime_artifacts: RuntimeArtifacts,
    evidence_catalog: EvidenceCatalog,
    plan_context: PlanExecutionContext,
) -> TaskExecutionInputs:
    """Bundle shared execution inputs for per-task nodes.

    Parameters
    ----------
    runtime_artifacts
        Runtime artifact container for the execution run.
    evidence_catalog
        Evidence catalog used for scheduling and contract validation.
    plan_context
        Execution-plan context for signatures, tasks, and scan inputs.

    Returns
    -------
    TaskExecutionInputs
        Bundled inputs for task execution.
    """
    return TaskExecutionInputs(
        runtime=runtime_artifacts,
        evidence=evidence_catalog,
        plan_signature=plan_context.plan_signature,
        active_task_names=plan_context.active_task_names,
        plan_bundles_by_task=plan_context.plan_bundles_by_task,
        scan_units=plan_context.plan_scan_inputs.scan_units,
        scan_keys_by_task=plan_context.plan_scan_inputs.scan_keys_by_task,
        scan_units_by_task_name=plan_context.plan_scan_inputs.scan_units_by_task_name,
        scan_units_hash=plan_context.plan_scan_inputs.scan_units_hash,
    )


def _ensure_scan_overrides(
    runtime: RuntimeArtifacts,
    *,
    scan_context: PlanScanInputs,
) -> DataFusionRuntimeProfile:
    session_runtime = runtime.execution
    if session_runtime is None:
        msg = "RuntimeArtifacts.execution must be configured for view execution."
        raise ValueError(msg)
    profile = session_runtime.profile
    session = session_runtime.ctx
    refresh_requested = (
        scan_context.scan_units_hash is not None
        and runtime.scan_override_hash != scan_context.scan_units_hash
    )
    if refresh_requested:
        ensure_view_graph(
            session,
            runtime_profile=profile,
            include_registry_views=True,
            scan_units=scan_context.scan_units,
        )
        runtime.scan_override_hash = scan_context.scan_units_hash
    return profile


def _execute_view(
    runtime: RuntimeArtifacts,
    *,
    view_name: str,
    plan_bundle: DataFusionPlanBundle | None,
    scan_context: PlanScanInputs,
) -> ExecutionResult:
    if plan_bundle is None:
        msg = f"Plan bundle is required for view execution: {view_name!r}."
        raise ValueError(msg)
    profile = _ensure_scan_overrides(runtime, scan_context=scan_context)
    session = profile.session_context()
    if not session.table_exist(view_name):
        ensure_view_graph(
            session,
            runtime_profile=profile,
            include_registry_views=True,
            scan_units=scan_context.scan_units,
        )
        if not session.table_exist(view_name):
            msg = f"View {view_name!r} is not registered; call ensure_view_graph first."
            raise ValueError(msg)
    from datafusion_engine.execution_facade import DataFusionExecutionFacade

    facade = DataFusionExecutionFacade(ctx=session, runtime_profile=profile)
    execution_result = facade.execute_plan_bundle(
        plan_bundle,
        view_name=view_name,
        scan_units=scan_context.scan_units,
        scan_keys=scan_context.scan_keys_by_task.get(view_name, ()),
    )
    dataframe = execution_result.require_dataframe()
    table = dataframe.to_arrow_table()
    return ExecutionResult.from_table(table)


def _execute_scan_task(
    runtime: RuntimeArtifacts,
    *,
    scan_task_name: str,
    scan_unit: ScanUnit,
    scan_context: PlanScanInputs,
) -> ExecutionResult:
    _ensure_scan_overrides(runtime, scan_context=scan_context)
    metadata: dict[bytes, bytes] = {
        b"scan_task_name": scan_task_name.encode("utf-8"),
        b"scan_unit_key": scan_unit.key.encode("utf-8"),
        b"scan_dataset_name": scan_unit.dataset_name.encode("utf-8"),
    }
    if scan_unit.delta_version is not None:
        metadata[b"scan_delta_version"] = str(scan_unit.delta_version).encode("utf-8")
    schema = pa.schema([], metadata=metadata)
    table = empty_table(schema)
    return ExecutionResult.from_table(table)


def execute_task_from_catalog(
    *,
    inputs: TaskExecutionInputs,
    dependencies: Sequence[TableLike],
    plan_signature: str,
    spec: TaskExecutionSpec,
) -> TableLike:
    """Execute a single view task from a catalog.

    Returns
    -------
    TableLike
        Materialized table output.

    Raises
    ------
    ValueError
        Raised when the plan signature is inconsistent or the view is missing.
    """
    _touch_dependencies(dependencies)
    runtime = inputs.runtime
    evidence = inputs.evidence
    if plan_signature != inputs.plan_signature:
        msg = "Plan signature mismatch between injected task inputs."
        raise ValueError(msg)
    existing_artifact = runtime.execution_artifacts.get(spec.task_output)
    if existing_artifact is not None and existing_artifact.result.table is not None:
        existing = existing_artifact.result.table
    else:
        existing = runtime.materialized_tables.get(spec.task_output)
    if existing is not None:
        if spec.task_output not in evidence.sources:
            evidence.register_schema(spec.task_output, existing.schema)
        return existing
    inactive = spec.task_name not in inputs.active_task_names
    if inactive:
        runtime.record_execution(f"{spec.task_name}:skipped")
        msg = f"Task {spec.task_name!r} is inactive under the current incremental plan."
        raise ValueError(msg)
    if spec.task_kind == "scan":
        scan_unit = inputs.scan_units_by_task_name.get(spec.task_name)
        if scan_unit is None:
            msg = f"Missing scan unit for scan task {spec.task_name!r}."
            raise ValueError(msg)
    return _execute_and_record(
        inputs=inputs,
        spec=spec,
        plan_signature=plan_signature,
        inactive=inactive,
    )


def _touch_dependencies(dependencies: Sequence[TableLike]) -> int:
    return len(dependencies)


def _execute_and_record(
    *,
    inputs: TaskExecutionInputs,
    spec: TaskExecutionSpec,
    plan_signature: str,
    inactive: bool,
) -> TableLike:
    runtime = inputs.runtime
    task_name = spec.task_name
    runtime.record_execution(task_name if not inactive else f"{task_name}:inactive")
    scan_context = PlanScanInputs(
        scan_units=inputs.scan_units,
        scan_keys_by_task=inputs.scan_keys_by_task,
        scan_units_by_task_name=inputs.scan_units_by_task_name,
        scan_units_hash=inputs.scan_units_hash,
    )
    if spec.task_kind == "scan":
        scan_unit = inputs.scan_units_by_task_name.get(spec.task_name)
        if scan_unit is None:
            msg = f"Scan task {spec.task_name!r} is missing a scan unit mapping."
            raise ValueError(msg)
        result = _execute_scan_task(
            runtime,
            scan_task_name=spec.task_name,
            scan_unit=scan_unit,
            scan_context=scan_context,
        )
    else:
        plan_bundle = inputs.plan_bundles_by_task.get(spec.task_output)
        result = _execute_view(
            runtime,
            view_name=spec.task_output,
            plan_bundle=plan_bundle,
            scan_context=scan_context,
        )
    table = result.require_table()
    _record_output(
        inputs=inputs,
        spec=spec,
        plan_signature=plan_signature,
        table=table,
    )
    return table


@pipe_input(
    step(_finalize_cpg_nodes_stage, runtime_profile_spec=source("runtime_profile_spec")),
    on_input="cpg_nodes_v1",
    namespace="cpg_nodes_final",
)
@tag(layer="execution", artifact="cpg_nodes_final", kind="table")
def cpg_nodes_final(cpg_nodes_v1: TableLike) -> TableLike:
    """Return the final CPG nodes table.

    Returns
    -------
    TableLike
        Final nodes table.
    """
    return cpg_nodes_v1


@pipe_input(
    step(_finalize_cpg_edges_stage, runtime_profile_spec=source("runtime_profile_spec")),
    on_input="cpg_edges_v1",
    namespace="cpg_edges_final",
)
@tag(layer="execution", artifact="cpg_edges_final", kind="table")
def cpg_edges_final(cpg_edges_v1: TableLike) -> TableLike:
    """Return the final CPG edges table.

    Returns
    -------
    TableLike
        Final edges table.
    """
    return cpg_edges_v1


@pipe_input(
    step(_finalize_cpg_props_stage, runtime_profile_spec=source("runtime_profile_spec")),
    on_input="cpg_props_v1",
    namespace="cpg_props_final",
)
@tag(layer="execution", artifact="cpg_props_final", kind="table")
def cpg_props_final(cpg_props_v1: TableLike) -> TableLike:
    """Return the final CPG properties table.

    Returns
    -------
    TableLike
        Final properties table.
    """
    return cpg_props_v1


__all__ = [
    "PlanExecutionContext",
    "PlanScanInputs",
    "TaskExecutionInputs",
    "TaskExecutionSpec",
    "_finalize_cpg_edges_stage",
    "_finalize_cpg_nodes_stage",
    "_finalize_cpg_props_stage",
    "cpg_edges_final",
    "cpg_nodes_final",
    "cpg_props_final",
    "execute_task_from_catalog",
    "plan_scan_inputs",
    "runtime_artifacts",
    "task_execution_inputs",
]
