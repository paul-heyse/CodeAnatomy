"""Hamilton nodes for executing tasks from inferred plans."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from hamilton.function_modifiers import pipe_input, source, step, tag

from arrowdsl.core.interop import TableLike as ArrowTableLike
from arrowdsl.schema.abi import schema_fingerprint
from arrowdsl.schema.build import empty_table
from core_types import JsonDict
from datafusion_engine.execution_facade import ExecutionResult
from datafusion_engine.finalize import Contract, normalize_only
from datafusion_engine.view_registry import ensure_view_graph
from relspec.evidence import EvidenceCatalog
from relspec.runtime_artifacts import ExecutionArtifactSpec, RuntimeArtifacts, TableLike

if TYPE_CHECKING:
    from arrowdsl.core.execution_context import ExecutionContext
    from datafusion_engine.plan_bundle import DataFusionPlanBundle
    from datafusion_engine.scan_planner import ScanUnit
    from engine.session import EngineSession


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
    scan_units_hash: str | None


@dataclass(frozen=True)
class PlanScanInputs:
    """Scan-unit context for deterministic execution."""

    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]
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


def _finalize_cpg_table(
    table: TableLike,
    *,
    name: str,
    ctx: ExecutionContext,
) -> TableLike:
    schema_names = getattr(table.schema, "names", [])
    if not schema_names:
        return empty_table(table.schema)
    contract = Contract(name=name, schema=table.schema)
    return normalize_only(cast("ArrowTableLike", table), contract=contract, ctx=ctx)


@tag(layer="execution", artifact="cpg_nodes_finalize", kind="stage")
def _finalize_cpg_nodes_stage(table: TableLike, ctx: ExecutionContext) -> TableLike:
    return _finalize_cpg_table(table, name="cpg_nodes_v1", ctx=ctx)


@tag(layer="execution", artifact="cpg_edges_finalize", kind="stage")
def _finalize_cpg_edges_stage(table: TableLike, ctx: ExecutionContext) -> TableLike:
    return _finalize_cpg_table(table, name="cpg_edges_v1", ctx=ctx)


@tag(layer="execution", artifact="cpg_props_finalize", kind="stage")
def _finalize_cpg_props_stage(table: TableLike, ctx: ExecutionContext) -> TableLike:
    return _finalize_cpg_table(table, name="cpg_props_v1", ctx=ctx)


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
        execution=engine_session.ctx,
        rulepack_param_values=relspec_param_values,
    )


@tag(layer="execution", artifact="plan_scan_inputs", kind="context")
def plan_scan_inputs(
    plan_scan_units: tuple[ScanUnit, ...],
    plan_scan_keys_by_task: Mapping[str, tuple[str, ...]],
) -> PlanScanInputs:
    """Bundle scan-unit context for deterministic task execution.

    Returns
    -------
    PlanScanInputs
        Scan-unit context and hash for the execution run.
    """
    scan_hash: str | None = None
    if plan_scan_units:
        from datafusion_engine.scan_overrides import scan_units_hash

        scan_hash = scan_units_hash(plan_scan_units)
    return PlanScanInputs(
        scan_units=plan_scan_units,
        scan_keys_by_task=plan_scan_keys_by_task,
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
        scan_units_hash=plan_context.plan_scan_inputs.scan_units_hash,
    )


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
    exec_ctx = runtime.execution
    if exec_ctx is None:
        msg = "RuntimeArtifacts.execution must be configured for view execution."
        raise ValueError(msg)
    profile = exec_ctx.runtime.datafusion
    if profile is None:
        msg = "DataFusion runtime profile is required for view execution."
        raise ValueError(msg)
    session = profile.session_context()
    refresh_requested = not session.table_exist(view_name)
    if (
        scan_context.scan_units_hash is not None
        and runtime.scan_override_hash != scan_context.scan_units_hash
    ):
        refresh_requested = True
    if refresh_requested:
        ensure_view_graph(
            session,
            runtime_profile=profile,
            include_registry_views=True,
            scan_units=scan_context.scan_units,
        )
        if not session.table_exist(view_name):
            msg = f"View {view_name!r} is not registered; call ensure_view_graph first."
            raise ValueError(msg)
        runtime.scan_override_hash = scan_context.scan_units_hash
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
    plan_bundle = inputs.plan_bundles_by_task.get(spec.task_output)
    result = _execute_view(
        runtime,
        view_name=spec.task_output,
        plan_bundle=plan_bundle,
        scan_context=PlanScanInputs(
            scan_units=inputs.scan_units,
            scan_keys_by_task=inputs.scan_keys_by_task,
            scan_units_hash=inputs.scan_units_hash,
        ),
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
    step(_finalize_cpg_nodes_stage, ctx=source("ctx")),
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
    step(_finalize_cpg_edges_stage, ctx=source("ctx")),
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
    step(_finalize_cpg_props_stage, ctx=source("ctx")),
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
    "TaskExecutionInputs",
    "TaskExecutionSpec",
    "_finalize_cpg_edges_stage",
    "_finalize_cpg_nodes_stage",
    "_finalize_cpg_props_stage",
    "cpg_edges_final",
    "cpg_nodes_final",
    "cpg_props_final",
    "execute_task_from_catalog",
    "runtime_artifacts",
    "task_execution_inputs",
]
