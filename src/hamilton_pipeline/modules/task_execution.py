"""Hamilton nodes for executing tasks from inferred plans."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from hamilton.function_modifiers import pipe_input, source, step, tag
from hamilton.htypes import Collect, Parallelizable

from arrowdsl.core.interop import TableLike as ArrowTableLike
from arrowdsl.schema.abi import schema_fingerprint
from arrowdsl.schema.build import empty_table
from core_types import JsonDict
from datafusion_engine.execution_facade import ExecutionResult
from datafusion_engine.finalize import Contract, normalize_only
from datafusion_engine.view_registry import ensure_view_graph
from relspec.evidence import EvidenceCatalog, initial_evidence_from_views
from relspec.inferred_deps import infer_deps_from_view_nodes
from relspec.runtime_artifacts import ExecutionArtifactSpec, RuntimeArtifacts, TableLike
from relspec.rustworkx_graph import task_graph_impact_subgraph

if TYPE_CHECKING:
    from datafusion import SessionContext

    from arrowdsl.core.execution_context import ExecutionContext
    from datafusion_engine.view_graph_registry import ViewNode
    from ibis_engine.execution import IbisExecutionContext
    from relspec.graph_inference import TaskGraph
    from relspec.incremental import IncrementalDiff


@dataclass(frozen=True)
class TaskExecutionInputs:
    """Shared inputs for task execution."""

    runtime: RuntimeArtifacts
    evidence: EvidenceCatalog
    plan_fingerprints: Mapping[str, str]


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


def _initial_evidence(
    nodes: Sequence[ViewNode],
    *,
    ctx: SessionContext | None = None,
    task_names: set[str] | None = None,
) -> EvidenceCatalog:
    return initial_evidence_from_views(
        nodes,
        ctx=ctx,
        task_names=task_names,
    )


def _plan_fingerprints(nodes: Sequence[ViewNode]) -> dict[str, str]:
    inferred = infer_deps_from_view_nodes(nodes)
    return {dep.task_name: dep.plan_fingerprint for dep in inferred}


def _record_output(
    *,
    inputs: TaskExecutionInputs,
    task_name: str,
    task_output: str,
    plan_fingerprint: str | None,
    table: TableLike,
) -> None:
    evidence = inputs.evidence
    runtime = inputs.runtime
    schema = table.schema
    evidence.register_schema(task_output, schema)
    if task_output in runtime.execution_artifacts:
        return
    runtime.register_execution(
        task_output,
        ExecutionResult.from_table(cast("ArrowTableLike", table)),
        spec=ExecutionArtifactSpec(
            source_task=task_name,
            schema_fingerprint=schema_fingerprint(schema),
            plan_fingerprint=plan_fingerprint,
        ),
    )


@tag(layer="execution", artifact="runtime_artifacts", kind="context")
def runtime_artifacts(
    ibis_execution: IbisExecutionContext,
    relspec_param_values: JsonDict,
) -> RuntimeArtifacts:
    """Return the runtime artifacts container for task execution.

    Returns
    -------
    RuntimeArtifacts
        Runtime artifacts container.
    """
    return RuntimeArtifacts(
        execution=ibis_execution,
        rulepack_param_values=relspec_param_values,
    )


@tag(layer="execution", artifact="task_execution_inputs", kind="context")
def task_execution_inputs(
    view_nodes: tuple[ViewNode, ...],
    runtime_artifacts: RuntimeArtifacts,
    evidence_catalog: EvidenceCatalog,
) -> TaskExecutionInputs:
    """Bundle shared execution inputs for per-task nodes.

    Returns
    -------
    TaskExecutionInputs
        Bundled inputs for task execution.
    """
    return TaskExecutionInputs(
        runtime=runtime_artifacts,
        evidence=evidence_catalog,
        plan_fingerprints=_plan_fingerprints(view_nodes),
    )


@tag(layer="execution", artifact="evidence_catalog", kind="catalog")
def evidence_catalog(
    view_nodes: tuple[ViewNode, ...],
    ibis_execution: IbisExecutionContext,
) -> EvidenceCatalog:
    """Return an initialized EvidenceCatalog for task execution.

    Returns
    -------
    EvidenceCatalog
    Evidence catalog seeded from view requirements.
    """
    df_profile = ibis_execution.ctx.runtime.datafusion
    session = df_profile.session_context() if df_profile is not None else None
    if df_profile is not None and session is not None:
        ensure_view_graph(
            session,
            runtime_profile=df_profile,
            include_registry_views=True,
        )
    evidence = _initial_evidence(view_nodes, ctx=session)
    if df_profile is not None and evidence.contract_violations_by_dataset:
        from datafusion_engine.diagnostics import record_artifact

        payload = [
            {
                "dataset": name,
                "violations": [str(item) for item in violations],
            }
            for name, violations in evidence.contract_violations_by_dataset.items()
        ]
        record_artifact(
            df_profile,
            "evidence_contract_violations_v1",
            {"violations": payload},
        )
    if df_profile is not None:
        from datafusion_engine.diagnostics import record_artifact
        from datafusion_engine.udf_parity import udf_parity_report
        from datafusion_engine.udf_runtime import register_rust_udfs

        session = df_profile.session_context()
        async_timeout_ms = None
        async_batch_size = None
        if df_profile.enable_async_udfs:
            async_timeout_ms = df_profile.async_udf_timeout_ms
            async_batch_size = df_profile.async_udf_batch_size
        registry_snapshot = register_rust_udfs(
            session,
            enable_async=df_profile.enable_async_udfs,
            async_udf_timeout_ms=async_timeout_ms,
            async_udf_batch_size=async_batch_size,
        )
        report = udf_parity_report(session, snapshot=registry_snapshot)
        record_artifact(df_profile, "udf_parity_v1", report.payload())
    return evidence


def task_graph_for_diff(
    task_graph: TaskGraph,
    diff: IncrementalDiff | None,
) -> TaskGraph:
    """Return the execution graph for an incremental diff.

    Returns
    -------
    TaskGraph
        Impact subgraph when diff is provided, otherwise the full graph.
    """
    if diff is None:
        return task_graph
    changed = diff.tasks_requiring_rebuild()
    if not changed:
        return task_graph_impact_subgraph(task_graph, task_names=())
    return task_graph_impact_subgraph(task_graph, task_names=changed)


def _execute_view(
    runtime: RuntimeArtifacts,
    *,
    view_name: str,
) -> ExecutionResult:
    exec_ctx = runtime.execution
    if exec_ctx is None:
        msg = "RuntimeArtifacts.execution must be configured for view execution."
        raise ValueError(msg)
    profile = exec_ctx.ctx.runtime.datafusion
    if profile is None:
        msg = "DataFusion runtime profile is required for view execution."
        raise ValueError(msg)
    session = profile.session_context()
    if not session.table_exist(view_name):
        ensure_view_graph(
            session,
            runtime_profile=profile,
            include_registry_views=True,
        )
        if not session.table_exist(view_name):
            msg = f"View {view_name!r} is not registered; call ensure_view_graph first."
            raise ValueError(msg)
    table = session.table(view_name).to_arrow_table()
    return ExecutionResult.from_table(table)


def execute_task_from_catalog(
    *,
    inputs: TaskExecutionInputs,
    task_name: str,
    task_output: str,
    dependencies: Sequence[TableLike],
) -> TableLike:
    """Execute a single view task from a catalog.

    Returns
    -------
    TableLike
        Materialized table output.
    """
    _touch_dependencies(dependencies)
    runtime = inputs.runtime
    evidence = inputs.evidence
    plan_fingerprint = inputs.plan_fingerprints.get(task_name)
    existing_artifact = runtime.execution_artifacts.get(task_output)
    if existing_artifact is not None and existing_artifact.result.table is not None:
        existing = existing_artifact.result.table
    else:
        existing = runtime.materialized_tables.get(task_output)
    if existing is not None:
        if task_output not in evidence.sources:
            evidence.register_schema(task_output, existing.schema)
        return existing
    result = _execute_view(runtime, view_name=task_output)
    table = result.require_table()
    _record_output(
        inputs=inputs,
        task_name=task_name,
        task_output=task_output,
        plan_fingerprint=plan_fingerprint,
        table=table,
    )
    return table


def _touch_dependencies(dependencies: Sequence[TableLike]) -> int:
    return len(dependencies)


@tag(layer="execution", artifact="task_generation_wave", kind="parallel")
def task_generation_wave(
    task_generations: tuple[tuple[str, ...], ...],
) -> Parallelizable[tuple[str, ...]]:
    """Yield task generations for dynamic execution.

    Yields
    ------
    tuple[str, ...]
        Task names for a single generation wave.
    """
    yield from task_generations


@tag(layer="execution", artifact="generation_outputs", kind="parallel")
def generation_outputs(
    task_generation_wave: tuple[str, ...],
    task_execution_inputs: TaskExecutionInputs,
) -> list[TableLike]:
    """Execute a generation of tasks sequentially and return outputs.

    Returns
    -------
    list[TableLike]
        Outputs produced for the generation.
    """
    return [
        execute_task_from_catalog(
            inputs=task_execution_inputs,
            task_name=task_name,
            task_output=task_name,
            dependencies=(),
        )
        for task_name in task_generation_wave
    ]


@tag(layer="execution", artifact="generation_outputs_all", kind="parallel")
def generation_outputs_all(
    generation_outputs: Collect[list[TableLike]],
) -> list[list[TableLike]]:
    """Collect outputs from all generations.

    Returns
    -------
    list[list[TableLike]]
        Outputs grouped by generation.
    """
    return list(generation_outputs)


@tag(layer="execution", artifact="generation_execution_gate", kind="gate")
def generation_execution_gate(generation_outputs_all: list[list[TableLike]]) -> int:
    """Return the total count of generation outputs executed.

    Returns
    -------
    int
        Count of outputs executed across generations.
    """
    return sum(len(outputs) for outputs in generation_outputs_all)


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
    "_finalize_cpg_edges_stage",
    "_finalize_cpg_nodes_stage",
    "_finalize_cpg_props_stage",
    "cpg_edges_final",
    "cpg_nodes_final",
    "cpg_props_final",
    "evidence_catalog",
    "execute_task_from_catalog",
    "generation_execution_gate",
    "generation_outputs",
    "generation_outputs_all",
    "runtime_artifacts",
    "task_execution_inputs",
    "task_generation_wave",
    "task_graph_for_diff",
]
