"""Hamilton nodes for executing tasks from inferred plans."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from hamilton.function_modifiers import pipe_input, source, step, tag
from hamilton.htypes import Collect, Parallelizable

from arrowdsl.core.interop import TableLike as ArrowTableLike
from arrowdsl.schema.build import empty_table
from arrowdsl.schema.serialization import schema_fingerprint
from core_types import JsonDict
from datafusion_engine.finalize import Contract, normalize_only
from relspec.evidence import EvidenceCatalog, initial_evidence_from_plan
from relspec.execution import TaskExecutionRequest, execute_plan_artifact
from relspec.plan_catalog import PlanArtifact, PlanCatalog
from relspec.runtime_artifacts import MaterializedTableSpec, RuntimeArtifacts, TableLike
from relspec.rustworkx_graph import task_graph_impact_subgraph

if TYPE_CHECKING:
    from datafusion import SessionContext

    from arrowdsl.core.execution_context import ExecutionContext
    from ibis_engine.execution import IbisExecutionContext
    from relspec.graph_inference import TaskGraph
    from relspec.incremental import IncrementalDiff


@dataclass(frozen=True)
class TaskExecutionInputs:
    """Shared inputs for task execution."""

    plan_catalog: PlanCatalog
    runtime: RuntimeArtifacts
    evidence: EvidenceCatalog


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


@tag(layer="execution", artifact="cpg_nodes_finalize", kind="stage")
def _finalize_cpg_nodes_stage(table: TableLike, ctx: ExecutionContext) -> TableLike:
    from cpg.schemas import CPG_NODES_CONTRACT

    return _finalize_cpg_table(table, contract=CPG_NODES_CONTRACT, ctx=ctx)


@tag(layer="execution", artifact="cpg_edges_finalize", kind="stage")
def _finalize_cpg_edges_stage(table: TableLike, ctx: ExecutionContext) -> TableLike:
    from cpg.schemas import CPG_EDGES_CONTRACT

    return _finalize_cpg_table(table, contract=CPG_EDGES_CONTRACT, ctx=ctx)


@tag(layer="execution", artifact="cpg_props_finalize", kind="stage")
def _finalize_cpg_props_stage(table: TableLike, ctx: ExecutionContext) -> TableLike:
    from cpg.schemas import CPG_PROPS_CONTRACT

    return _finalize_cpg_table(table, contract=CPG_PROPS_CONTRACT, ctx=ctx)


def _initial_evidence(
    catalog: PlanCatalog,
    *,
    ctx: SessionContext | None = None,
    task_names: set[str] | None = None,
) -> EvidenceCatalog:
    return initial_evidence_from_plan(
        catalog,
        ctx=ctx,
        task_names=task_names,
    )


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
    plan_catalog: PlanCatalog,
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
        plan_catalog=plan_catalog,
        runtime=runtime_artifacts,
        evidence=evidence_catalog,
    )


@tag(layer="execution", artifact="evidence_catalog", kind="catalog")
def evidence_catalog(
    plan_catalog: PlanCatalog,
    ibis_execution: IbisExecutionContext,
) -> EvidenceCatalog:
    """Return an initialized EvidenceCatalog for task execution.

    Returns
    -------
    EvidenceCatalog
        Evidence catalog seeded from plan requirements.
    """
    df_profile = ibis_execution.ctx.runtime.datafusion
    session = df_profile.session_context() if df_profile is not None else None
    evidence = _initial_evidence(plan_catalog, ctx=session)
    if (
        df_profile is not None
        and df_profile.diagnostics_sink is not None
        and evidence.contract_violations_by_dataset
    ):
        payload = [
            {
                "dataset": name,
                "violations": [str(item) for item in violations],
            }
            for name, violations in evidence.contract_violations_by_dataset.items()
        ]
        df_profile.diagnostics_sink.record_artifact(
            "evidence_contract_violations_v1",
            {"violations": payload},
        )
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


def execute_task_from_catalog(
    *,
    inputs: TaskExecutionInputs,
    task_name: str,
    task_output: str,
    dependencies: Sequence[TableLike],
) -> TableLike:
    """Execute a single plan artifact from a catalog.

    Returns
    -------
    TableLike
        Materialized table output.

    Raises
    ------
    KeyError
        Raised when the task artifact is missing from the catalog.
    """
    _touch_dependencies(dependencies)
    plan_catalog = inputs.plan_catalog
    runtime = inputs.runtime
    evidence = inputs.evidence
    existing = runtime.materialized_tables.get(task_output)
    if existing is not None:
        if task_output not in evidence.sources:
            evidence.register(task_output, existing.schema)
        return existing
    by_task = plan_catalog.by_task()
    artifact = by_task.get(task_name)
    if artifact is None:
        msg = f"Missing plan artifact for task {task_name!r}."
        raise KeyError(msg)
    table = execute_plan_artifact(TaskExecutionRequest(artifact=artifact, runtime=runtime))
    outputs: dict[str, TableLike] = {}
    _record_output(
        artifact,
        table=table,
        outputs=outputs,
        evidence=evidence,
        runtime=runtime,
    )
    return outputs.get(task_output, table)


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
    outputs: list[TableLike] = []
    by_task = task_execution_inputs.plan_catalog.by_task()
    for task_name in task_generation_wave:
        artifact = by_task.get(task_name)
        if artifact is None:
            continue
        outputs.append(
            execute_task_from_catalog(
                inputs=task_execution_inputs,
                task_name=task_name,
                task_output=artifact.task.output,
                dependencies=(),
            )
        )
    return outputs


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
