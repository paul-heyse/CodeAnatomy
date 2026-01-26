"""Task catalog for CPG build plans."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from cpg.plan_builders import build_cpg_edges_plan, build_cpg_nodes_plan, build_cpg_props_plan
from cpg.specs import TaskIdentity
from datafusion_engine.diagnostics import record_artifact
from relspec.context import ensure_task_build_context
from relspec.task_catalog import TaskBuildContext, TaskCatalog, TaskSpec

if TYPE_CHECKING:
    from arrowdsl.core.execution_context import ExecutionContext
    from datafusion_engine.diagnostics import DiagnosticsRecorder
    from ibis_engine.plan import IbisPlan


def cpg_task_catalog() -> TaskCatalog:
    """Return CPG tasks for the inference pipeline.

    Returns
    -------
    TaskCatalog
        Catalog with task specs for CPG outputs.
    """
    cpg_task_priority = 100
    cpg_nodes_task = "cpg.nodes"
    cpg_edges_task = "cpg.edges"
    cpg_props_task = "cpg.props"

    nodes_identity = TaskIdentity(name=cpg_nodes_task, priority=cpg_task_priority)
    props_identity = TaskIdentity(name=cpg_props_task, priority=cpg_task_priority)

    def _build_nodes(context: TaskBuildContext) -> IbisPlan:
        resolved = _ensure_cpg_build_context(context, operation_id=f"cpg.build.{cpg_nodes_task}")
        if resolved.ibis_catalog is None:
            msg = "TaskBuildContext.ibis_catalog is required for CPG tasks."
            raise ValueError(msg)
        plan = build_cpg_nodes_plan(
            resolved.ibis_catalog,
            resolved.ctx,
            resolved.backend,
            task_identity=nodes_identity,
        )
        _record_build_artifact(
            resolved.ctx,
            recorder=resolved.diagnostics,
            task_name=cpg_nodes_task,
            output="cpg_nodes_v1",
        )
        return plan

    def _build_edges(context: TaskBuildContext) -> IbisPlan:
        resolved = _ensure_cpg_build_context(context, operation_id=f"cpg.build.{cpg_edges_task}")
        if resolved.ibis_catalog is None:
            msg = "TaskBuildContext.ibis_catalog is required for CPG tasks."
            raise ValueError(msg)
        plan = build_cpg_edges_plan(
            resolved.ibis_catalog,
            resolved.ctx,
            resolved.backend,
        )
        _record_build_artifact(
            resolved.ctx,
            recorder=resolved.diagnostics,
            task_name=cpg_edges_task,
            output="cpg_edges_v1",
        )
        return plan

    def _build_props(context: TaskBuildContext) -> IbisPlan:
        resolved = _ensure_cpg_build_context(context, operation_id=f"cpg.build.{cpg_props_task}")
        if resolved.ibis_catalog is None:
            msg = "TaskBuildContext.ibis_catalog is required for CPG tasks."
            raise ValueError(msg)
        plan = build_cpg_props_plan(
            resolved.ibis_catalog,
            resolved.ctx,
            resolved.backend,
            task_identity=props_identity,
        )
        _record_build_artifact(
            resolved.ctx,
            recorder=resolved.diagnostics,
            task_name=cpg_props_task,
            output="cpg_props_v1",
        )
        return plan

    tasks = (
        TaskSpec(
            name=cpg_nodes_task,
            output="cpg_nodes_v1",
            build=_build_nodes,
            kind="compute",
            priority=cpg_task_priority,
        ),
        TaskSpec(
            name=cpg_edges_task,
            output="cpg_edges_v1",
            build=_build_edges,
            kind="compute",
            priority=cpg_task_priority,
        ),
        TaskSpec(
            name=cpg_props_task,
            output="cpg_props_v1",
            build=_build_props,
            kind="compute",
            priority=cpg_task_priority,
        ),
    )
    return TaskCatalog(tasks=tasks)


def _ensure_cpg_build_context(
    context: TaskBuildContext,
    *,
    operation_id: str,
) -> TaskBuildContext:
    """Return a TaskBuildContext with facade/diagnostics for CPG tasks.

    Parameters
    ----------
    context:
        Task build context passed to the CPG builder.
    operation_id:
        Diagnostics operation identifier for the build.

    Returns
    -------
    TaskBuildContext
        Build context with facade and diagnostics recorder attached when available.
    """
    resolved = ensure_task_build_context(
        context.ctx,
        context.backend,
        build_context=context,
        ibis_catalog=context.ibis_catalog,
        runtime=context.runtime,
    )
    if resolved.diagnostics is None and resolved.facade is not None:
        recorder = resolved.facade.diagnostics_recorder(operation_id=operation_id)
        if recorder is not None:
            resolved = replace(resolved, diagnostics=recorder)
    return resolved


def _record_build_artifact(
    ctx: ExecutionContext,
    *,
    recorder: DiagnosticsRecorder | None,
    task_name: str,
    output: str,
) -> None:
    """Record a build-time diagnostics artifact for a CPG task.

    Parameters
    ----------
    ctx:
        Execution context providing the runtime profile.
    recorder:
        Optional diagnostics recorder scoped to the build operation.
    task_name:
        Task identifier for the CPG build.
    output:
        Output dataset name produced by the task.
    """
    runtime = getattr(ctx, "runtime", None)
    profile = getattr(runtime, "datafusion", None) if runtime is not None else None
    payload = {"task_name": task_name, "output": output}
    if recorder is not None:
        recorder.record_artifact("cpg_plan_build_v1", payload)
        return
    record_artifact(profile, "cpg_plan_build_v1", payload)


__all__ = ["cpg_task_catalog"]
