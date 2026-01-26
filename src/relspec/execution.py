"""Execution helpers for plan artifacts."""

from __future__ import annotations

from dataclasses import dataclass, replace

from arrowdsl.core.interop import TableLike
from datafusion_engine.diagnostics import record_artifact
from datafusion_engine.execution_facade import ExecutionResult
from ibis_engine.execution import execute_ibis_plan
from relspec.plan_catalog import PlanArtifact
from relspec.runtime_artifacts import ExecutionArtifactSpec, RuntimeArtifacts


@dataclass(frozen=True)
class TaskExecutionRequest:
    """Execution inputs for a plan artifact."""

    artifact: PlanArtifact
    runtime: RuntimeArtifacts


def execute_plan_artifact(request: TaskExecutionRequest) -> TableLike:
    """Execute a compiled plan artifact.

    Parameters
    ----------
    request : TaskExecutionRequest
        Execution request containing the plan artifact and runtime.

    Returns
    -------
    TableLike
        Materialized table output.

    Raises
    ------
    ValueError
        Raised when the runtime execution context is missing.
    """
    exec_ctx = request.runtime.execution
    if exec_ctx is None:
        msg = "RuntimeArtifacts.execution must be configured for plan execution."
        raise ValueError(msg)
    if request.artifact.task.kind == "view":
        result = _execute_view_artifact(request)
        _register_execution_result(request, result)
        return result.require_table()
    param_mapping = request.runtime.param_mapping_for_task(request.artifact)
    execution = replace(exec_ctx, params=param_mapping) if param_mapping is not None else exec_ctx
    result = execute_ibis_plan(
        request.artifact.plan,
        execution=execution,
        streaming=False,
    )
    _register_execution_result(request, result)
    return result.require_table()


def _execute_view_artifact(request: TaskExecutionRequest) -> ExecutionResult:
    """Execute a view task using the DataFusion session context.

    Parameters
    ----------
    request
        Task execution request containing the view artifact.

    Returns
    -------
    ExecutionResult
        Execution result with materialized Arrow table.

    Raises
    ------
    ValueError
        Raised when the runtime execution context is missing.
    """
    exec_ctx = request.runtime.execution
    if exec_ctx is None:
        msg = "RuntimeArtifacts.execution must be configured for view execution."
        raise ValueError(msg)
    profile = exec_ctx.ctx.runtime.datafusion
    if profile is None:
        msg = "DataFusion runtime profile is required for view execution."
        raise ValueError(msg)
    session = profile.session_context()
    output = request.artifact.task.output
    if not session.table_exist(output):
        msg = f"View {output!r} is not registered; call ensure_view_graph first."
        raise ValueError(msg)
    df = session.table(output)
    table = df.to_arrow_table()
    return ExecutionResult.from_table(table)


def _register_execution_result(
    request: TaskExecutionRequest,
    result: ExecutionResult,
) -> None:
    runtime = request.runtime
    artifact = request.artifact
    runtime.register_execution(
        artifact.task.output,
        result,
        spec=ExecutionArtifactSpec(
            source_task=artifact.task.name,
            plan_fingerprint=artifact.plan_fingerprint,
        ),
    )
    exec_ctx = runtime.execution
    if exec_ctx is None:
        return
    profile = exec_ctx.ctx.runtime.datafusion
    if profile is None:
        return
    row_count = None
    if result.table is not None:
        to_pyarrow = getattr(result.table, "to_pyarrow", None)
        if callable(to_pyarrow):
            try:
                arrow_table = to_pyarrow()
            except (AttributeError, TypeError, ValueError):
                arrow_table = None
            if hasattr(arrow_table, "num_rows"):
                num_rows = getattr(arrow_table, "num_rows", None)
                if isinstance(num_rows, int):
                    row_count = num_rows
    payload = {
        "task_name": artifact.task.name,
        "output": artifact.task.output,
        "plan_fingerprint": artifact.plan_fingerprint,
        "result_kind": result.kind.value,
        "rows": row_count,
    }
    record_artifact(profile, "relspec_plan_execute_v1", payload)


__all__ = ["TaskExecutionRequest", "execute_plan_artifact"]
