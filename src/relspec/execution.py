"""Execution helpers for plan artifacts."""

from __future__ import annotations

from dataclasses import dataclass, replace

from arrowdsl.core.interop import TableLike
from ibis_engine.execution import materialize_ibis_plan
from relspec.plan_catalog import PlanArtifact
from relspec.runtime_artifacts import RuntimeArtifacts


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
    param_mapping = request.runtime.param_mapping_for_task(request.artifact)
    execution = replace(exec_ctx, params=param_mapping) if param_mapping is not None else exec_ctx
    return materialize_ibis_plan(request.artifact.plan, execution=execution)


__all__ = ["TaskExecutionRequest", "execute_plan_artifact"]
