"""Plan-aware execution manager routing scan tasks to remote executors."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from hamilton.execution import executors

_SCAN_TASK_KIND = "scan"


def _task_kind(task: executors.TaskImplementation) -> str | None:
    nodes = getattr(task, "nodes", None)
    if not isinstance(nodes, Sequence):
        return None
    for node in nodes:
        tags = getattr(node, "tags", None)
        if not isinstance(tags, Mapping):
            continue
        kind = tags.get("task_kind")
        if isinstance(kind, str):
            return kind
    return None


class PlanExecutionManager(executors.ExecutionManager):
    """Route scan tasks to remote execution and keep view tasks local."""

    def __init__(
        self,
        *,
        local_executor: executors.TaskExecutor,
        remote_executor: executors.TaskExecutor,
    ) -> None:
        super().__init__([local_executor, remote_executor])
        self._local_executor = local_executor
        self._remote_executor = remote_executor

    def get_executor_for_task(
        self,
        task: executors.TaskImplementation,
    ) -> executors.TaskExecutor:
        """Return the executor for a task based on its tagged kind.

        Returns
        -------
        executors.TaskExecutor
            Remote executor for scan tasks, local executor otherwise.
        """
        if _task_kind(task) == _SCAN_TASK_KIND:
            return self._remote_executor
        return self._local_executor


__all__ = ["PlanExecutionManager"]
