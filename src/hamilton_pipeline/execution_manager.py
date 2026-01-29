"""Plan-aware execution manager routing scan tasks to remote executors."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence

from hamilton.execution import executors

from obs.diagnostics import DiagnosticsCollector

_SCAN_TASK_KIND = "scan"


def _task_tags(task: executors.TaskImplementation) -> Mapping[str, object]:
    nodes = getattr(task, "nodes", None)
    if not isinstance(nodes, Sequence):
        return {}
    for node in nodes:
        tags = getattr(node, "tags", None)
        if isinstance(tags, Mapping):
            return tags
    return {}


def _task_kind(tags: Mapping[str, object]) -> str | None:
    kind = tags.get("task_kind")
    return kind if isinstance(kind, str) else None


def _task_cost(tags: Mapping[str, object]) -> float | None:
    value = tags.get("task_cost")
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


def _is_high_cost(tags: Mapping[str, object], *, threshold: float | None) -> bool:
    if threshold is None:
        return False
    cost = _task_cost(tags)
    return cost is not None and cost >= threshold


class PlanExecutionManager(executors.ExecutionManager):
    """Route scan tasks to remote execution and keep view tasks local."""

    def __init__(
        self,
        *,
        local_executor: executors.TaskExecutor,
        remote_executor: executors.TaskExecutor,
        cost_threshold: float | None = None,
        diagnostics: DiagnosticsCollector | None = None,
    ) -> None:
        super().__init__([local_executor, remote_executor])
        self._local_executor = local_executor
        self._remote_executor = remote_executor
        self._cost_threshold = cost_threshold
        self._diagnostics = diagnostics

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
        tags = _task_tags(task)
        use_remote = _task_kind(tags) == _SCAN_TASK_KIND or _is_high_cost(
            tags,
            threshold=self._cost_threshold,
        )
        executor = self._remote_executor if use_remote else self._local_executor
        if self._diagnostics is not None:
            payload = {
                "run_id": getattr(task, "run_id", None),
                "task_id": getattr(task, "task_id", None),
                "task_name": tags.get("task_name"),
                "task_kind": _task_kind(tags),
                "task_cost": _task_cost(tags),
                "cost_threshold": self._cost_threshold,
                "executor": "remote" if use_remote else "local",
                "event_time_unix_ms": int(time.time() * 1000),
            }
            self._diagnostics.record_events("hamilton_task_routing_v1", [payload])
        return executor


__all__ = ["PlanExecutionManager"]
