"""Task catalog definitions for inference-driven scheduling."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from ibis.backends import BaseBackend

    from arrowdsl.core.execution_context import ExecutionContext
    from ibis_engine.catalog import IbisPlanCatalog
    from ibis_engine.plan import IbisPlan
    from normalize.runtime import NormalizeRuntime

TaskKind = Literal["view", "compute", "materialization"]
CachePolicy = Literal["none", "session", "persistent"]


@dataclass(frozen=True)
class TaskBuildContext:
    """Context passed to task builders."""

    ctx: ExecutionContext
    backend: BaseBackend
    ibis_catalog: IbisPlanCatalog | None = None
    runtime: NormalizeRuntime | None = None


@dataclass(frozen=True)
class TaskSpec:
    """Specification for a computation task.

    Attributes
    ----------
    name : str
        Stable task identifier.
    output : str
        Output dataset name produced by the task.
    build : Callable[[TaskBuildContext], IbisPlan]
        Builder for the Ibis plan.
    kind : TaskKind
        Execution classification for scheduling.
    cache_policy : CachePolicy
        Cache policy for runtime artifacts.
    metadata : Mapping[str, str]
        Optional metadata tags for observability.
    """

    name: str
    output: str
    build: Callable[[TaskBuildContext], IbisPlan]
    kind: TaskKind = "view"
    cache_policy: CachePolicy = "none"
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class TaskCatalog:
    """Immutable catalog of tasks used for plan compilation."""

    tasks: tuple[TaskSpec, ...]

    def by_name(self) -> Mapping[str, TaskSpec]:
        """Return tasks keyed by name.

        Returns
        -------
        Mapping[str, TaskSpec]
            Mapping of task name to specification.
        """
        return {task.name: task for task in self.tasks}

    def by_output(self) -> Mapping[str, TaskSpec]:
        """Return tasks keyed by output name.

        Returns
        -------
        Mapping[str, TaskSpec]
            Mapping of output dataset name to task spec.
        """
        return {task.output: task for task in self.tasks}

    def names(self) -> tuple[str, ...]:
        """Return task names in catalog order.

        Returns
        -------
        tuple[str, ...]
            Task names.
        """
        return tuple(task.name for task in self.tasks)

    def outputs(self) -> tuple[str, ...]:
        """Return output names in catalog order.

        Returns
        -------
        tuple[str, ...]
            Output dataset names.
        """
        return tuple(task.output for task in self.tasks)


__all__ = [
    "CachePolicy",
    "TaskBuildContext",
    "TaskCatalog",
    "TaskKind",
    "TaskSpec",
]
