"""Task catalog definitions for inference-driven scheduling."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from ibis.backends import BaseBackend

    from arrowdsl.core.execution_context import ExecutionContext
    from datafusion_engine.diagnostics import DiagnosticsRecorder
    from datafusion_engine.execution_facade import DataFusionExecutionFacade
    from datafusion_engine.parameterized_execution import ParameterizedRulepack
    from ibis_engine.catalog import IbisPlanCatalog
    from ibis_engine.plan import IbisPlan
    from normalize.runtime import NormalizeRuntime
    from sqlglot_tools.compat import Expression

TaskKind = Literal["view", "compute", "materialization"]
CachePolicy = Literal["none", "session", "persistent"]

if TYPE_CHECKING:
    PlanBuildResult = IbisPlan | ParameterizedRulepack
else:
    PlanBuildResult = object

@dataclass(frozen=True)
class TaskBuildContext:
    """Context passed to task builders."""

    ctx: ExecutionContext
    backend: BaseBackend
    ibis_catalog: IbisPlanCatalog | None = None
    runtime: NormalizeRuntime | None = None
    facade: DataFusionExecutionFacade | None = None
    diagnostics: DiagnosticsRecorder | None = None


PlanBuilder = Callable[[TaskBuildContext], PlanBuildResult]


@dataclass(frozen=True)
class TaskSpec:
    """Specification for a computation task.

    Attributes
    ----------
    name : str
        Stable task identifier.
    output : str
        Output dataset name produced by the task.
    build : Callable[[TaskBuildContext], IbisPlan | ParameterizedRulepack]
        Builder for the Ibis plan.
    kind : TaskKind
        Execution classification for scheduling.
    priority : int
        Scheduling priority (lower runs earlier).
    cache_policy : CachePolicy
        Cache policy for runtime artifacts.
    metadata : Mapping[str, str]
        Optional metadata tags for observability.
    sqlglot_builder : Callable[[TaskBuildContext], Expression] | None
        Optional SQLGlot builder for AST-first lineage and fingerprints.
    """

    name: str
    output: str
    build: PlanBuilder
    kind: TaskKind = "view"
    priority: int = 100
    cache_policy: CachePolicy = "none"
    metadata: Mapping[str, str] = field(default_factory=dict)
    sqlglot_builder: Callable[[TaskBuildContext], Expression] | None = None


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
