"""Task registry for discovery and catalog construction."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from relspec.task_catalog import TaskCatalog, TaskSpec


@dataclass
class TaskRegistry:
    """Mutable registry for assembling task catalogs."""

    tasks: dict[str, TaskSpec] = field(default_factory=dict)

    def register(self, task: TaskSpec) -> None:
        """Register a task in the registry.

        Parameters
        ----------
        task : TaskSpec
            Task specification to register.
        """
        self.tasks[task.name] = task

    def register_many(self, tasks: Iterable[TaskSpec]) -> None:
        """Register multiple tasks in the registry.

        Parameters
        ----------
        tasks : Iterable[TaskSpec]
            Tasks to register.
        """
        for task in tasks:
            self.register(task)

    def catalog(self) -> TaskCatalog:
        """Return an immutable task catalog.

        Returns
        -------
        TaskCatalog
            Catalog constructed from registered tasks.
        """
        return TaskCatalog(tasks=tuple(self.tasks.values()))


__all__ = ["TaskRegistry"]
