"""Dynamic Hamilton module generation for inferred task execution."""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import ModuleType

from hamilton.function_modifiers import cache, group, inject, source, tag, value
from hamilton.function_modifiers.dependencies import ParametrizedDependency

from hamilton_pipeline.modules import task_execution
from relspec.runtime_artifacts import TableLike
from relspec.schedule_events import TaskScheduleMetadata
from relspec.task_catalog import TaskCatalog, TaskSpec


@dataclass(frozen=True)
class TaskExecutionModuleOptions:
    """Optional configuration for task execution module generation."""

    plan_fingerprints: Mapping[str, str] | None = None
    schedule_metadata: Mapping[str, TaskScheduleMetadata] | None = None
    use_generation_gate: bool = False
    module_name: str = "hamilton_pipeline.generated_tasks"


def build_task_execution_module(
    *,
    dependency_map: Mapping[str, Sequence[str]],
    task_catalog: TaskCatalog,
    options: TaskExecutionModuleOptions | None = None,
) -> ModuleType:
    """Build a Hamilton module with per-task execution nodes.

    Returns
    -------
    ModuleType
        Module containing dynamically generated task output nodes.
    """
    resolved = options or TaskExecutionModuleOptions()
    module = ModuleType(resolved.module_name)
    sys.modules[resolved.module_name] = module
    all_names: list[str] = []
    module.__dict__["__all__"] = all_names
    outputs = {task.output: task for task in task_catalog.tasks}
    plan_fingerprints = dict(resolved.plan_fingerprints or {})
    schedule_metadata = dict(resolved.schedule_metadata or {})
    for output_name, task in outputs.items():
        node = _build_task_node(
            TaskNodeContext(
                task=task,
                output_name=output_name,
                dependencies=tuple(dependency_map.get(output_name, ())),
                plan_fingerprint=plan_fingerprints.get(task.name),
                schedule_metadata=schedule_metadata.get(task.name),
                use_generation_gate=resolved.use_generation_gate,
            )
        )
        node.__module__ = resolved.module_name
        module.__dict__[output_name] = node
        all_names.append(output_name)
    return module


@dataclass(frozen=True)
class TaskNodeContext:
    """Context for generating a single task node."""

    task: TaskSpec
    output_name: str
    dependencies: Sequence[str]
    plan_fingerprint: str | None
    schedule_metadata: TaskScheduleMetadata | None
    use_generation_gate: bool


def _build_task_node(context: TaskNodeContext) -> Callable[..., TableLike]:
    output_name = context.output_name

    def _execute_task_with_gate(
        task_execution_inputs: task_execution.TaskExecutionInputs,
        dependencies: list[TableLike],
        task_name: str,
        task_output: str,
        generation_gate: int,
    ) -> TableLike:
        _ = generation_gate
        return task_execution.execute_task_from_catalog(
            inputs=task_execution_inputs,
            task_name=task_name,
            task_output=task_output,
            dependencies=dependencies,
        )

    def _execute_task_without_gate(
        task_execution_inputs: task_execution.TaskExecutionInputs,
        dependencies: list[TableLike],
        task_name: str,
        task_output: str,
    ) -> TableLike:
        return task_execution.execute_task_from_catalog(
            inputs=task_execution_inputs,
            task_name=task_name,
            task_output=task_output,
            dependencies=dependencies,
        )

    node_fn = _execute_task_with_gate if context.use_generation_gate else _execute_task_without_gate
    node_fn.__name__ = output_name
    node_fn.__qualname__ = output_name
    return _decorate_task_node(
        node_fn=node_fn,
        context=context,
    )


def _decorate_task_node(
    *,
    node_fn: Callable[..., TableLike],
    context: TaskNodeContext,
) -> Callable[..., TableLike]:
    task = context.task
    if task.cache_policy in {"session", "persistent"}:
        node_fn = cache(format="parquet", behavior="default")(node_fn)
    dependency_sources = tuple(source(dep) for dep in sorted(set(context.dependencies)))
    generation_gate = context.use_generation_gate
    if dependency_sources:
        inject_kwargs: dict[str, ParametrizedDependency] = {
            "dependencies": group(*dependency_sources),
            "task_name": value(task.name),
            "task_output": value(context.output_name),
        }
        if generation_gate:
            inject_kwargs["generation_gate"] = source("generation_execution_gate")
        node_fn = inject(**inject_kwargs)(node_fn)
    else:
        inject_kwargs: dict[str, ParametrizedDependency] = {
            "dependencies": value(()),
            "task_name": value(task.name),
            "task_output": value(context.output_name),
        }
        if generation_gate:
            inject_kwargs["generation_gate"] = source("generation_execution_gate")
        node_fn = inject(**inject_kwargs)(node_fn)
    schedule_tags: dict[str, str] = {}
    schedule_metadata = context.schedule_metadata
    if schedule_metadata is not None:
        schedule_tags = {
            "schedule_index": str(schedule_metadata.schedule_index),
            "generation_index": str(schedule_metadata.generation_index),
            "generation_order": str(schedule_metadata.generation_order),
            "generation_size": str(schedule_metadata.generation_size),
        }
    if schedule_tags:
        return tag(
            layer="execution",
            artifact=context.output_name,
            kind="task",
            task_name=task.name,
            output=context.output_name,
            task_kind=task.kind,
            cache_policy=task.cache_policy,
            priority=str(task.priority),
            plan_fingerprint=context.plan_fingerprint or "",
            schedule_index=schedule_tags["schedule_index"],
            generation_index=schedule_tags["generation_index"],
            generation_order=schedule_tags["generation_order"],
            generation_size=schedule_tags["generation_size"],
        )(node_fn)
    return tag(
        layer="execution",
        artifact=context.output_name,
        kind="task",
        task_name=task.name,
        output=context.output_name,
        task_kind=task.kind,
        cache_policy=task.cache_policy,
        priority=str(task.priority),
        plan_fingerprint=context.plan_fingerprint or "",
    )(node_fn)


__all__ = ["TaskExecutionModuleOptions", "build_task_execution_module"]
