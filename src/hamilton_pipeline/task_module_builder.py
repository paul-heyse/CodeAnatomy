"""Dynamic Hamilton module generation for execution-plan task nodes."""

from __future__ import annotations

import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, Literal

from hamilton.function_modifiers import cache, group, inject, source, tag, value
from hamilton.function_modifiers.dependencies import ParametrizedDependency

from hamilton_pipeline.modules import task_execution
from relspec.execution_plan import ExecutionPlan, priority_for_task
from relspec.runtime_artifacts import TableLike

if TYPE_CHECKING:
    from datafusion_engine.view_graph_registry import ViewNode
    from relspec.schedule_events import TaskScheduleMetadata


@dataclass(frozen=True)
class TaskExecutionModuleOptions:
    """Optional configuration for task execution module generation."""

    module_name: str = "hamilton_pipeline.generated_tasks"


def build_task_execution_module(
    *,
    plan: ExecutionPlan,
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
    dependency_map = plan.dependency_map
    outputs = {node.name: node for node in plan.view_nodes}
    scan_units_by_task = dict(plan.scan_task_units_by_name)
    plan_fingerprints = dict(plan.plan_fingerprints)
    plan_task_signatures = dict(plan.plan_task_signatures)
    schedule_metadata = dict(plan.schedule_metadata)
    for scan_task_name in sorted(scan_units_by_task):
        scan_unit = scan_units_by_task[scan_task_name]
        plan_fingerprint = plan_fingerprints.get(scan_task_name, "")
        plan_task_signature = plan_task_signatures.get(scan_task_name, plan_fingerprint)
        task_spec = TaskNodeSpec(
            name=scan_task_name,
            output=scan_task_name,
            kind="scan",
            priority=priority_for_task(scan_task_name),
            cache_policy="none",
        )
        task_node = _build_task_node(
            TaskNodeContext(
                task=task_spec,
                output_name=scan_task_name,
                dependencies=tuple(dependency_map.get(scan_task_name, ())),
                plan_fingerprint=plan_fingerprint,
                plan_task_signature=plan_task_signature,
                plan_signature=plan.plan_signature,
                schedule_metadata=schedule_metadata.get(scan_task_name),
                scan_unit_key=scan_unit.key,
            )
        )
        task_node.__module__ = resolved.module_name
        module.__dict__[scan_task_name] = task_node
        all_names.append(scan_task_name)
    for output_name, view_node in outputs.items():
        spec = _task_spec_from_view_node(view_node)
        plan_fingerprint = plan_fingerprints.get(spec.name, "")
        plan_task_signature = plan_task_signatures.get(spec.name, plan_fingerprint)
        task_node = _build_task_node(
            TaskNodeContext(
                task=spec,
                output_name=output_name,
                dependencies=tuple(dependency_map.get(output_name, ())),
                plan_fingerprint=plan_fingerprint,
                plan_task_signature=plan_task_signature,
                plan_signature=plan.plan_signature,
                schedule_metadata=schedule_metadata.get(spec.name),
                scan_unit_key=None,
            )
        )
        task_node.__module__ = resolved.module_name
        module.__dict__[output_name] = task_node
        all_names.append(output_name)
    return module


@dataclass(frozen=True)
class TaskNodeContext:
    """Context for generating a single task node."""

    task: TaskNodeSpec
    output_name: str
    dependencies: Sequence[str]
    plan_fingerprint: str
    plan_task_signature: str
    plan_signature: str
    schedule_metadata: TaskScheduleMetadata | None
    scan_unit_key: str | None


def _build_task_node(context: TaskNodeContext) -> Callable[..., TableLike]:
    output_name = context.output_name

    def _execute_task(
        task_execution_inputs: task_execution.TaskExecutionInputs,
        dependencies: list[TableLike],
        plan_signature: str,
        task_spec: task_execution.TaskExecutionSpec,
    ) -> TableLike:
        return task_execution.execute_task_from_catalog(
            inputs=task_execution_inputs,
            dependencies=dependencies,
            plan_signature=plan_signature,
            spec=task_spec,
        )

    _execute_task.__name__ = output_name
    _execute_task.__qualname__ = output_name
    return _decorate_task_node(
        node_fn=_execute_task,
        context=context,
    )


@dataclass(frozen=True)
class TaskNodeSpec:
    """Minimal task metadata derived from a view node."""

    name: str
    output: str
    kind: Literal["view", "scan"]
    priority: int
    cache_policy: str = "none"


def _task_spec_from_view_node(node: ViewNode) -> TaskNodeSpec:
    return TaskNodeSpec(
        name=node.name,
        output=node.name,
        kind="view",
        priority=priority_for_task(node.name),
        cache_policy="none",
    )


def _decorate_task_node(
    *,
    node_fn: Callable[..., TableLike],
    context: TaskNodeContext,
) -> Callable[..., TableLike]:
    task = context.task
    if task.cache_policy in {"session", "persistent"}:
        node_fn = cache(format="delta", behavior="default")(node_fn)
    dependency_sources = tuple(source(dep) for dep in sorted(set(context.dependencies)))
    task_spec = task_execution.TaskExecutionSpec(
        task_name=task.name,
        task_output=context.output_name,
        plan_fingerprint=context.plan_fingerprint,
        plan_task_signature=context.plan_task_signature,
        task_kind=task.kind,
        scan_unit_key=context.scan_unit_key,
    )
    if dependency_sources:
        inject_kwargs: dict[str, ParametrizedDependency] = {
            "dependencies": group(*dependency_sources),
            "plan_signature": source("plan_signature"),
            "task_spec": value(task_spec),
        }
        node_fn = inject(**inject_kwargs)(node_fn)
    else:
        inject_kwargs: dict[str, ParametrizedDependency] = {
            "dependencies": value(()),
            "plan_signature": source("plan_signature"),
            "task_spec": value(task_spec),
        }
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
            plan_signature=context.plan_signature,
            plan_fingerprint=context.plan_fingerprint,
            plan_task_signature=context.plan_task_signature,
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
        plan_signature=context.plan_signature,
        plan_fingerprint=context.plan_fingerprint,
        plan_task_signature=context.plan_task_signature,
    )(node_fn)


__all__ = ["TaskExecutionModuleOptions", "build_task_execution_module"]
