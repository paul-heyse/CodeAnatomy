"""Dynamic Hamilton module generation for execution-plan task nodes."""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, Literal

from hamilton.function_modifiers import cache, group, inject, source, value
from hamilton.function_modifiers.dependencies import ParametrizedDependency

from hamilton_pipeline.modules import task_execution
from hamilton_pipeline.tag_policy import TagPolicy, apply_tag
from relspec.execution_plan import ExecutionPlan, priority_for_task
from relspec.runtime_artifacts import TableLike

if TYPE_CHECKING:
    from datafusion_engine.views.graph import ViewNode
    from relspec.schedule_events import TaskScheduleMetadata


@dataclass(frozen=True)
class TaskExecutionModuleOptions:
    """Optional configuration for task execution module generation."""

    module_name: str = "hamilton_pipeline.generated_tasks"


@dataclass(frozen=True)
class _PlanTaskContext:
    dependency_map: Mapping[str, tuple[str, ...]]
    plan_fingerprints: Mapping[str, str]
    plan_task_signatures: Mapping[str, str]
    schedule_metadata: Mapping[str, TaskScheduleMetadata]
    task_costs: Mapping[str, float]
    bottom_level_costs: Mapping[str, float]
    slack_by_task: Mapping[str, float]
    critical_path_tasks: frozenset[str]
    betweenness_centrality: Mapping[str, float]
    dominators: Mapping[str, str | None]
    bridge_task_counts: Mapping[str, int]
    articulation_tasks: frozenset[str]
    plan_signature: str

    @classmethod
    def from_plan(cls, plan: ExecutionPlan) -> _PlanTaskContext:
        bridge_task_counts = _bridge_task_counts(plan.diagnostics.bridge_edges)
        return cls(
            dependency_map=plan.dependency_map,
            plan_fingerprints=plan.plan_fingerprints,
            plan_task_signatures=plan.plan_task_signatures,
            schedule_metadata=plan.schedule_metadata,
            task_costs=plan.task_costs,
            bottom_level_costs=plan.bottom_level_costs,
            slack_by_task=plan.slack_by_task,
            critical_path_tasks=frozenset(plan.critical_path_task_names),
            betweenness_centrality=plan.diagnostics.betweenness_centrality or {},
            dominators=plan.diagnostics.dominators or {},
            bridge_task_counts=bridge_task_counts,
            articulation_tasks=frozenset(plan.diagnostics.articulation_tasks or ()),
            plan_signature=plan.plan_signature,
        )

    def build_context(
        self,
        *,
        task: TaskNodeSpec,
        output_name: str,
        dependency_key: str,
        scan_unit_key: str | None,
    ) -> TaskNodeContext:
        plan_fingerprint = self.plan_fingerprints.get(task.name, "")
        plan_task_signature = self.plan_task_signatures.get(task.name, plan_fingerprint)
        task_cost = float(self.task_costs.get(task.name, task.priority))
        bottom_cost = float(self.bottom_level_costs.get(task.name, task_cost))
        slack = float(self.slack_by_task.get(task.name, 0.0))
        on_critical_path = task.name in self.critical_path_tasks
        centrality = float(self.betweenness_centrality.get(task.name, 0.0))
        dominator = self.dominators.get(task.name)
        bridge_edge_count = int(self.bridge_task_counts.get(task.name, 0))
        is_articulation = task.name in self.articulation_tasks
        return TaskNodeContext(
            task=task,
            output_name=output_name,
            dependencies=tuple(self.dependency_map.get(dependency_key, ())),
            plan_fingerprint=plan_fingerprint,
            plan_task_signature=plan_task_signature,
            plan_signature=self.plan_signature,
            schedule_metadata=self.schedule_metadata.get(task.name),
            scan_unit_key=scan_unit_key,
            task_cost=task_cost,
            bottom_level_cost=bottom_cost,
            slack=slack,
            on_critical_path=on_critical_path,
            betweenness_centrality=centrality,
            immediate_dominator=dominator,
            bridge_edge_count=bridge_edge_count,
            is_articulation_task=is_articulation,
        )


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
    plan_context = _PlanTaskContext.from_plan(plan)
    outputs = {node.name: node for node in plan.view_nodes}
    scan_units_by_task = dict(plan.scan_task_units_by_name)
    extract_task_names = tuple(
        name
        for name in sorted(plan.active_tasks)
        if name not in outputs and name not in scan_units_by_task
    )
    for scan_task_name in sorted(scan_units_by_task):
        scan_unit = scan_units_by_task[scan_task_name]
        task_spec = TaskNodeSpec(
            name=scan_task_name,
            output=scan_task_name,
            kind="scan",
            priority=priority_for_task(scan_task_name),
            cache_policy="none",
        )
        task_node = _build_task_node(
            plan_context.build_context(
                task=task_spec,
                output_name=scan_task_name,
                dependency_key=scan_task_name,
                scan_unit_key=scan_unit.key,
            )
        )
        task_node.__module__ = resolved.module_name
        module.__dict__[scan_task_name] = task_node
        all_names.append(scan_task_name)
    for extract_task_name in extract_task_names:
        task_spec = TaskNodeSpec(
            name=extract_task_name,
            output=extract_task_name,
            kind="extract",
            priority=priority_for_task(extract_task_name),
            cache_policy="none",
        )
        task_node = _build_task_node(
            plan_context.build_context(
                task=task_spec,
                output_name=extract_task_name,
                dependency_key=extract_task_name,
                scan_unit_key=None,
            )
        )
        task_node.__module__ = resolved.module_name
        module.__dict__[extract_task_name] = task_node
        all_names.append(extract_task_name)
    for output_name, view_node in outputs.items():
        spec = _task_spec_from_view_node(view_node)
        task_node = _build_task_node(
            plan_context.build_context(
                task=spec,
                output_name=output_name,
                dependency_key=output_name,
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
    task_cost: float
    bottom_level_cost: float
    slack: float
    on_critical_path: bool
    betweenness_centrality: float
    immediate_dominator: str | None
    bridge_edge_count: int
    is_articulation_task: bool


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
    kind: Literal["view", "scan", "extract"]
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
    tag_payload: dict[str, str] = {
        "task_name": task.name,
        "output": context.output_name,
        "task_kind": task.kind,
        "cache_policy": task.cache_policy,
        "priority": str(task.priority),
        "task_cost": str(context.task_cost),
        "bottom_level_cost": str(context.bottom_level_cost),
        "slack": str(context.slack),
        "on_critical_path": str(context.on_critical_path),
        "betweenness_centrality": str(context.betweenness_centrality),
        "immediate_dominator": str(context.immediate_dominator or ""),
        "bridge_edge_count": str(context.bridge_edge_count),
        "is_bridge_task": str(context.bridge_edge_count > 0),
        "is_articulation_task": str(context.is_articulation_task),
        "plan_signature": context.plan_signature,
        "plan_fingerprint": context.plan_fingerprint,
        "plan_task_signature": context.plan_task_signature,
    }
    if schedule_tags:
        tag_payload.update(schedule_tags)
    return apply_tag(
        TagPolicy(
            layer="execution",
            kind="task",
            artifact=context.output_name,
            extra_tags=tag_payload,
        )
    )(node_fn)


def _bridge_task_counts(bridge_edges: Sequence[tuple[str, str]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for left, right in bridge_edges:
        counts[left] = counts.get(left, 0) + 1
        counts[right] = counts.get(right, 0) + 1
    return counts


__all__ = ["TaskExecutionModuleOptions", "build_task_execution_module"]
