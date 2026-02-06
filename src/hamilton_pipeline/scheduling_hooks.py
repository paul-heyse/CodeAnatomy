"""Plan-aware scheduling strategies for Hamilton dynamic execution."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from hamilton.execution import grouping
from hamilton.lifecycle import api as lifecycle_api

from obs.diagnostics import DiagnosticsCollector
from relspec.execution_plan import ExecutionPlan

if TYPE_CHECKING:
    from hamilton import node as hamilton_node

    from relspec.schedule_events import TaskScheduleMetadata


@runtime_checkable
class _HamiltonNode(Protocol):
    """Structural protocol for Hamilton nodes used by scheduling hooks."""

    @property
    def name(self) -> str:
        """Return the Hamilton node name."""
        ...

    @property
    def tags(self) -> Mapping[str, object]:
        """Return Hamilton node tags."""
        ...


@dataclass(frozen=True)
class PlanSchedulingContext:
    """Scheduling metadata derived from a compiled execution plan."""

    schedule_metadata: Mapping[str, TaskScheduleMetadata]
    bottom_level_costs: Mapping[str, float]
    slack_by_task: Mapping[str, float]
    active_tasks: frozenset[str]
    critical_path_tasks: tuple[str, ...]
    betweenness_centrality: Mapping[str, float]
    dominators: Mapping[str, str | None]
    bridge_task_counts: Mapping[str, int]
    articulation_tasks: frozenset[str]
    plan_signature: str
    reduced_plan_signature: str

    @classmethod
    def from_plan(cls, plan: ExecutionPlan) -> PlanSchedulingContext:
        """Build a scheduling context from a compiled execution plan.

        Returns:
        -------
        PlanSchedulingContext
            Scheduling metadata derived from the plan.
        """
        return cls(
            schedule_metadata=plan.schedule_metadata,
            bottom_level_costs=plan.bottom_level_costs,
            slack_by_task=plan.slack_by_task,
            active_tasks=plan.active_tasks,
            critical_path_tasks=plan.critical_path_task_names,
            betweenness_centrality=plan.diagnostics.betweenness_centrality or {},
            dominators=plan.diagnostics.dominators or {},
            bridge_task_counts=_bridge_task_counts(plan.diagnostics.bridge_edges),
            articulation_tasks=frozenset(plan.diagnostics.articulation_tasks or ()),
            plan_signature=plan.plan_signature,
            reduced_plan_signature=plan.reduced_task_dependency_signature,
        )


class PlanGroupingStrategy(grouping.GroupingStrategy):
    """Group tasks by plan generation and prioritize by criticality."""

    def __init__(self, plan: ExecutionPlan) -> None:
        """__init__."""
        ctx = PlanSchedulingContext.from_plan(plan)
        self._schedule_metadata = dict(ctx.schedule_metadata)
        self._bottom_level_costs = dict(ctx.bottom_level_costs)
        self._slack_by_task = dict(ctx.slack_by_task)
        self._active_tasks = set(ctx.active_tasks)
        self._critical_path_tasks = set(ctx.critical_path_tasks)
        self._centrality = dict(ctx.betweenness_centrality)
        self._bridge_task_counts = dict(ctx.bridge_task_counts)
        self._articulation_tasks = set(ctx.articulation_tasks)
        self._fallback = grouping.GroupNodesByLevel()

    def group_nodes(self, nodes: list[hamilton_node.Node]) -> list[grouping.NodeGroup]:
        """Group nodes by generation with plan-aware ordering.

        Returns:
        -------
        list[grouping.NodeGroup]
            Grouped nodes ordered by plan generations.
        """
        nodes_by_generation: dict[int, list[hamilton_node.Node]] = defaultdict(list)
        other_nodes: list[hamilton_node.Node] = []
        for node_ in nodes:
            task_name = _execution_task_name(node_)
            if task_name is not None and task_name not in self._active_tasks:
                continue
            meta = self._schedule_metadata.get(node_.name)
            if meta is None:
                other_nodes.append(node_)
                continue
            nodes_by_generation[meta.generation_index].append(node_)
        groups = self._fallback.group_nodes(other_nodes) if other_nodes else []
        for generation_index in sorted(nodes_by_generation):
            generation_nodes = nodes_by_generation[generation_index]
            generation_nodes.sort(key=self._generation_sort_key)
            groups.append(
                grouping.NodeGroup(
                    base_id=f"generation_{generation_index}",
                    spawning_task_base_id=None,
                    nodes=generation_nodes,
                    purpose=grouping.NodeGroupPurpose.EXECUTE_BLOCK,
                )
            )
        return groups

    def _generation_sort_key(self, node_: hamilton_node.Node) -> tuple[float, float, float, str]:
        bottom_cost = self._bottom_level_costs.get(node_.name, 0.0)
        critical_boost = 1.0 if node_.name in self._critical_path_tasks else 0.0
        structural_bonus = 0.0
        if node_.name in self._articulation_tasks:
            structural_bonus += 1.0
        if self._bridge_task_counts.get(node_.name, 0) > 0:
            structural_bonus += 0.5
        centrality = self._centrality.get(node_.name, 0.0)
        slack = self._slack_by_task.get(node_.name, 0.0)
        return (
            -(bottom_cost + critical_boost + structural_bonus),
            -float(centrality),
            float(slack),
            node_.name,
        )


def plan_grouping_strategy(plan: ExecutionPlan) -> grouping.GroupingStrategy:
    """Return a plan-aware grouping strategy for dynamic execution.

    Returns:
    -------
    grouping.GroupingStrategy
        Grouping strategy that honors plan ordering and priority.
    """
    return PlanGroupingStrategy(plan)


@dataclass
class PlanTaskSubmissionHook(lifecycle_api.TaskSubmissionHook):
    """Record and optionally enforce plan-native task admission."""

    plan: ExecutionPlan
    diagnostics: DiagnosticsCollector
    enforce_active: bool = True

    def run_before_task_submission(self, **kwargs: object) -> None:
        """Record diagnostics and optionally enforce task admission.

        Args:
            **kwargs: Description.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        run_id = _require_str(kwargs, "run_id")
        task_id = _require_str(kwargs, "task_id")
        nodes = _require_nodes(kwargs)
        inputs = _require_mapping(kwargs, "inputs")
        purpose = kwargs.get("purpose")
        execution_task_names = _execution_task_names(nodes)
        active_tasks = _active_task_names(self.plan, inputs=inputs)
        admitted = tuple(name for name in execution_task_names if name in active_tasks)
        rejected = tuple(name for name in execution_task_names if name not in active_tasks)
        unknown = tuple(name for name in execution_task_names if name not in self.plan.active_tasks)
        task_facts = [
            _task_fact(self.plan, name) for name in admitted if name in self.plan.schedule_metadata
        ]
        payload = {
            "run_id": run_id,
            "task_id": task_id,
            "purpose": _purpose_label(purpose),
            "plan_signature": self.plan.plan_signature,
            "reduced_plan_signature": (self.plan.reduced_task_dependency_signature),
            "plan_task_count": len(self.plan.active_tasks),
            "execution_task_names": list(execution_task_names),
            "admitted_tasks": list(admitted),
            "rejected_tasks": list(rejected),
            "unknown_tasks": list(unknown),
            "critical_path_tasks": list(self.plan.critical_path_task_names),
            "task_facts": task_facts,
            "reduction_edge_count": self.plan.reduction_edge_count,
            "reduction_removed_edge_count": self.plan.reduction_removed_edge_count,
        }
        self.diagnostics.record_events("hamilton_task_submission_v1", [payload])
        if self.enforce_active and rejected:
            msg = f"Plan admission control rejected tasks not in the active set: {list(rejected)}."
            raise ValueError(msg)


@dataclass
class PlanTaskGroupingHook(lifecycle_api.TaskGroupingHook):
    """Record task grouping and expansion diagnostics."""

    plan: ExecutionPlan
    diagnostics: DiagnosticsCollector

    def run_after_task_grouping(
        self,
        *,
        run_id: str,
        task_ids: list[str],
        **future_kwargs: object,
    ) -> None:
        """Record diagnostics for task grouping events.

        Parameters
        ----------
        run_id : str
            Hamilton run identifier.
        task_ids : list[str]
            Task group identifiers produced by grouping.
        **future_kwargs : object
            Extra hook arguments (unused).
        """
        _ = future_kwargs
        payload = {
            "run_id": run_id,
            "plan_signature": self.plan.plan_signature,
            "reduced_plan_signature": self.plan.reduced_task_dependency_signature,
            "task_ids": list(task_ids),
            "task_count": len(task_ids),
        }
        self.diagnostics.record_events("hamilton_task_grouping_v1", [payload])

    def run_after_task_expansion(
        self,
        *,
        run_id: str,
        task_id: str,
        parameters: dict[str, object],
        **future_kwargs: object,
    ) -> None:
        """Record diagnostics for task expansion events.

        Parameters
        ----------
        run_id : str
            Hamilton run identifier.
        task_id : str
            Task group identifier.
        parameters : dict[str, object]
            Parameter bindings for the expanded task.
        **future_kwargs : object
            Extra hook arguments (unused).
        """
        _ = future_kwargs
        payload = {
            "run_id": run_id,
            "plan_signature": self.plan.plan_signature,
            "task_id": task_id,
            "parameter_keys": sorted(parameters),
            "parameter_count": len(parameters),
        }
        self.diagnostics.record_events("hamilton_task_expansion_v1", [payload])


def plan_task_submission_hook(
    plan: ExecutionPlan,
    diagnostics: DiagnosticsCollector,
    *,
    enforce_active: bool,
) -> lifecycle_api.TaskSubmissionHook:
    """Return the plan-native task submission hook.

    Returns:
    -------
    lifecycle_api.TaskSubmissionHook
        Hook that records task admission diagnostics.
    """
    return PlanTaskSubmissionHook(
        plan=plan,
        diagnostics=diagnostics,
        enforce_active=enforce_active,
    )


def plan_task_grouping_hook(
    plan: ExecutionPlan,
    diagnostics: DiagnosticsCollector,
) -> lifecycle_api.TaskGroupingHook:
    """Return the plan-native task grouping hook.

    Returns:
    -------
    lifecycle_api.TaskGroupingHook
        Hook that records task grouping diagnostics.
    """
    return PlanTaskGroupingHook(plan=plan, diagnostics=diagnostics)


def _execution_task_name(node_: _HamiltonNode) -> str | None:
    tags = node_.tags
    layer = tags.get("layer")
    kind = tags.get("kind")
    if layer == "execution" and kind == "task":
        return node_.name
    return None


def _execution_task_names(nodes: Sequence[_HamiltonNode]) -> tuple[str, ...]:
    names = [task_name for node_ in nodes if (task_name := _execution_task_name(node_))]
    return tuple(sorted(set(names)))


def _active_task_names(
    plan: ExecutionPlan,
    *,
    inputs: Mapping[str, object],
) -> frozenset[str]:
    active_value = inputs.get("active_task_names")
    active_iterable: Sequence[object] = ()
    if isinstance(active_value, (set, frozenset)):
        active_iterable = tuple(active_value)
    elif isinstance(active_value, Sequence) and not isinstance(
        active_value,
        (str, bytes),
    ):
        active_iterable = active_value
    active = {name for name in active_iterable if isinstance(name, str)}
    if active:
        return frozenset(active & set(plan.active_tasks))
    return plan.active_tasks


def _task_fact(plan: ExecutionPlan, name: str) -> dict[str, object]:
    schedule_meta = plan.schedule_metadata.get(name)
    bridge_counts = _bridge_task_counts(plan.diagnostics.bridge_edges)
    return {
        "task_name": name,
        "bottom_level_cost": plan.bottom_level_costs.get(name, 0.0),
        "slack": plan.slack_by_task.get(name, 0.0),
        "on_critical_path": name in set(plan.critical_path_task_names),
        "betweenness_centrality": (plan.diagnostics.betweenness_centrality or {}).get(name, 0.0),
        "immediate_dominator": (plan.diagnostics.dominators or {}).get(name),
        "bridge_edge_count": bridge_counts.get(name, 0),
        "is_bridge_task": bridge_counts.get(name, 0) > 0,
        "is_articulation_task": name in set(plan.diagnostics.articulation_tasks or ()),
        "generation_index": (schedule_meta.generation_index if schedule_meta is not None else None),
        "schedule_index": (schedule_meta.schedule_index if schedule_meta is not None else None),
    }


def _require_str(kwargs: Mapping[str, object], key: str) -> str:
    value = kwargs.get(key)
    if not isinstance(value, str):
        msg = f"Task submission hook expected a string for {key!r}."
        raise TypeError(msg)
    return value


def _require_mapping(kwargs: Mapping[str, object], key: str) -> dict[str, object]:
    value = kwargs.get(key)
    if not isinstance(value, Mapping):
        msg = f"Task submission hook expected a mapping for {key!r}."
        raise TypeError(msg)
    return {
        inner_key: inner_value
        for inner_key, inner_value in value.items()
        if isinstance(inner_key, str)
    }


def _require_nodes(kwargs: Mapping[str, object]) -> list[_HamiltonNode]:
    nodes_value = kwargs.get("nodes")
    if not isinstance(nodes_value, Sequence) or isinstance(nodes_value, (str, bytes)):
        msg = "Task submission hook expected a sequence of Hamilton nodes."
        raise TypeError(msg)
    nodes: list[_HamiltonNode] = []
    for node_ in nodes_value:
        if not isinstance(node_, _HamiltonNode):
            msg = "Task submission hook received an invalid Hamilton node payload."
            raise TypeError(msg)
        nodes.append(node_)
    return nodes


def _purpose_label(purpose: object) -> str:
    value = getattr(purpose, "value", None)
    if isinstance(value, str) and value:
        return value
    if purpose is None:
        return "unknown"
    return str(purpose)


def _bridge_task_counts(bridge_edges: Sequence[tuple[str, str]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for left, right in bridge_edges:
        counts[left] = counts.get(left, 0) + 1
        counts[right] = counts.get(right, 0) + 1
    return counts


__all__ = [
    "PlanGroupingStrategy",
    "PlanSchedulingContext",
    "PlanTaskGroupingHook",
    "PlanTaskSubmissionHook",
    "plan_grouping_strategy",
    "plan_task_grouping_hook",
    "plan_task_submission_hook",
]
