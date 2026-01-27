"""Plan-aware scheduling strategies for Hamilton dynamic execution."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from hamilton.execution import grouping

from relspec.execution_plan import ExecutionPlan

if TYPE_CHECKING:
    from hamilton import node as hamilton_node

    from relspec.schedule_events import TaskScheduleMetadata


@dataclass(frozen=True)
class PlanSchedulingContext:
    """Scheduling metadata derived from a compiled execution plan."""

    schedule_metadata: Mapping[str, TaskScheduleMetadata]
    bottom_level_costs: Mapping[str, float]

    @classmethod
    def from_plan(cls, plan: ExecutionPlan) -> PlanSchedulingContext:
        return cls(
            schedule_metadata=plan.schedule_metadata,
            bottom_level_costs=plan.bottom_level_costs,
        )


class PlanGroupingStrategy(grouping.GroupingStrategy):
    """Group tasks by plan generation and prioritize by criticality."""

    def __init__(self, plan: ExecutionPlan) -> None:
        ctx = PlanSchedulingContext.from_plan(plan)
        self._schedule_metadata = dict(ctx.schedule_metadata)
        self._bottom_level_costs = dict(ctx.bottom_level_costs)
        self._fallback = grouping.GroupNodesByLevel()

    def group_nodes(self, nodes: list[hamilton_node.Node]) -> list[grouping.NodeGroup]:
        nodes_by_generation: dict[int, list[hamilton_node.Node]] = defaultdict(list)
        other_nodes: list[hamilton_node.Node] = []
        for node_ in nodes:
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

    def _generation_sort_key(self, node_: hamilton_node.Node) -> tuple[float, str]:
        bottom_cost = self._bottom_level_costs.get(node_.name, 0.0)
        return (-bottom_cost, node_.name)


def plan_grouping_strategy(plan: ExecutionPlan) -> grouping.GroupingStrategy:
    """Return a plan-aware grouping strategy for dynamic execution."""
    return PlanGroupingStrategy(plan)


__all__ = ["PlanGroupingStrategy", "PlanSchedulingContext", "plan_grouping_strategy"]
