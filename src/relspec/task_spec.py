"""Task specification for view vs compute classification.

This module provides data models for classifying rule tasks by their
execution characteristics (view, compute, materialization) and cache policies.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from relspec.rustworkx_graph import RuleGraph

TaskKind = Literal["view", "compute", "materialization"]
CachePolicy = Literal["none", "session", "persistent"]

# Threshold for classifying tasks with many inputs as materializations
_MATERIALIZATION_INPUT_THRESHOLD: int = 3


@dataclass(frozen=True)
class TaskSpec:
    """Specification for a rule execution task.

    Classifies rules by their execution characteristics to enable
    optimal scheduling and resource allocation.

    Attributes
    ----------
    name : str
        Task/rule name.
    kind : TaskKind
        Task classification:
        - "view": Cheap, can be recomputed (registered as database view)
        - "compute": Moderate cost, benefits from caching
        - "materialization": Expensive, should be persisted
    output : str
        Output dataset name produced by the task.
    cache_policy : CachePolicy
        Caching strategy:
        - "none": No caching, recompute on demand
        - "session": Cache within session lifetime
        - "persistent": Cache across sessions
    plan_fingerprint : str | None
        Stable hash for cache invalidation.
    estimated_cost : float | None
        Estimated relative cost (0.0-1.0 scale, optional).
    row_estimate : int | None
        Estimated output row count (optional).
    metadata : Mapping[str, str]
        Additional task metadata.
    """

    name: str
    kind: TaskKind
    output: str
    cache_policy: CachePolicy = "none"
    plan_fingerprint: str | None = None
    estimated_cost: float | None = None
    row_estimate: int | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class TaskClassification:
    """Summary of task classifications for a rule set.

    Attributes
    ----------
    views : tuple[str, ...]
        Tasks classified as views.
    computes : tuple[str, ...]
        Tasks classified as compute.
    materializations : tuple[str, ...]
        Tasks classified as materializations.
    total_tasks : int
        Total number of tasks.
    """

    views: tuple[str, ...] = ()
    computes: tuple[str, ...] = ()
    materializations: tuple[str, ...] = ()
    total_tasks: int = 0


@dataclass(frozen=True)
class TaskClassificationRequest:
    """Inputs for classifying a rule task."""

    rule_name: str
    output: str
    inputs: tuple[str, ...]
    plan_fingerprint: str | None = None


@dataclass(frozen=True)
class TaskClassificationOverrides:
    """Optional overrides for task classification."""

    force_materialize: set[str] | None = None
    force_view: set[str] | None = None


def classify_task(
    request: TaskClassificationRequest,
    *,
    overrides: TaskClassificationOverrides | None = None,
) -> TaskSpec:
    """Classify a rule as view, compute, or materialization.

    Classification heuristics:
    1. Rules in force_materialize are always materializations
    2. Rules in force_view are always views
    3. Rules with no inputs are views (source tables)
    4. Rules with many inputs (>3) are materializations
    5. Default is compute

    Parameters
    ----------
    request : TaskClassificationRequest
        Rule name, outputs, inputs, and optional plan fingerprint.
    overrides : TaskClassificationOverrides | None
        Optional force overrides for classification.

    Returns
    -------
    TaskSpec
        Task specification with classification.
    """
    resolved_overrides = overrides or TaskClassificationOverrides()
    force_mat = resolved_overrides.force_materialize or set()
    force_v = resolved_overrides.force_view or set()

    # Explicit overrides
    if request.rule_name in force_mat:
        return TaskSpec(
            name=request.rule_name,
            kind="materialization",
            output=request.output,
            cache_policy="persistent",
            plan_fingerprint=request.plan_fingerprint,
        )

    if request.rule_name in force_v:
        return TaskSpec(
            name=request.rule_name,
            kind="view",
            output=request.output,
            cache_policy="none",
            plan_fingerprint=request.plan_fingerprint,
        )

    # Heuristics
    if len(request.inputs) == 0:
        # Source tables are views
        return TaskSpec(
            name=request.rule_name,
            kind="view",
            output=request.output,
            cache_policy="none",
            plan_fingerprint=request.plan_fingerprint,
        )

    if len(request.inputs) > _MATERIALIZATION_INPUT_THRESHOLD:
        # Complex joins benefit from materialization
        return TaskSpec(
            name=request.rule_name,
            kind="materialization",
            output=request.output,
            cache_policy="session",
            plan_fingerprint=request.plan_fingerprint,
        )

    # Default to compute
    return TaskSpec(
        name=request.rule_name,
        kind="compute",
        output=request.output,
        cache_policy="session",
        plan_fingerprint=request.plan_fingerprint,
    )


def classify_tasks_from_graph(
    graph: RuleGraph,
    *,
    fingerprints: Mapping[str, str] | None = None,
    force_materialize: set[str] | None = None,
    force_view: set[str] | None = None,
) -> Mapping[str, TaskSpec]:
    """Classify all tasks in a rule graph.

    Parameters
    ----------
    graph : RuleGraph
        The rule graph to classify.
    fingerprints : Mapping[str, str] | None
        Plan fingerprints keyed by rule name.
    force_materialize : set[str] | None
        Rules that must be materialized.
    force_view : set[str] | None
        Rules that must be views.

    Returns
    -------
    Mapping[str, TaskSpec]
        Task specifications keyed by rule name.
    """
    from relspec.rustworkx_graph import GraphNode, RuleNode

    fps = fingerprints or {}
    tasks: dict[str, TaskSpec] = {}

    for rule_name, rule_idx in graph.rule_idx.items():
        node = graph.graph[rule_idx]
        if not isinstance(node, GraphNode) or not isinstance(node.payload, RuleNode):
            continue

        payload = node.payload
        tasks[rule_name] = classify_task(
            TaskClassificationRequest(
                rule_name=rule_name,
                output=payload.output,
                inputs=payload.inputs,
                plan_fingerprint=fps.get(rule_name),
            ),
            overrides=TaskClassificationOverrides(
                force_materialize=force_materialize,
                force_view=force_view,
            ),
        )

    return tasks


def summarize_task_classifications(
    tasks: Mapping[str, TaskSpec],
) -> TaskClassification:
    """Summarize task classifications.

    Parameters
    ----------
    tasks : Mapping[str, TaskSpec]
        Task specifications keyed by name.

    Returns
    -------
    TaskClassification
        Summary of classifications.
    """
    views: list[str] = []
    computes: list[str] = []
    materializations: list[str] = []

    for name, spec in tasks.items():
        if spec.kind == "view":
            views.append(name)
        elif spec.kind == "compute":
            computes.append(name)
        else:
            materializations.append(name)

    return TaskClassification(
        views=tuple(sorted(views)),
        computes=tuple(sorted(computes)),
        materializations=tuple(sorted(materializations)),
        total_tasks=len(tasks),
    )


def tasks_requiring_materialization(
    tasks: Sequence[TaskSpec],
) -> tuple[str, ...]:
    """Return task names that require materialization.

    Parameters
    ----------
    tasks : Sequence[TaskSpec]
        Task specifications.

    Returns
    -------
    tuple[str, ...]
        Names of tasks requiring materialization.
    """
    return tuple(
        sorted(t.name for t in tasks if t.kind == "materialization")
    )


def tasks_with_cache_policy(
    tasks: Sequence[TaskSpec],
    policy: CachePolicy,
) -> tuple[str, ...]:
    """Return task names with a specific cache policy.

    Parameters
    ----------
    tasks : Sequence[TaskSpec]
        Task specifications.
    policy : CachePolicy
        Cache policy to filter by.

    Returns
    -------
    tuple[str, ...]
        Names of tasks with the specified cache policy.
    """
    return tuple(
        sorted(t.name for t in tasks if t.cache_policy == policy)
    )


__all__ = [
    "CachePolicy",
    "TaskClassification",
    "TaskClassificationOverrides",
    "TaskClassificationRequest",
    "TaskKind",
    "TaskSpec",
    "classify_task",
    "classify_tasks_from_graph",
    "summarize_task_classifications",
    "tasks_requiring_materialization",
    "tasks_with_cache_policy",
]
