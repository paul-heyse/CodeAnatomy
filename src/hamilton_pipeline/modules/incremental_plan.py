"""Hamilton nodes for incremental plan fingerprinting."""

from __future__ import annotations

from typing import TYPE_CHECKING

from hamilton.function_modifiers import tag

from datafusion_engine.semantic_diff import ChangeCategory, RebuildPolicy
from incremental.delta_context import DeltaAccessContext
from incremental.plan_fingerprints import read_plan_snapshots, write_plan_snapshots
from incremental.runtime import IncrementalRuntime
from incremental.state_store import StateStore
from incremental.types import IncrementalConfig
from relspec.incremental import diff_plan_snapshots, view_snapshot_map

if TYPE_CHECKING:
    from arrowdsl.core.execution_context import ExecutionContext
    from datafusion_engine.view_graph_registry import ViewNode
    from ibis_engine.execution import IbisExecutionContext
    from relspec.incremental import IncrementalDiff


@tag(layer="incremental", artifact="incremental_plan_diff", kind="mapping")
def incremental_plan_diff(
    view_nodes: tuple[ViewNode, ...],
    incremental_config: IncrementalConfig,
    ctx: ExecutionContext,
    ibis_execution: IbisExecutionContext,
) -> IncrementalDiff | None:
    """Compute incremental plan diff and persist current fingerprints.

    Returns
    -------
    IncrementalDiff | None
        Diff summary when incremental is enabled, otherwise None.
    """
    if not incremental_config.enabled or incremental_config.state_dir is None:
        return None
    try:
        runtime = IncrementalRuntime.build(ctx=ctx)
    except ValueError:
        return None
    state_store = StateStore(root=incremental_config.state_dir)
    context = DeltaAccessContext(runtime=runtime)
    previous = read_plan_snapshots(state_store, context=context)
    current = view_snapshot_map(view_nodes)
    diff = diff_plan_snapshots(previous, current)
    _record_plan_diff(diff, ctx=ctx, total_tasks=len(current))
    write_plan_snapshots(state_store, current, execution=ibis_execution)
    return diff


def _record_plan_diff(
    diff: IncrementalDiff,
    *,
    ctx: ExecutionContext,
    total_tasks: int,
) -> None:
    profile = ctx.runtime.datafusion
    if profile is None:
        return
    payload: dict[str, object] = {
        "total_tasks": total_tasks,
        "changed_tasks": list(diff.changed_tasks),
        "added_tasks": list(diff.added_tasks),
        "removed_tasks": list(diff.removed_tasks),
        "unchanged_tasks": list(diff.unchanged_tasks),
        "changed_count": len(diff.changed_tasks),
        "added_count": len(diff.added_tasks),
        "removed_count": len(diff.removed_tasks),
        "unchanged_count": len(diff.unchanged_tasks),
    }
    if diff.semantic_changes:
        payload["semantic_changes"] = [
            {
                "task_name": name,
                "overall_category": change.overall_category.name,
                "breaking": change.is_breaking(),
                "rebuild_needed": change.requires_rebuild(RebuildPolicy.CONSERVATIVE),
                "summary": change.summary(),
                "change_categories": [
                    item.category.name
                    for item in change.changes
                    if item.category != ChangeCategory.NONE
                ],
            }
            for name, change in diff.semantic_changes.items()
        ]
    from datafusion_engine.diagnostics import record_artifact

    record_artifact(profile, "incremental_plan_diff_v1", payload)


__all__ = ["incremental_plan_diff"]
