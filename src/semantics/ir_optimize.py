"""IR optimization utilities for pruning and cost-aware ordering."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass

from semantics.ir import SemanticIR, SemanticIRJoinGroup, SemanticIRView


@dataclass(frozen=True)
class IRCost:
    """Cost estimates for IR optimization."""

    row_count: int | None = None
    selectivity: float | None = None


def order_join_groups(
    join_groups: Sequence[SemanticIRJoinGroup],
    *,
    costs: Mapping[str, IRCost] | None = None,
) -> tuple[SemanticIRJoinGroup, ...]:
    """Return join groups ordered by a stable heuristic.

    Returns:
    -------
    tuple[SemanticIRJoinGroup, ...]
        Ordered join group specs.
    """

    def _cost_key(group: SemanticIRJoinGroup) -> tuple[object, ...]:
        cost = costs.get(group.name) if costs is not None else None
        row_hint = cost.row_count if cost is not None else None
        sel_hint = cost.selectivity if cost is not None else None
        return (
            row_hint if row_hint is not None else float("inf"),
            -(sel_hint if sel_hint is not None else 0.0),
            len(group.left_on) + len(group.right_on),
            -len(group.relationship_names),
            group.name,
        )

    return tuple(sorted(join_groups, key=_cost_key))


def _view_dependency_map(views: Sequence[SemanticIRView]) -> dict[str, tuple[str, ...]]:
    return {view.name: view.inputs for view in views}


def prune_ir(ir: SemanticIR, *, outputs: Collection[str] | None) -> SemanticIR:
    """Return a pruned IR containing only views needed for outputs.

    Parameters
    ----------
    ir
        Semantic IR to prune.
    outputs
        Output view names to retain. When None, returns the IR unchanged.

    Returns:
    -------
    SemanticIR
        Pruned semantic IR.
    """
    if outputs is None:
        return ir

    view_by_name: Mapping[str, SemanticIRView] = {view.name: view for view in ir.views}
    deps_by_name = _view_dependency_map(ir.views)
    required: set[str] = set()
    stack: list[str] = [name for name in outputs if name in view_by_name]

    while stack:
        name = stack.pop()
        if name in required:
            continue
        required.add(name)
        stack.extend(
            dep for dep in deps_by_name.get(name, ()) if dep in view_by_name and dep not in required
        )

    join_groups = [
        group
        for group in ir.join_groups
        if any(rel in required for rel in group.relationship_names)
    ]
    for group in join_groups:
        required.add(group.name)

    pruned_views = tuple(view for view in ir.views if view.name in required)
    pruned_rows = tuple(row for row in ir.dataset_rows if row.name in required)

    return SemanticIR(
        views=pruned_views,
        dataset_rows=pruned_rows,
        join_groups=tuple(join_groups),
        model_hash=ir.model_hash,
        ir_hash=ir.ir_hash,
    )


__all__ = ["IRCost", "order_join_groups", "prune_ir"]
