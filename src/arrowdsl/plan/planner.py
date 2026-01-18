"""Planner and segmentation for ArrowDSL fallback execution."""

from __future__ import annotations

from dataclasses import dataclass

from arrowdsl.core.context import DeterminismTier, ExecutionContext
from arrowdsl.ir.plan import OpNode, PlanIR
from arrowdsl.ops.catalog import Lane, OpDef

LANE_PRIORITY: tuple[Lane, ...] = ("datafusion", "acero", "kernel")


@dataclass(frozen=True)
class Segment:
    """A contiguous sequence of operations executed in one lane."""

    ops: tuple[OpNode, ...]
    lane: Lane


@dataclass(frozen=True)
class SegmentPlan:
    """Segmented plan with pipeline breaker metadata."""

    segments: tuple[Segment, ...]
    pipeline_breakers: tuple[str, ...]


def select_lane(op_def: OpDef, *, ctx: ExecutionContext) -> Lane:
    """Select the highest-priority supported lane for an op.

    Returns
    -------
    Lane
        Selected execution lane.

    Raises
    ------
    ValueError
        Raised when no supported lane is available.
    """
    if (
        ctx.determinism in {DeterminismTier.CANONICAL, DeterminismTier.STABLE_SET}
        and op_def.name == "order_by"
        and op_def.supports("kernel")
    ):
        return "kernel"
    for lane in LANE_PRIORITY:
        if lane == "datafusion" and ctx.runtime.datafusion is None:
            continue
        if op_def.supports(lane):
            return lane
    msg = f"No supported lane for op {op_def.name!r}."
    raise ValueError(msg)


def segment_plan(
    ir: PlanIR,
    *,
    catalog: dict[str, OpDef],
    ctx: ExecutionContext,
) -> SegmentPlan:
    """Segment a plan into lane-consistent blocks.

    Returns
    -------
    SegmentPlan
        Segmented plan with pipeline breaker metadata.

    Raises
    ------
    ValueError
        Raised when a segment is missing a resolved execution lane.
    """
    segments: list[Segment] = []
    current_ops: list[OpNode] = []
    current_lane: Lane | None = None
    breakers: list[str] = []

    for node in ir.nodes:
        op_def = catalog[node.name]
        lane = select_lane(op_def, ctx=ctx)
        if current_lane is None:
            current_lane = lane
        if lane != current_lane and current_ops:
            if current_lane is None:
                msg = "Segment lane missing during lane transition."
                raise ValueError(msg)
            segments.append(Segment(tuple(current_ops), current_lane))
            current_ops = []
            current_lane = lane
        current_ops.append(node)
        if op_def.pipeline_breaker:
            if current_lane is None:
                msg = "Segment lane missing at pipeline breaker."
                raise ValueError(msg)
            segments.append(Segment(tuple(current_ops), current_lane))
            breakers.append(node.name)
            current_ops = []
            current_lane = None

    if current_ops:
        if current_lane is None:
            fallback_lane: Lane = "kernel"
        else:
            fallback_lane = current_lane
        segments.append(Segment(tuple(current_ops), fallback_lane))

    return SegmentPlan(tuple(segments), tuple(breakers))


__all__ = ["LANE_PRIORITY", "Segment", "SegmentPlan", "segment_plan", "select_lane"]
