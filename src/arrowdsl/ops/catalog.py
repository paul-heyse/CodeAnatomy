"""Operation catalog and metadata for ArrowDSL fallback planning."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from arrowdsl.core.context import OrderingEffect

RowCountEffect = Literal["preserve", "reduce", "expand", "unknown"]
Lane = Literal["datafusion", "acero", "kernel"]


@dataclass(frozen=True)
class OpDef:
    """Operation definition with ordering and lane support metadata."""

    name: str
    ordering_effect: OrderingEffect
    pipeline_breaker: bool
    row_count_effect: RowCountEffect
    lanes: frozenset[Lane]
    acero_node: str | None = None
    kernel_name: str | None = None

    def supports(self, lane: Lane) -> bool:
        """Return whether this op supports the requested lane.

        Returns
        -------
        bool
            ``True`` when the lane is supported.
        """
        return lane in self.lanes


OP_CATALOG: dict[str, OpDef] = {
    "scan": OpDef(
        name="scan",
        ordering_effect=OrderingEffect.IMPLICIT,
        pipeline_breaker=False,
        row_count_effect="preserve",
        lanes=frozenset({"datafusion", "acero"}),
        acero_node="scan",
    ),
    "table_source": OpDef(
        name="table_source",
        ordering_effect=OrderingEffect.IMPLICIT,
        pipeline_breaker=False,
        row_count_effect="preserve",
        lanes=frozenset({"acero"}),
        acero_node="table_source",
    ),
    "filter": OpDef(
        name="filter",
        ordering_effect=OrderingEffect.PRESERVE,
        pipeline_breaker=False,
        row_count_effect="reduce",
        lanes=frozenset({"datafusion", "acero"}),
        acero_node="filter",
    ),
    "project": OpDef(
        name="project",
        ordering_effect=OrderingEffect.PRESERVE,
        pipeline_breaker=False,
        row_count_effect="preserve",
        lanes=frozenset({"datafusion", "acero"}),
        acero_node="project",
    ),
    "order_by": OpDef(
        name="order_by",
        ordering_effect=OrderingEffect.EXPLICIT,
        pipeline_breaker=True,
        row_count_effect="preserve",
        lanes=frozenset({"datafusion", "acero", "kernel"}),
        acero_node="order_by",
        kernel_name="canonical_sort",
    ),
    "aggregate": OpDef(
        name="aggregate",
        ordering_effect=OrderingEffect.UNORDERED,
        pipeline_breaker=True,
        row_count_effect="reduce",
        lanes=frozenset({"datafusion", "acero"}),
        acero_node="aggregate",
    ),
    "hash_join": OpDef(
        name="hash_join",
        ordering_effect=OrderingEffect.UNORDERED,
        pipeline_breaker=True,
        row_count_effect="unknown",
        lanes=frozenset({"acero"}),
        acero_node="hashjoin",
    ),
    "union_all": OpDef(
        name="union_all",
        ordering_effect=OrderingEffect.UNORDERED,
        pipeline_breaker=True,
        row_count_effect="preserve",
        lanes=frozenset({"acero"}),
        acero_node="union",
    ),
    "winner_select": OpDef(
        name="winner_select",
        ordering_effect=OrderingEffect.UNORDERED,
        pipeline_breaker=True,
        row_count_effect="reduce",
        lanes=frozenset({"acero"}),
    ),
    "explode_list": OpDef(
        name="explode_list",
        ordering_effect=OrderingEffect.UNORDERED,
        pipeline_breaker=True,
        row_count_effect="expand",
        lanes=frozenset({"kernel"}),
        kernel_name="explode_list",
    ),
}

__all__ = ["OP_CATALOG", "Lane", "OpDef", "RowCountEffect"]
