"""PlanIR builder for ArrowDSL consolidation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from arrowdsl.core.context import Ordering, OrderingEffect, OrderingKey, OrderingLevel
from arrowdsl.core.expr_types import ExplodeSpec
from arrowdsl.core.interop import ComputeExpression
from arrowdsl.ir.plan import OpNode, PlanIR
from arrowdsl.ops.catalog import OP_CATALOG

type OpInputs = tuple[PlanIR, ...]
type RowAgg = tuple[str, str]
type SortKeys = tuple[OrderingKey, ...]


@dataclass
class PlanBuilder:
    """Mutable builder for PlanIR with ordering metadata."""

    nodes: list[OpNode] = field(default_factory=list)
    ordering: Ordering = field(default_factory=Ordering.unordered)
    pipeline_breakers: list[str] = field(default_factory=list)

    @classmethod
    def from_plan(
        cls,
        *,
        ir: PlanIR,
        ordering: Ordering,
        pipeline_breakers: tuple[str, ...],
    ) -> PlanBuilder:
        """Create a builder from an existing PlanIR and metadata.

        Returns
        -------
        PlanBuilder
            Builder seeded with the provided plan state.
        """
        return cls(
            nodes=list(ir.nodes),
            ordering=ordering,
            pipeline_breakers=list(pipeline_breakers),
        )

    def build(self) -> tuple[PlanIR, Ordering, tuple[str, ...]]:
        """Finalize the builder into PlanIR and metadata.

        Returns
        -------
        tuple[PlanIR, Ordering, tuple[str, ...]]
            Plan IR, ordering metadata, and pipeline breakers.
        """
        return PlanIR(tuple(self.nodes)), self.ordering, tuple(self.pipeline_breakers)

    def _append(
        self,
        *,
        name: str,
        args: dict[str, object],
        inputs: OpInputs = (),
        ordering_effect: OrderingEffect | None = None,
        ordering_keys: SortKeys | None = None,
    ) -> None:
        op_def = OP_CATALOG.get(name)
        if op_def is None:
            msg = f"Unknown op: {name!r}."
            raise ValueError(msg)
        effect = ordering_effect or op_def.ordering_effect
        self.ordering = _apply_ordering_effect(
            self.ordering,
            effect,
            ordering_keys=ordering_keys,
        )
        if op_def.pipeline_breaker:
            self.pipeline_breakers.append(name)
        self.nodes.append(OpNode(name=name, args=args, inputs=inputs))

    def scan(
        self,
        *,
        dataset: object,
        columns: Sequence[str] | Mapping[str, ComputeExpression],
        predicate: ComputeExpression | None,
        ordering_effect: OrderingEffect,
    ) -> None:
        """Append a scan operation."""
        self._append(
            name="scan",
            args={
                "dataset": dataset,
                "columns": columns,
                "predicate": predicate,
            },
            ordering_effect=ordering_effect,
        )

    def table_source(self, *, table: object) -> None:
        """Append a table_source operation."""
        self._append(
            name="table_source",
            args={"table": table},
        )

    def filter(self, *, predicate: ComputeExpression) -> None:
        """Append a filter operation."""
        self._append(
            name="filter",
            args={"predicate": predicate},
        )

    def project(self, *, expressions: list[ComputeExpression], names: list[str]) -> None:
        """Append a project operation."""
        self._append(
            name="project",
            args={"expressions": expressions, "names": names},
        )

    def order_by(self, *, sort_keys: SortKeys) -> None:
        """Append an order_by operation."""
        self._append(
            name="order_by",
            args={"sort_keys": sort_keys},
            ordering_keys=sort_keys,
        )

    def aggregate(self, *, group_keys: list[str], aggs: list[RowAgg]) -> None:
        """Append an aggregate operation."""
        self._append(
            name="aggregate",
            args={"group_keys": group_keys, "aggs": aggs},
        )

    def winner_select(self, *, spec: object, columns: list[str]) -> None:
        """Append a winner_select operation."""
        self._append(
            name="winner_select",
            args={"spec": spec, "columns": columns},
        )

    def explode_list(
        self,
        *,
        spec: ExplodeSpec,
        out_parent_col: str,
    ) -> None:
        """Append an explode_list operation."""
        self._append(
            name="explode_list",
            args={
                "spec": spec,
                "out_parent_col": out_parent_col,
            },
        )

    def hash_join(self, *, right: PlanIR, spec: object) -> None:
        """Append a hash_join operation with a right input."""
        self._append(
            name="hash_join",
            args={"spec": spec},
            inputs=(right,),
        )

    def union_all(self, *, inputs: OpInputs) -> None:
        """Append a union_all operation."""
        self._append(
            name="union_all",
            args={},
            inputs=inputs,
            ordering_effect=OrderingEffect.UNORDERED,
        )


def _apply_ordering_effect(
    ordering: Ordering,
    effect: OrderingEffect,
    *,
    ordering_keys: SortKeys | None = None,
) -> Ordering:
    result = ordering
    if effect == OrderingEffect.UNORDERED:
        result = Ordering.unordered()
    elif effect == OrderingEffect.IMPLICIT and ordering.level == OrderingLevel.UNORDERED:
        result = Ordering.implicit()
    elif effect == OrderingEffect.EXPLICIT:
        keys = tuple(ordering_keys) if ordering_keys is not None else ()
        result = Ordering.explicit(keys)
    return result


__all__ = ["PlanBuilder"]
