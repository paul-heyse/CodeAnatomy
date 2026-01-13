"""Plan ops and shared specs for ArrowDSL."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

import pyarrow.dataset as ds

from arrowdsl.core.context import ExecutionContext, Ordering, OrderingEffect
from arrowdsl.core.interop import ComputeExpression, DeclarationLike, TableLike, acero


class PlanOp(Protocol):
    """Protocol for Acero plan operations."""

    @property
    def ordering_effect(self) -> OrderingEffect:
        """Return the ordering effect for this operation."""
        ...

    @property
    def is_pipeline_breaker(self) -> bool:
        """Return whether the operation is a pipeline breaker."""
        ...

    def to_declaration(
        self, inputs: list[DeclarationLike], ctx: ExecutionContext | None
    ) -> DeclarationLike:
        """Build an Acero declaration for this operation."""
        ...

    def apply_ordering(self, ordering: Ordering) -> Ordering:
        """Return the updated ordering metadata for this operation."""
        ...


def scan_ordering_effect(ctx: ExecutionContext) -> OrderingEffect:
    """Return the default scan ordering effect for a context.

    Returns
    -------
    OrderingEffect
        Ordering effect implied by the execution context.
    """
    if ctx.runtime.scan.implicit_ordering or ctx.runtime.scan.require_sequenced_output:
        return OrderingEffect.IMPLICIT
    return OrderingEffect.UNORDERED


@runtime_checkable
class KernelOp(Protocol):
    """Protocol for kernel-lane operations on tables."""

    @property
    def ordering_effect(self) -> OrderingEffect:
        """Return the ordering effect for this operation."""
        ...

    def apply(self, table: TableLike, ctx: ExecutionContext) -> TableLike:
        """Apply the operation to a table."""
        ...

    def apply_ordering(self, ordering: Ordering) -> Ordering:
        """Return the updated ordering metadata for this operation."""
        ...


@dataclass(frozen=True)
class ScanOp:
    """Scan operation for dataset sources."""

    dataset: ds.Dataset
    columns: Sequence[str] | Mapping[str, ComputeExpression]
    predicate: ComputeExpression | None = None

    ordering_effect: OrderingEffect = OrderingEffect.IMPLICIT
    is_pipeline_breaker: bool = False

    def to_declaration(
        self, inputs: list[DeclarationLike], ctx: ExecutionContext | None
    ) -> DeclarationLike:
        """Build the Acero scan declaration.

        Returns
        -------
        DeclarationLike
            Acero scan declaration.

        Raises
        ------
        ValueError
            Raised when the execution context is missing.
        """
        _ = inputs
        if ctx is None:
            msg = "ScanOp requires an execution context."
            raise ValueError(msg)
        scan_kwargs = ctx.runtime.scan.scanner_kwargs()
        scan_kwargs.update(ctx.runtime.scan.scan_node_kwargs())
        opts = acero.ScanNodeOptions(
            self.dataset,
            columns=self.columns,
            filter=self.predicate,
            **scan_kwargs,
        )
        return acero.Declaration("scan", opts)

    def apply_ordering(self, ordering: Ordering) -> Ordering:
        """Apply the ordering effect of the scan operation.

        Returns
        -------
        Ordering
            Updated ordering after the scan.
        """
        if self.ordering_effect == OrderingEffect.IMPLICIT:
            if ordering.level == Ordering.unordered().level:
                return Ordering.implicit()
            return ordering
        return ordering


@dataclass(frozen=True)
class TableSourceOp:
    """Table source operation for in-memory tables."""

    table: TableLike

    ordering_effect: OrderingEffect = OrderingEffect.IMPLICIT
    is_pipeline_breaker: bool = False

    def to_declaration(
        self, inputs: list[DeclarationLike], ctx: ExecutionContext | None
    ) -> DeclarationLike:
        """Build the Acero table-source declaration.

        Returns
        -------
        DeclarationLike
            Acero table-source declaration.
        """
        _ = inputs
        _ = ctx
        return acero.Declaration("table_source", acero.TableSourceNodeOptions(self.table))

    def apply_ordering(self, ordering: Ordering) -> Ordering:
        """Apply the ordering effect of the table source.

        Returns
        -------
        Ordering
            Updated ordering after the table source.
        """
        if self.ordering_effect == OrderingEffect.IMPLICIT:
            return Ordering.implicit()
        return ordering


@dataclass(frozen=True)
class FilterOp:
    """Filter operation preserving ordering."""

    predicate: ComputeExpression

    ordering_effect: OrderingEffect = OrderingEffect.PRESERVE
    is_pipeline_breaker: bool = False

    def to_declaration(
        self, inputs: list[DeclarationLike], ctx: ExecutionContext | None
    ) -> DeclarationLike:
        """Build the Acero filter declaration.

        Returns
        -------
        DeclarationLike
            Acero filter declaration.
        """
        _ = ctx
        return acero.Declaration("filter", acero.FilterNodeOptions(self.predicate), inputs=inputs)

    @staticmethod
    def apply_ordering(ordering: Ordering) -> Ordering:
        """Apply the ordering effect of the filter.

        Returns
        -------
        Ordering
            Unchanged ordering for filters.
        """
        return ordering


@dataclass(frozen=True)
class ProjectOp:
    """Projection operation preserving ordering."""

    expressions: Sequence[ComputeExpression]
    names: Sequence[str]

    ordering_effect: OrderingEffect = OrderingEffect.PRESERVE
    is_pipeline_breaker: bool = False

    def to_declaration(
        self, inputs: list[DeclarationLike], ctx: ExecutionContext | None
    ) -> DeclarationLike:
        """Build the Acero project declaration.

        Returns
        -------
        DeclarationLike
            Acero project declaration.
        """
        _ = ctx
        return acero.Declaration(
            "project",
            acero.ProjectNodeOptions(list(self.expressions), list(self.names)),
            inputs=inputs,
        )

    @staticmethod
    def apply_ordering(ordering: Ordering) -> Ordering:
        """Apply the ordering effect of the projection.

        Returns
        -------
        Ordering
            Unchanged ordering for projections.
        """
        return ordering


@dataclass(frozen=True)
class OrderByOp:
    """Order-by operation establishing explicit ordering."""

    sort_keys: Sequence[tuple[str, str]]

    ordering_effect: OrderingEffect = OrderingEffect.EXPLICIT
    is_pipeline_breaker: bool = True

    def to_declaration(
        self, inputs: list[DeclarationLike], ctx: ExecutionContext | None
    ) -> DeclarationLike:
        """Build the Acero order-by declaration.

        Returns
        -------
        DeclarationLike
            Acero order-by declaration.
        """
        _ = ctx
        return acero.Declaration(
            "order_by",
            acero.OrderByNodeOptions(sort_keys=list(self.sort_keys)),
            inputs=inputs,
        )

    def apply_ordering(self, ordering: Ordering) -> Ordering:
        """Apply the ordering effect of the order-by operation.

        Returns
        -------
        Ordering
            Explicit ordering for the sort keys.
        """
        _ = ordering
        return Ordering.explicit(tuple(self.sort_keys))


@dataclass(frozen=True)
class AggregateOp:
    """Aggregate operation that breaks ordering."""

    group_keys: Sequence[str]
    aggs: Sequence[tuple[str, str]]

    ordering_effect: OrderingEffect = OrderingEffect.UNORDERED
    is_pipeline_breaker: bool = True

    def to_declaration(
        self, inputs: list[DeclarationLike], ctx: ExecutionContext | None
    ) -> DeclarationLike:
        """Build the Acero aggregate declaration.

        Returns
        -------
        DeclarationLike
            Acero aggregate declaration.
        """
        _ = ctx
        agg_specs = [(col, fn, None, f"{col}_{fn}") for col, fn in self.aggs]
        keys = list(self.group_keys) if self.group_keys else None
        return acero.Declaration(
            "aggregate",
            acero.AggregateNodeOptions(agg_specs, keys=keys),
            inputs=inputs,
        )

    @staticmethod
    def apply_ordering(ordering: Ordering) -> Ordering:
        """Apply the ordering effect of the aggregate operation.

        Returns
        -------
        Ordering
            Unordered ordering after aggregation.
        """
        _ = ordering
        return Ordering.unordered()


@dataclass(frozen=True)
class SortKey:
    """Sort key specification for deterministic ordering."""

    column: str
    order: Literal["ascending", "descending"] = "ascending"


type DedupeStrategy = Literal[
    "KEEP_FIRST_AFTER_SORT",
    "KEEP_BEST_BY_SCORE",
    "COLLAPSE_LIST",
    "KEEP_ARBITRARY",
]


@dataclass(frozen=True)
class DedupeSpec:
    """Dedupe semantics for a table."""

    keys: tuple[str, ...]
    tie_breakers: tuple[SortKey, ...] = ()
    strategy: DedupeStrategy = "KEEP_FIRST_AFTER_SORT"


@dataclass(frozen=True)
class IntervalAlignOptions:
    """Interval alignment configuration."""

    mode: Literal["EXACT", "CONTAINED_BEST", "OVERLAP_BEST"] = "CONTAINED_BEST"
    how: Literal["inner", "left"] = "inner"

    left_path_col: str = "path"
    left_start_col: str = "bstart"
    left_end_col: str = "bend"

    right_path_col: str = "path"
    right_start_col: str = "bstart"
    right_end_col: str = "bend"

    select_left: tuple[str, ...] = ()
    select_right: tuple[str, ...] = ()

    tie_breakers: tuple[SortKey, ...] = ()

    emit_match_meta: bool = True
    match_kind_col: str = "match_kind"
    match_score_col: str = "match_score"
    right_suffix: str = "__r"


type JoinType = Literal[
    "inner",
    "left outer",
    "right outer",
    "full outer",
    "left semi",
    "right semi",
    "left anti",
    "right anti",
]


@dataclass(frozen=True)
class JoinSpec:
    """Join specification for hash joins."""

    join_type: JoinType
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    left_output: tuple[str, ...]
    right_output: tuple[str, ...]
    output_suffix_for_left: str = ""
    output_suffix_for_right: str = ""

    def __post_init__(self) -> None:
        """Validate that left and right key counts match.

        Raises
        ------
        ValueError
            Raised when the key counts do not match.
        """
        if len(self.left_keys) != len(self.right_keys):
            msg = "left_keys and right_keys must have the same length."
            raise ValueError(msg)


@dataclass(frozen=True)
class JoinOp:
    """Hash-join operation producing unordered output."""

    spec: JoinSpec

    ordering_effect: OrderingEffect = OrderingEffect.UNORDERED
    is_pipeline_breaker: bool = False

    def to_declaration(
        self, inputs: list[DeclarationLike], ctx: ExecutionContext | None
    ) -> DeclarationLike:
        """Build the Acero hash-join declaration.

        Returns
        -------
        DeclarationLike
            Acero hash-join declaration.
        """
        _ = ctx
        opts = acero.HashJoinNodeOptions(
            self.spec.join_type,
            list(self.spec.left_keys),
            list(self.spec.right_keys),
            left_output=list(self.spec.left_output),
            right_output=list(self.spec.right_output),
            output_suffix_for_left=self.spec.output_suffix_for_left,
            output_suffix_for_right=self.spec.output_suffix_for_right,
        )
        return acero.Declaration("hashjoin", opts, inputs=inputs)

    @staticmethod
    def apply_ordering(ordering: Ordering) -> Ordering:
        """Return unordered output after a hash join.

        Returns
        -------
        Ordering
            Unordered ordering after the join.
        """
        _ = ordering
        return Ordering.unordered()


__all__ = [
    "AggregateOp",
    "DedupeSpec",
    "DedupeStrategy",
    "FilterOp",
    "IntervalAlignOptions",
    "JoinOp",
    "JoinSpec",
    "JoinType",
    "KernelOp",
    "OrderByOp",
    "PlanOp",
    "ProjectOp",
    "ScanOp",
    "SortKey",
    "TableSourceOp",
    "scan_ordering_effect",
]
