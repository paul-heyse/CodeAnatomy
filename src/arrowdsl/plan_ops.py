"""Plan-lane helpers for Acero declarations and ordering metadata."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import pyarrow.dataset as ds

from arrowdsl.acero import acero
from arrowdsl.pyarrow_protocols import ComputeExpression, DeclarationLike, TableLike
from arrowdsl.runtime import ExecutionContext, Ordering, OrderingEffect


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

        Parameters
        ----------
        inputs:
            Upstream declarations (unused for scans).
        ctx:
            Execution context for runtime options.

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
        opts = acero.ScanNodeOptions(
            self.dataset,
            columns=self.columns,
            filter=self.predicate,
            **ctx.runtime.scan.scan_node_kwargs(),
        )
        return acero.Declaration("scan", opts)

    def apply_ordering(self, ordering: Ordering) -> Ordering:
        """Apply the ordering effect of the scan operation.

        Parameters
        ----------
        ordering:
            Incoming ordering.

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

        Parameters
        ----------
        inputs:
            Upstream declarations (unused for table sources).
        ctx:
            Execution context (unused for table sources).

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

        Parameters
        ----------
        ordering:
            Incoming ordering.

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

        Parameters
        ----------
        inputs:
            Upstream declarations.
        ctx:
            Execution context (unused for filters).

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

        Parameters
        ----------
        ordering:
            Incoming ordering.

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

        Parameters
        ----------
        inputs:
            Upstream declarations.
        ctx:
            Execution context (unused for projections).

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

        Parameters
        ----------
        ordering:
            Incoming ordering.

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

        Parameters
        ----------
        inputs:
            Upstream declarations.
        ctx:
            Execution context (unused for order-by).

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

        Parameters
        ----------
        ordering:
            Incoming ordering.

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

        Parameters
        ----------
        inputs:
            Upstream declarations.
        ctx:
            Execution context (unused for aggregates).

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

        Parameters
        ----------
        ordering:
            Incoming ordering.

        Returns
        -------
        Ordering
            Unordered ordering after aggregation.
        """
        _ = ordering
        return Ordering.unordered()
