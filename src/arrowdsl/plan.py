"""Plan wrapper around Acero declarations and eager sources."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

import pyarrow as pa

from arrowdsl.acero import acero
from arrowdsl.pyarrow_protocols import ComputeExpression, DeclarationLike
from arrowdsl.runtime import ExecutionContext, Ordering, OrderingKey, OrderingLevel

ReaderThunk = Callable[[], pa.RecordBatchReader]
TableThunk = Callable[[], pa.Table]


@dataclass(frozen=True)
class Plan:
    """Wrapper around an Acero Declaration or eager source.

    Exactly one of ``decl``, ``reader_thunk``, or ``table_thunk`` must be provided.
    """

    decl: DeclarationLike | None = None
    reader_thunk: ReaderThunk | None = None
    table_thunk: TableThunk | None = None

    label: str = ""
    ordering: Ordering = field(default_factory=Ordering.unordered)

    def __post_init__(self) -> None:
        """Validate that exactly one execution source is set.

        Raises
        ------
        ValueError
            Raised when zero or multiple sources are provided.
        """
        sources = [
            self.decl is not None,
            self.reader_thunk is not None,
            self.table_thunk is not None,
        ]
        if sum(sources) != 1:
            msg = "Plan must have exactly one of: decl, reader_thunk, table_thunk."
            raise ValueError(msg)

    def to_reader(self, *, ctx: ExecutionContext) -> pa.RecordBatchReader:
        """Return a streaming reader for this plan.

        Parameters
        ----------
        ctx:
            Execution context controlling threading.

        Returns
        -------
        pyarrow.RecordBatchReader
            Streaming reader.

        Raises
        ------
        ValueError
            Raised when the plan has no execution source.
        """
        if self.reader_thunk is not None:
            return self.reader_thunk()
        if self.decl is not None:
            return self.decl.to_reader(use_threads=ctx.use_threads)
        if self.table_thunk is not None:
            return self.table_thunk().to_reader()
        msg = "Plan has no execution source."
        raise ValueError(msg)

    def to_table(self, *, ctx: ExecutionContext) -> pa.Table:
        """Materialize the plan as a table.

        Parameters
        ----------
        ctx:
            Execution context controlling threading.

        Returns
        -------
        pyarrow.Table
            Materialized table.

        Raises
        ------
        ValueError
            Raised when the plan has no execution source.
        """
        if self.table_thunk is not None:
            return self.table_thunk()
        if self.decl is not None:
            return self.decl.to_table(use_threads=ctx.use_threads)
        if self.reader_thunk is not None:
            return self.reader_thunk().read_all()
        msg = "Plan has no execution source."
        raise ValueError(msg)

    def schema(self, *, ctx: ExecutionContext) -> pa.Schema:
        """Return the output schema for this plan.

        Parameters
        ----------
        ctx:
            Execution context controlling plan execution.

        Returns
        -------
        pyarrow.Schema
            Output schema for the plan.
        """
        if self.table_thunk is not None:
            return self.table_thunk().schema
        reader = self.to_reader(ctx=ctx)
        schema = reader.schema
        close = getattr(reader, "close", None)
        if callable(close):
            close()
        return schema

    @staticmethod
    def table_source(table: pa.Table, *, label: str = "") -> Plan:
        """Create a Plan from an in-memory table using a table_source node.

        Parameters
        ----------
        table:
            Source table.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Plan backed by a table_source declaration.
        """
        decl = acero.Declaration("table_source", acero.TableSourceNodeOptions(table))
        return Plan(decl=decl, label=label, ordering=Ordering.implicit())

    @staticmethod
    def from_table(table: pa.Table, *, label: str = "") -> Plan:
        """Create a Plan from an eager table.

        Parameters
        ----------
        table:
            Source table.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Plan backed by a table thunk.
        """
        return Plan(table_thunk=lambda: table, label=label, ordering=Ordering.implicit())

    @staticmethod
    def from_reader(reader: pa.RecordBatchReader, *, label: str = "") -> Plan:
        """Create a Plan from a reader.

        Parameters
        ----------
        reader:
            Source reader.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Plan backed by a reader thunk.
        """
        return Plan(reader_thunk=lambda: reader, label=label, ordering=Ordering.implicit())

    def filter(self, predicate: ComputeExpression, *, label: str = "") -> Plan:
        """Add a filter node to the plan.

        Parameters
        ----------
        predicate:
            Predicate expression to apply.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Filtered plan.

        Raises
        ------
        TypeError
            Raised when the plan is not Acero-backed.
        """
        if self.decl is None:
            msg = "filter() requires an Acero-backed Plan (decl is None)."
            raise TypeError(msg)
        decl = acero.Declaration("filter", acero.FilterNodeOptions(predicate), inputs=[self.decl])
        return Plan(decl=decl, label=label or self.label, ordering=self.ordering)

    def project(
        self, expressions: Sequence[ComputeExpression], names: Sequence[str], *, label: str = ""
    ) -> Plan:
        """Add a project node to the plan.

        Parameters
        ----------
        expressions:
            Expressions to project.
        names:
            Output column names.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Projected plan.

        Raises
        ------
        TypeError
            Raised when the plan is not Acero-backed.
        """
        if self.decl is None:
            msg = "project() requires an Acero-backed Plan (decl is None)."
            raise TypeError(msg)
        decl = acero.Declaration(
            "project",
            acero.ProjectNodeOptions(list(expressions), list(names)),
            inputs=[self.decl],
        )
        return Plan(decl=decl, label=label or self.label, ordering=self.ordering)

    def order_by(self, sort_keys: Sequence[OrderingKey], *, label: str = "") -> Plan:
        """Add an order-by node to the plan.

        Parameters
        ----------
        sort_keys:
            Sort keys as (column, order) pairs.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Ordered plan.

        Raises
        ------
        TypeError
            Raised when the plan is not Acero-backed.
        """
        if self.decl is None:
            msg = "order_by() requires an Acero-backed Plan (decl is None)."
            raise TypeError(msg)
        decl = acero.Declaration(
            "order_by",
            acero.OrderByNodeOptions(sort_keys=list(sort_keys)),
            inputs=[self.decl],
        )
        return Plan(
            decl=decl, label=label or self.label, ordering=Ordering.explicit(tuple(sort_keys))
        )

    def aggregate(
        self,
        group_keys: Sequence[str],
        aggs: Sequence[tuple[str, str]],
        *,
        label: str = "",
    ) -> Plan:
        """Add an aggregate node to the plan.

        Parameters
        ----------
        group_keys:
            Key columns to group by.
        aggs:
            Aggregates as (column, function) pairs.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Aggregated plan (unordered output).

        Raises
        ------
        TypeError
            Raised when the plan is not Acero-backed.
        """
        if self.decl is None:
            msg = "aggregate() requires an Acero-backed Plan (decl is None)."
            raise TypeError(msg)
        agg_specs = [(col, fn, None, f"{col}_{fn}") for col, fn in aggs]
        keys = list(group_keys) if group_keys else None
        decl = acero.Declaration(
            "aggregate",
            acero.AggregateNodeOptions(agg_specs, keys=keys),
            inputs=[self.decl],
        )
        return Plan(decl=decl, label=label or self.label, ordering=Ordering.unordered())

    def mark_unordered(self, *, label: str = "") -> Plan:
        """Return a copy of the plan marked unordered.

        Parameters
        ----------
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Plan marked unordered.

        Raises
        ------
        ValueError
            Raised when the plan has no execution source.
        """
        if self.decl is not None:
            return Plan(decl=self.decl, label=label or self.label, ordering=Ordering.unordered())
        if self.table_thunk is not None:
            return Plan(
                table_thunk=self.table_thunk,
                label=label or self.label,
                ordering=Ordering.unordered(),
            )
        if self.reader_thunk is not None:
            return Plan(
                reader_thunk=self.reader_thunk,
                label=label or self.label,
                ordering=Ordering.unordered(),
            )
        msg = "Plan has no execution source."
        raise ValueError(msg)

    def is_ordered(self) -> bool:
        """Return whether this plan is ordered.

        Returns
        -------
        bool
            ``True`` when ordering is implicit or explicit.
        """
        return self.ordering.level in {OrderingLevel.IMPLICIT, OrderingLevel.EXPLICIT}


def union_all_plans(plans: Sequence[Plan], *, label: str = "") -> Plan:
    """Union multiple Acero-backed plans into a single plan.

    Parameters
    ----------
    plans:
        Plans to union.
    label:
        Optional plan label.

    Returns
    -------
    Plan
        Unioned plan (unordered output).

    Raises
    ------
    ValueError
        Raised when no plans are provided.
    TypeError
        Raised when any plan is not Acero-backed.
    """
    if not plans:
        msg = "union_all_plans requires at least one plan."
        raise ValueError(msg)
    if len(plans) == 1:
        plan = plans[0]
        if plan.decl is None:
            msg = "union_all_plans requires Acero-backed Plans (missing declarations)."
            raise TypeError(msg)
        return Plan(decl=plan.decl, label=label or plan.label, ordering=Ordering.unordered())
    decls: list[DeclarationLike] = []
    for plan in plans:
        if plan.decl is None:
            msg = "union_all_plans requires Acero-backed Plans (missing declarations)."
            raise TypeError(msg)
        decls.append(plan.decl)
    decl = acero.Declaration("union", None, inputs=decls)
    return Plan(decl=decl, label=label, ordering=Ordering.unordered())
