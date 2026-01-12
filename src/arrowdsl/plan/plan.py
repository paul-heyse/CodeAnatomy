"""Plan wrapper around Acero declarations and eager sources."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from arrowdsl.core.context import ExecutionContext, Ordering, OrderingKey, OrderingLevel
from arrowdsl.core.interop import (
    ComputeExpression,
    DeclarationLike,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    acero,
)
from arrowdsl.plan.ops import (
    AggregateOp,
    FilterOp,
    JoinSpec,
    OrderByOp,
    PlanOp,
    ProjectOp,
    TableSourceOp,
)

ReaderThunk = Callable[[], RecordBatchReaderLike]
TableThunk = Callable[[], TableLike]


@dataclass(frozen=True)
class PlanSpec:
    """Plan wrapper with pipeline-breaker metadata."""

    plan: Plan
    pipeline_breakers: tuple[str, ...] = ()

    @classmethod
    def from_plan(cls, plan: Plan) -> PlanSpec:
        """Create a PlanSpec from a Plan instance.

        Returns
        -------
        PlanSpec
            PlanSpec wrapper with pipeline-breaker metadata.
        """
        return cls(plan=plan, pipeline_breakers=plan.pipeline_breakers)

    def to_reader(self, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
        """Return a streaming reader if no pipeline breakers are present.

        Returns
        -------
        pyarrow.RecordBatchReader
            Streaming reader for the plan.

        Raises
        ------
        ValueError
            Raised when pipeline breakers are present.
        """
        if self.pipeline_breakers:
            msg = f"Plan contains pipeline breakers: {self.pipeline_breakers}"
            raise ValueError(msg)
        return self.plan.to_reader(ctx=ctx)

    def to_table(self, *, ctx: ExecutionContext) -> TableLike:
        """Materialize the plan as a table.

        Returns
        -------
        pyarrow.Table
            Materialized table.
        """
        return self.plan.to_table(ctx=ctx)

    def schema(self, *, ctx: ExecutionContext) -> SchemaLike:
        """Return the output schema for the plan.

        Returns
        -------
        pyarrow.Schema
            Output schema for the plan.
        """
        return self.plan.schema(ctx=ctx)


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
    pipeline_breakers: tuple[str, ...] = ()

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

    def to_reader(self, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
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
        if self.pipeline_breakers:
            msg = f"Plan contains pipeline breakers: {self.pipeline_breakers}"
            raise ValueError(msg)
        if self.reader_thunk is not None:
            return self.reader_thunk()
        if self.decl is not None:
            return self.decl.to_reader(use_threads=ctx.use_threads)
        if self.table_thunk is not None:
            return self.table_thunk().to_reader()
        msg = "Plan has no execution source."
        raise ValueError(msg)

    def to_table(self, *, ctx: ExecutionContext) -> TableLike:
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

    def _apply_plan_op(
        self,
        op: PlanOp,
        *,
        ctx: ExecutionContext | None = None,
        label: str = "",
    ) -> Plan:
        if self.decl is None:
            msg = "Plan operation requires an Acero-backed Plan (decl is None)."
            raise TypeError(msg)
        decl = op.to_declaration([self.decl], ctx=ctx)
        ordering = op.apply_ordering(self.ordering)
        pipeline_breakers = self.pipeline_breakers
        if op.is_pipeline_breaker:
            pipeline_breakers = (*pipeline_breakers, op.__class__.__name__)
        return Plan(
            decl=decl,
            label=label or self.label,
            ordering=ordering,
            pipeline_breakers=pipeline_breakers,
        )

    def schema(self, *, ctx: ExecutionContext) -> SchemaLike:
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
        if self.pipeline_breakers:
            return self.to_table(ctx=ctx).schema
        if self.table_thunk is not None:
            return self.table_thunk().schema
        reader = self.to_reader(ctx=ctx)
        schema = reader.schema
        close = getattr(reader, "close", None)
        if callable(close):
            close()
        return schema

    @staticmethod
    def table_source(table: TableLike, *, label: str = "") -> Plan:
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
        op = TableSourceOp(table=table)
        decl = op.to_declaration([], ctx=None)
        ordering = op.apply_ordering(Ordering.unordered())
        return Plan(decl=decl, label=label, ordering=ordering, pipeline_breakers=())

    @staticmethod
    def from_table(table: TableLike, *, label: str = "") -> Plan:
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
        return Plan(
            table_thunk=lambda: table,
            label=label,
            ordering=Ordering.implicit(),
            pipeline_breakers=(),
        )

    @staticmethod
    def from_reader(reader: RecordBatchReaderLike, *, label: str = "") -> Plan:
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
        return Plan(
            reader_thunk=lambda: reader,
            label=label,
            ordering=Ordering.implicit(),
            pipeline_breakers=(),
        )

    def filter(
        self,
        predicate: ComputeExpression,
        *,
        ctx: ExecutionContext | None = None,
        label: str = "",
    ) -> Plan:
        """Add a filter node to the plan.

        Parameters
        ----------
        predicate:
            Predicate expression to apply.
        ctx:
            Optional execution context for plan compilation.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Filtered plan.
        """
        return self._apply_plan_op(FilterOp(predicate=predicate), ctx=ctx, label=label)

    def project(
        self,
        expressions: Sequence[ComputeExpression],
        names: Sequence[str],
        *,
        ctx: ExecutionContext | None = None,
        label: str = "",
    ) -> Plan:
        """Add a project node to the plan.

        Parameters
        ----------
        expressions:
            Expressions to project.
        names:
            Output column names.
        ctx:
            Optional execution context for plan compilation.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Projected plan.
        """
        return self._apply_plan_op(
            ProjectOp(expressions=expressions, names=names),
            ctx=ctx,
            label=label,
        )

    def order_by(
        self,
        sort_keys: Sequence[OrderingKey],
        *,
        ctx: ExecutionContext | None = None,
        label: str = "",
    ) -> Plan:
        """Add an order-by node to the plan.

        Parameters
        ----------
        sort_keys:
            Sort keys as (column, order) pairs.
        ctx:
            Optional execution context for plan compilation.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Ordered plan.
        """
        op = OrderByOp(sort_keys=sort_keys)
        return self._apply_plan_op(op, ctx=ctx, label=label)

    def aggregate(
        self,
        group_keys: Sequence[str],
        aggs: Sequence[tuple[str, str]],
        *,
        ctx: ExecutionContext | None = None,
        label: str = "",
    ) -> Plan:
        """Add an aggregate node to the plan.

        Parameters
        ----------
        group_keys:
            Key columns to group by.
        aggs:
            Aggregates as (column, function) pairs.
        ctx:
            Optional execution context for plan compilation.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Aggregated plan (unordered output).
        """
        op = AggregateOp(group_keys=group_keys, aggs=aggs)
        return self._apply_plan_op(op, ctx=ctx, label=label)

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
            return Plan(
                decl=self.decl,
                label=label or self.label,
                ordering=Ordering.unordered(),
                pipeline_breakers=self.pipeline_breakers,
            )
        if self.table_thunk is not None:
            return Plan(
                table_thunk=self.table_thunk,
                label=label or self.label,
                ordering=Ordering.unordered(),
                pipeline_breakers=self.pipeline_breakers,
            )
        if self.reader_thunk is not None:
            return Plan(
                reader_thunk=self.reader_thunk,
                label=label or self.label,
                ordering=Ordering.unordered(),
                pipeline_breakers=self.pipeline_breakers,
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
        return Plan(
            decl=plan.decl,
            label=label or plan.label,
            ordering=Ordering.unordered(),
            pipeline_breakers=plan.pipeline_breakers,
        )
    decls: list[DeclarationLike] = []
    pipeline_breakers: list[str] = []
    for plan in plans:
        if plan.decl is None:
            msg = "union_all_plans requires Acero-backed Plans (missing declarations)."
            raise TypeError(msg)
        decls.append(plan.decl)
        pipeline_breakers.extend(plan.pipeline_breakers)
    decl = acero.Declaration("union", None, inputs=decls)
    return Plan(
        decl=decl,
        label=label,
        ordering=Ordering.unordered(),
        pipeline_breakers=tuple(pipeline_breakers),
    )


def hash_join(*, left: Plan, right: Plan, spec: JoinSpec, label: str = "") -> Plan:
    """Perform a hash join in the plan lane.

    Parameters
    ----------
    left:
        Left-side plan.
    right:
        Right-side plan.
    spec:
        Join specification.
    label:
        Optional plan label.

    Returns
    -------
    Plan
        Joined plan (unordered output).

    Raises
    ------
    TypeError
        Raised when one or both plans are missing Acero declarations.
    """
    if left.decl is None or right.decl is None:
        msg = "hash_join requires Acero-backed Plans (missing declarations)."
        raise TypeError(msg)

    decl = acero.Declaration(
        "hashjoin",
        acero.HashJoinNodeOptions(
            join_type=spec.join_type,
            left_keys=list(spec.left_keys),
            right_keys=list(spec.right_keys),
            left_output=list(spec.left_output),
            right_output=list(spec.right_output),
            output_suffix_for_left=spec.output_suffix_for_left,
            output_suffix_for_right=spec.output_suffix_for_right,
        ),
        inputs=[left.decl, right.decl],
    )
    return Plan(
        decl=decl,
        label=label or f"{left.label}_join_{right.label}",
        ordering=Ordering.unordered(),
    )
