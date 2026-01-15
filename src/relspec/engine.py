"""Relational plan compilers and resolvers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal, Protocol, TypeVar, cast

import ibis
from ibis.backends import BaseBackend
from ibis.expr.datatypes import DataType
from ibis.expr.types import BooleanValue, Scalar
from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.context import ExecutionContext, Ordering
from arrowdsl.plan.query import ScanTelemetry
from ibis_engine.expr_compiler import IbisExprRegistry, expr_ir_to_ibis
from ibis_engine.plan import IbisPlan
from relspec.model import DatasetRef, HashJoinConfig
from relspec.plan import (
    RelAggregate,
    RelFilter,
    RelJoin,
    RelNode,
    RelPlan,
    RelProject,
    RelSource,
    RelUnion,
)
from relspec.rules.rel_ops import AggregateExpr

PlanT_co = TypeVar("PlanT_co", covariant=True)
PlanT = TypeVar("PlanT")
JoinKind = Literal[
    "anti",
    "any_inner",
    "any_left",
    "asof",
    "cross",
    "inner",
    "left",
    "outer",
    "positional",
    "right",
    "semi",
]


class PlanResolver(Protocol[PlanT_co]):
    """Resolve a ``DatasetRef`` to an executable plan."""

    @property
    def backend(self) -> BaseBackend | None:
        """Return the backend associated with this resolver."""
        ...

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> PlanT_co:
        """Resolve a dataset reference into a plan."""
        ...

    def telemetry(self, ref: DatasetRef, *, ctx: ExecutionContext) -> ScanTelemetry | None:
        """Return scan telemetry for a dataset reference."""
        ...


class RelPlanCompiler(Protocol[PlanT]):
    """Protocol for compiling relational plans into an engine plan."""

    plan_type: type[PlanT]

    def compile(
        self,
        plan: RelPlan,
        *,
        ctx: ExecutionContext,
        resolver: PlanResolver[PlanT],
    ) -> PlanT:
        """Compile a relational plan into an engine-specific plan."""
        ...


class IbisRelPlanCompiler(RelPlanCompiler[IbisPlan]):
    """Compile relational plans into Ibis plans."""

    plan_type = IbisPlan

    def __init__(self, *, registry: IbisExprRegistry | None = None) -> None:
        self.registry = registry or IbisExprRegistry()

    def compile(
        self,
        plan: RelPlan,
        *,
        ctx: ExecutionContext,
        resolver: PlanResolver[IbisPlan],
    ) -> IbisPlan:
        """Compile a relational plan into an Ibis plan.

        Returns
        -------
        IbisPlan
            Ibis plan with ordering metadata applied when available.
        """
        expr, ordering = _compile_node(
            plan.root, ctx=ctx, resolver=resolver, registry=self.registry
        )
        ordering = plan.ordering if plan.ordering != Ordering.unordered() else ordering
        return IbisPlan(expr=expr, ordering=ordering)


def _compile_node(
    node: RelNode,
    *,
    ctx: ExecutionContext,
    resolver: PlanResolver[IbisPlan],
    registry: IbisExprRegistry,
) -> tuple[IbisTable, Ordering]:
    """Compile a relational plan node into an Ibis table plus ordering.

    Parameters
    ----------
    node
        Relational plan node to compile.
    ctx
        Execution context for compilation.
    resolver
        Resolver for source datasets.
    registry
        Expression registry for IR translation.

    Returns
    -------
    tuple[IbisTable, Ordering]
        Compiled Ibis table and its ordering metadata.

    Raises
    ------
    TypeError
        Raised when an unsupported plan node is encountered.
    """
    unary = _compile_unary_node(node, ctx=ctx, resolver=resolver, registry=registry)
    if unary is not None:
        return unary
    if isinstance(node, RelSource):
        return _compile_source_node(node, ctx=ctx, resolver=resolver)
    if isinstance(node, RelJoin):
        return _compile_join_node(node, ctx=ctx, resolver=resolver, registry=registry)
    if isinstance(node, RelAggregate):
        return _compile_aggregate_node(node, ctx=ctx, resolver=resolver, registry=registry)
    if isinstance(node, RelUnion):
        return _compile_union_node(node, ctx=ctx, resolver=resolver, registry=registry)
    msg = f"Unsupported RelNode type: {type(node).__name__}."
    raise TypeError(msg)


def _compile_source_node(
    node: RelSource,
    *,
    ctx: ExecutionContext,
    resolver: PlanResolver[IbisPlan],
) -> tuple[IbisTable, Ordering]:
    """Compile a source node by resolving its dataset reference.

    Parameters
    ----------
    node
        Source node to compile.
    ctx
        Execution context for compilation.
    resolver
        Resolver for dataset references.

    Returns
    -------
    tuple[IbisTable, Ordering]
        Compiled Ibis table and its ordering metadata.
    """
    ref = node.ref
    if node.query is not None:
        ref = DatasetRef(name=ref.name, query=node.query, label=ref.label)
    plan = resolver.resolve(ref, ctx=ctx)
    return plan.expr, plan.ordering


def _compile_unary_node(
    node: RelNode,
    *,
    ctx: ExecutionContext,
    resolver: PlanResolver[IbisPlan],
    registry: IbisExprRegistry,
) -> tuple[IbisTable, Ordering] | None:
    """Compile a unary node (project or filter) when applicable.

    Parameters
    ----------
    node
        Relational plan node to compile.
    ctx
        Execution context for compilation.
    resolver
        Resolver for dataset references.
    registry
        Expression registry for IR translation.

    Returns
    -------
    tuple[IbisTable, Ordering] | None
        Compiled table and ordering, or ``None`` when node is not unary.
    """
    if isinstance(node, RelProject):
        source, ordering = _compile_node(node.source, ctx=ctx, resolver=resolver, registry=registry)
        return _apply_project(source, node, registry=registry), ordering
    if isinstance(node, RelFilter):
        source, ordering = _compile_node(node.source, ctx=ctx, resolver=resolver, registry=registry)
        predicate = expr_ir_to_ibis(node.predicate, source, registry=registry)
        return source.filter(cast("BooleanValue", predicate)), Ordering.unordered()
    return None


def _ibis_join_kind(config: HashJoinConfig) -> str:
    """Map a hash join configuration to an Ibis join kind.

    Parameters
    ----------
    config
        Hash join configuration to map.

    Returns
    -------
    str
        Ibis join kind string.

    Raises
    ------
    ValueError
        Raised when the join type is unsupported.
    """
    join_type = config.join_type
    if join_type == "inner":
        return "inner"
    if join_type == "left outer":
        return "left"
    if join_type == "right outer":
        return "right"
    if join_type == "full outer":
        return "outer"
    if join_type in {"left semi", "right semi"}:
        return "semi"
    if join_type in {"left anti", "right anti"}:
        return "anti"
    msg = f"Unsupported join type for Ibis: {join_type!r}."
    raise ValueError(msg)


def _compile_join_node(
    node: RelJoin,
    *,
    ctx: ExecutionContext,
    resolver: PlanResolver[IbisPlan],
    registry: IbisExprRegistry,
) -> tuple[IbisTable, Ordering]:
    """Compile a join node into an Ibis table.

    Parameters
    ----------
    node
        Join node to compile.
    ctx
        Execution context for compilation.
    resolver
        Resolver for dataset references.
    registry
        Expression registry for IR translation.

    Returns
    -------
    tuple[IbisTable, Ordering]
        Compiled join table and ordering metadata.
    """
    left, _left_order = _compile_node(node.left, ctx=ctx, resolver=resolver, registry=registry)
    right, _right_order = _compile_node(node.right, ctx=ctx, resolver=resolver, registry=registry)
    joined = left.join(
        right,
        predicates=_join_predicates(left, right, node.join),
        how=cast("JoinKind", _ibis_join_kind(node.join)),
    )
    joined = _select_join_output(joined, left=left, right=right, config=node.join)
    return joined, Ordering.unordered()


def _compile_aggregate_node(
    node: RelAggregate,
    *,
    ctx: ExecutionContext,
    resolver: PlanResolver[IbisPlan],
    registry: IbisExprRegistry,
) -> tuple[IbisTable, Ordering]:
    """Compile an aggregate node into an Ibis table.

    Parameters
    ----------
    node
        Aggregate node to compile.
    ctx
        Execution context for compilation.
    resolver
        Resolver for dataset references.
    registry
        Expression registry for IR translation.

    Returns
    -------
    tuple[IbisTable, Ordering]
        Compiled aggregate table and ordering metadata.
    """
    source, _ordering = _compile_node(node.source, ctx=ctx, resolver=resolver, registry=registry)
    grouped = _apply_aggregate(source, node, registry=registry)
    return grouped, Ordering.unordered()


def _compile_union_node(
    node: RelUnion,
    *,
    ctx: ExecutionContext,
    resolver: PlanResolver[IbisPlan],
    registry: IbisExprRegistry,
) -> tuple[IbisTable, Ordering]:
    """Compile a union node into an Ibis table.

    Parameters
    ----------
    node
        Union node to compile.
    ctx
        Execution context for compilation.
    resolver
        Resolver for dataset references.
    registry
        Expression registry for IR translation.

    Returns
    -------
    tuple[IbisTable, Ordering]
        Compiled union table and ordering metadata.
    """
    inputs = [
        _compile_node(item, ctx=ctx, resolver=resolver, registry=registry)[0]
        for item in node.inputs
    ]
    aligned = _align_union_tables(inputs)
    unioned = aligned[0]
    for table in aligned[1:]:
        unioned = unioned.union(table)
    if node.distinct:
        unioned = unioned.distinct()
    return unioned, Ordering.unordered()


def _apply_project(
    table: IbisTable,
    node: RelProject,
    *,
    registry: IbisExprRegistry,
) -> IbisTable:
    """Apply a projection node to an Ibis table.

    Parameters
    ----------
    table
        Source Ibis table.
    node
        Projection node describing columns and derived expressions.
    registry
        Expression registry for IR translation.

    Returns
    -------
    IbisTable
        Projected Ibis table.
    """
    cols: list[IbisValue] = []
    if node.columns:
        cols.extend(table[col] for col in node.columns if col in table.columns)
    for name, expr in node.derived.items():
        cols.append(expr_ir_to_ibis(expr, table, registry=registry).name(name))
    if not cols:
        return table
    return table.select(cols)


def _join_predicates(
    left: IbisTable,
    right: IbisTable,
    config: HashJoinConfig,
) -> list[BooleanValue]:
    """Build join predicates for an Ibis join.

    Parameters
    ----------
    left
        Left Ibis table.
    right
        Right Ibis table.
    config
        Hash join configuration describing keys.

    Returns
    -------
    list[BooleanValue]
        Join predicates matching left/right key pairs.

    Raises
    ------
    ValueError
        Raised when join keys are missing or mismatched.
    """
    if not config.left_keys:
        msg = "HashJoinConfig requires left_keys."
        raise ValueError(msg)
    right_keys = config.resolved_right_keys()
    if len(right_keys) != len(config.left_keys):
        msg = "HashJoinConfig left/right key count mismatch."
        raise ValueError(msg)
    return [
        left[lkey] == right[rkey] for lkey, rkey in zip(config.left_keys, right_keys, strict=True)
    ]


def _select_join_output(
    joined: IbisTable,
    *,
    left: IbisTable,
    right: IbisTable,
    config: HashJoinConfig,
) -> IbisTable:
    """Select output columns from a joined Ibis table.

    Parameters
    ----------
    joined
        Joined Ibis table.
    left
        Left input table.
    right
        Right input table.
    config
        Join configuration including output selections and suffixes.

    Returns
    -------
    IbisTable
        Joined table with selected output columns.

    Raises
    ------
    ValueError
        Raised when join output selection is invalid.
    """
    left_cols = config.left_output or tuple(left.columns)
    right_cols = config.right_output or tuple(right.columns)
    collisions = set(left_cols) & set(right_cols)
    if collisions and not config.output_suffix_for_left and not config.output_suffix_for_right:
        msg = "HashJoinConfig output columns collide without suffixes."
        raise ValueError(msg)
    left_suffix = config.output_suffix_for_left
    right_suffix = config.output_suffix_for_right
    out_cols: list[IbisValue] = []
    for col in left_cols:
        name = f"{col}{left_suffix}" if col in collisions and left_suffix else col
        out_cols.append(joined[name])
    for col in right_cols:
        name = f"{col}{right_suffix}" if col in collisions and right_suffix else col
        out_cols.append(joined[name])
    return joined.select(out_cols)


def _apply_aggregate(
    table: IbisTable,
    node: RelAggregate,
    *,
    registry: IbisExprRegistry,
) -> IbisTable:
    """Apply aggregate specifications to an Ibis table.

    Parameters
    ----------
    table
        Source Ibis table.
    node
        Aggregate node with grouping and aggregate specs.
    registry
        Expression registry for IR translation.

    Returns
    -------
    IbisTable
        Aggregated Ibis table.
    """
    group_cols = [table[col] for col in node.group_by if col in table.columns]
    aggs: list[Scalar] = [
        _aggregate_expr(table, spec, registry=registry) for spec in node.aggregates
    ]
    if group_cols:
        return table.group_by(group_cols).aggregate(aggs)
    return table.aggregate(aggs)


def _aggregate_expr(
    table: IbisTable,
    spec: AggregateExpr,
    *,
    registry: IbisExprRegistry,
) -> Scalar:
    """Compile an aggregate expression into an Ibis scalar.

    Parameters
    ----------
    table
        Source Ibis table.
    spec
        Aggregate expression specification.
    registry
        Expression registry for IR translation.

    Returns
    -------
    Scalar
        Named aggregate scalar expression.

    Raises
    ------
    TypeError
        Raised when the aggregate function is unsupported.
    ValueError
        Raised when the aggregate expression has no arguments.
    """
    if not spec.args:
        msg = f"Aggregate {spec.name!r} missing arguments."
        raise ValueError(msg)
    arg = spec.args[0]
    expr = expr_ir_to_ibis(arg, table, registry=registry)
    func = getattr(expr, spec.func, None)
    if not callable(func):
        msg = f"Aggregate function {spec.func!r} is not supported."
        raise TypeError(msg)
    value = func(distinct=True) if spec.distinct else func()
    scalar = cast("Scalar", value)
    return cast("Scalar", scalar.name(spec.name))


def _align_union_tables(tables: Sequence[IbisTable]) -> list[IbisTable]:
    """Align union inputs to a shared schema order.

    Parameters
    ----------
    tables
        Tables to align before union.

    Returns
    -------
    list[IbisTable]
        Tables with matching column order and types.

    Raises
    ------
    ValueError
        Raised when no union inputs are provided.
    """
    if not tables:
        msg = "Union requires at least one table."
        raise ValueError(msg)
    names: list[str] = []
    types: dict[str, DataType] = {}
    for table in tables:
        schema = table.schema()
        schema_names = cast("Sequence[str]", schema.names)
        schema_types = cast("Sequence[DataType]", schema.types)
        for name, dtype in zip(schema_names, schema_types, strict=True):
            if name in types:
                continue
            names.append(name)
            types[name] = dtype
    aligned: list[IbisTable] = []
    for table in tables:
        cols: list[IbisValue] = []
        for name in names:
            if name in table.columns:
                cols.append(table[name])
            else:
                cols.append(ibis.literal(None, type=types[name]).name(name))
        aligned.append(table.select(cols))
    return aligned


__all__ = [
    "IbisRelPlanCompiler",
    "PlanResolver",
    "RelPlanCompiler",
]
