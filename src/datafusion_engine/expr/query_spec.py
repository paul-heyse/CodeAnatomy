"""Declarative query specs for DataFusion execution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import msgspec
from datafusion import col
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr

from datafusion_engine.expr.spec import ExprIR, ExprSpec, scalar_literal
from serde_msgspec import StructBaseStrict


class ProjectionSpec(StructBaseStrict, frozen=True):
    """Projection spec for DataFusion query compilation."""

    base: tuple[str, ...]
    derived: Mapping[str, ExprSpec] = msgspec.field(default_factory=dict)


class QuerySpec(StructBaseStrict, frozen=True):
    """Declarative query spec for DataFusion execution."""

    projection: ProjectionSpec
    predicate: ExprSpec | None = None
    pushdown_predicate: ExprSpec | None = None

    @staticmethod
    def simple(*cols: str) -> QuerySpec:
        """Return a simple query spec from column names.

        Returns:
        -------
        QuerySpec
            Query spec with only base projection columns.
        """
        return QuerySpec(projection=ProjectionSpec(base=tuple(cols)))


def query_for_schema(schema_names: Sequence[str]) -> QuerySpec:
    """Return a QuerySpec projecting the schema columns.

    Returns:
    -------
    QuerySpec
        Query spec projecting the schema fields.
    """
    return QuerySpec(projection=ProjectionSpec(base=tuple(schema_names)))


def apply_query_spec(df: DataFrame, *, spec: QuerySpec) -> DataFrame:
    """Apply a query spec to a DataFusion DataFrame.

    Returns:
    -------
    datafusion.dataframe.DataFrame
        DataFrame with projections and predicates applied.
    """
    df = _apply_derived(df, spec.projection.derived)
    cols = _projection_columns(spec.projection.base, spec.projection.derived)
    if cols:
        df = df.select(*cols)
    if spec.pushdown_predicate is not None:
        df = _apply_predicate(df, spec.pushdown_predicate)
    if spec.predicate is not None:
        df = _apply_predicate(df, spec.predicate)
    return df


def apply_projection(
    df: DataFrame,
    *,
    base: Sequence[str],
    derived: Mapping[str, ExprSpec] | None = None,
) -> DataFrame:
    """Apply a projection with optional derived columns.

    Returns:
    -------
    datafusion.dataframe.DataFrame
        DataFrame with projected and derived columns applied.
    """
    spec = QuerySpec(
        projection=ProjectionSpec(base=tuple(base), derived=derived or {}),
    )
    return apply_query_spec(df, spec=spec)


def _apply_derived(df: DataFrame, derived: Mapping[str, ExprSpec]) -> DataFrame:
    if not derived:
        return df
    derived_exprs = [spec.to_expr().alias(name) for name, spec in derived.items()]
    base_exprs = [col(name) for name in df.schema().names]
    return df.select(*base_exprs, *derived_exprs)


def _projection_columns(
    base: Sequence[str],
    derived: Mapping[str, ExprSpec],
) -> list[Expr]:
    cols: list[Expr] = []
    seen: set[str] = set()
    for name in base:
        if name not in seen:
            cols.append(col(name))
            seen.add(name)
    for name in derived:
        if name not in seen:
            cols.append(col(name))
            seen.add(name)
    return cols


def _apply_predicate(df: DataFrame, predicate: ExprSpec) -> DataFrame:
    compiled = predicate.to_expr()
    return df.filter(compiled)


def false_predicate() -> ExprSpec:
    """Return a predicate that is always false.

    Returns:
    -------
    ExprSpec
        Expression spec that evaluates to false.
    """
    return ExprSpec(expr_ir=ExprIR(op="literal", value=scalar_literal(value=False)))


def in_set_expr(name: str, values: Sequence[str]) -> ExprSpec:
    """Return an in-set predicate expression spec.

    Returns:
    -------
    ExprSpec
        Expression spec representing an IN predicate.
    """
    if not values:
        return false_predicate()
    args = (
        ExprIR(op="field", name=name),
        *(ExprIR(op="literal", value=scalar_literal(value)) for value in values),
    )
    return ExprSpec(expr_ir=ExprIR(op="call", name="in_set", args=args))


__all__ = [
    "ProjectionSpec",
    "QuerySpec",
    "apply_projection",
    "apply_query_spec",
    "false_predicate",
    "in_set_expr",
    "query_for_schema",
]
