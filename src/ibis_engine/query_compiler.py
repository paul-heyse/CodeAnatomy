"""QuerySpec-style compilation into Ibis expressions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import cast

from ibis.expr.types import BooleanValue, Table, Value

from ibis_engine.expr_compiler import (
    ExprIRLike,
    IbisExprRegistry,
    default_expr_registry,
    expr_ir_to_ibis,
)
from ibis_engine.macros import IbisMacroSpec, apply_macros


@dataclass(frozen=True)
class IbisProjectionSpec:
    """Projection spec for Ibis query compilation."""

    base: tuple[str, ...]
    derived: Mapping[str, ExprIRLike] = field(default_factory=dict)


@dataclass(frozen=True)
class IbisQuerySpec:
    """Declarative query spec for Ibis execution."""

    projection: IbisProjectionSpec
    predicate: ExprIRLike | None = None
    pushdown_predicate: ExprIRLike | None = None
    macros: tuple[IbisMacroSpec, ...] = ()

    @staticmethod
    def simple(*cols: str) -> IbisQuerySpec:
        """Return a simple query spec from column names.

        Returns
        -------
        IbisQuerySpec
            Query spec with base columns only.
        """
        return IbisQuerySpec(projection=IbisProjectionSpec(base=tuple(cols)))


def apply_query_spec(
    table: Table,
    *,
    spec: IbisQuerySpec,
    registry: IbisExprRegistry | None = None,
) -> Table:
    """Apply a query spec to an Ibis table.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table with filters and projections applied.
    """
    registry = registry or default_expr_registry()
    if spec.macros:
        table = apply_macros(table, macros=spec.macros)
    table = _apply_derived(table, spec.projection.derived, registry=registry)
    cols = _projection_columns(table, spec.projection.base, spec.projection.derived)
    if cols:
        table = table.select(cols)
    if spec.pushdown_predicate is not None:
        predicate = expr_ir_to_ibis(spec.pushdown_predicate, table, registry=registry)
        table = table.filter(cast("BooleanValue", predicate))
    if spec.predicate is not None:
        predicate = expr_ir_to_ibis(spec.predicate, table, registry=registry)
        table = table.filter(cast("BooleanValue", predicate))
    return table


def apply_projection(
    table: Table,
    *,
    base: Sequence[str],
    derived: Mapping[str, ExprIRLike] | None = None,
    registry: IbisExprRegistry | None = None,
) -> Table:
    """Apply a projection with optional derived columns.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table with projection applied.
    """
    derived = derived or {}
    registry = registry or default_expr_registry()
    table = _apply_derived(table, derived, registry=registry)
    cols = _projection_columns(table, base, derived)
    if not cols:
        return table
    return table.select(cols)


def _apply_derived(
    table: Table,
    derived: Mapping[str, ExprIRLike],
    *,
    registry: IbisExprRegistry,
) -> Table:
    out = table
    for name, expr in derived.items():
        out = out.mutate(**{name: expr_ir_to_ibis(expr, out, registry=registry)})
    return out


def _projection_columns(
    table: Table,
    base: Sequence[str],
    derived: Mapping[str, ExprIRLike],
) -> list[Value]:
    cols: list[Value] = [table[name] for name in base if name in table.columns]
    cols.extend(table[name] for name in derived if name in table.columns)
    return cols
