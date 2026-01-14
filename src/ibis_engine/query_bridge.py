"""Adapters for bridging ArrowDSL QuerySpec and ExprSpec to Ibis."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from arrowdsl.compute.expr_core import ExprSpec
from arrowdsl.compute.macros import (
    CoalesceExpr,
    CoalesceStringExprSpec,
    ConstExpr,
    FieldExpr,
    TrimExprSpec,
)
from arrowdsl.core.interop import ScalarLike
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.spec.expr_ir import ExprIR
from ibis_engine.query_compiler import IbisProjectionSpec, IbisQuerySpec


@dataclass(frozen=True)
class QueryBridgeResult:
    """Bridge result for a QuerySpec conversion."""

    ibis_spec: IbisQuerySpec
    kernel_derived: Mapping[str, ExprSpec] = field(default_factory=dict)
    kernel_predicate: ExprSpec | None = None
    kernel_pushdown: ExprSpec | None = None

    def has_kernel_fallback(self) -> bool:
        """Return whether kernel-lane fallbacks are required.

        Returns
        -------
        bool
            ``True`` when kernel-lane fallbacks are required.
        """
        return bool(self.kernel_derived or self.kernel_predicate or self.kernel_pushdown)


def queryspec_to_ibis(spec: QuerySpec) -> QueryBridgeResult:
    """Convert a legacy QuerySpec into an IbisQuerySpec plus fallbacks.

    Returns
    -------
    QueryBridgeResult
        Bridge result with IbisQuerySpec and kernel fallbacks.
    """
    derived: dict[str, ExprIR] = {}
    kernel_derived: dict[str, ExprSpec] = {}
    for name, expr in spec.projection.derived.items():
        ir = expr_spec_to_ir(expr)
        if ir is None:
            kernel_derived[name] = expr
        else:
            derived[name] = ir
    predicate_ir = expr_spec_to_ir(spec.predicate) if spec.predicate is not None else None
    pushdown_ir = (
        expr_spec_to_ir(spec.pushdown_predicate) if spec.pushdown_predicate is not None else None
    )
    return QueryBridgeResult(
        ibis_spec=IbisQuerySpec(
            projection=IbisProjectionSpec(base=spec.projection.base, derived=derived),
            predicate=predicate_ir,
            pushdown_predicate=pushdown_ir,
        ),
        kernel_derived=kernel_derived,
        kernel_predicate=spec.predicate if predicate_ir is None else None,
        kernel_pushdown=spec.pushdown_predicate if pushdown_ir is None else None,
    )


def projection_to_ibis(projection: ProjectionSpec) -> QueryBridgeResult:
    """Convert a ProjectionSpec into an IbisQuerySpec bridge.

    Returns
    -------
    QueryBridgeResult
        Bridge result with IbisQuerySpec and kernel fallbacks.
    """
    return queryspec_to_ibis(
        QuerySpec(
            projection=projection,
            predicate=None,
            pushdown_predicate=None,
        )
    )


def expr_spec_to_ir(expr: ExprSpec | None) -> ExprIR | None:
    """Convert supported ExprSpec into ExprIR.

    Returns ``None`` when the expression must be handled in the kernel lane.

    Returns
    -------
    ExprIR | None
        ExprIR representation when supported, otherwise ``None``.
    """
    result: ExprIR | None = None
    if expr is not None:
        if isinstance(expr, FieldExpr):
            result = ExprIR(op="field", name=expr.name)
        elif isinstance(expr, ConstExpr):
            value = expr.value
            if (
                isinstance(value, (bool, int, float, str, bytes))
                or value is None
                or isinstance(value, ScalarLike)
            ):
                result = ExprIR(op="literal", value=value)
        elif isinstance(expr, TrimExprSpec):
            result = ExprIR(
                op="call",
                name="strip",
                args=(ExprIR(op="field", name=expr.column),),
            )
        elif isinstance(expr, CoalesceStringExprSpec):
            args = tuple(ExprIR(op="field", name=col) for col in expr.columns)
            result = ExprIR(op="call", name="coalesce", args=args)
        elif isinstance(expr, CoalesceExpr):
            args = tuple(expr_spec_to_ir(item) for item in expr.exprs)
            if not any(arg is None for arg in args):
                result = ExprIR(op="call", name="coalesce", args=tuple(arg for arg in args if arg))
    return result


__all__ = [
    "QueryBridgeResult",
    "expr_spec_to_ir",
    "projection_to_ibis",
    "queryspec_to_ibis",
]
