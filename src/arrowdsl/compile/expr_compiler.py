"""Expression compiler for ArrowDSL fallback lanes."""

from __future__ import annotations

from typing import Protocol, cast

from arrowdsl.compute.options import deserialize_options
from arrowdsl.core.interop import ComputeExpression, call_expression_function, pc
from arrowdsl.ir.expr import ExprNode


class _FunctionOptionsProto(Protocol):
    @classmethod
    def deserialize(cls, payload: bytes) -> _FunctionOptionsProto:
        raise NotImplementedError


class ExprCompiler:
    """Compile ExprNode trees into Arrow compute expressions."""

    def __init__(self, *, registry: object | None = None) -> None:
        self._registry = registry

    def to_compute_expr(self, expr: ExprNode) -> ComputeExpression:
        """Compile an ExprNode to a compute expression.

        Returns
        -------
        ComputeExpression
            Compiled compute expression.

        Raises
        ------
        ValueError
            Raised when the ExprNode is missing required fields or has an unsupported op.
        """
        expr.validate()
        if expr.op == "field":
            if expr.name is None:
                msg = "ExprNode(field) requires name."
                raise ValueError(msg)
            return pc.field(expr.name)
        if expr.op == "literal":
            return pc.scalar(expr.value)
        if expr.op == "call":
            if expr.name is None:
                msg = "ExprNode(call) requires name."
                raise ValueError(msg)
            name = expr.name
            if self._registry is not None:
                ensure = getattr(self._registry, "ensure", None)
                if callable(ensure):
                    name = cast("str", ensure(name))
            args = [self.to_compute_expr(arg) for arg in expr.args]
            opts = cast("_FunctionOptionsProto | None", deserialize_options(expr.options))
            return call_expression_function(name, args, options=opts)
        msg = f"Unsupported ExprNode op: {expr.op!r}."
        raise ValueError(msg)


__all__ = ["ExprCompiler"]
