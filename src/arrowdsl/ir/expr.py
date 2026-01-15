"""Canonical expression IR for ArrowDSL fallback compilation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from arrowdsl.compute.options import serialize_options

ExprOp = Literal["field", "literal", "call"]

if TYPE_CHECKING:
    from arrowdsl.spec.expr_ir import ExprIR


@dataclass(frozen=True)
class ExprNode:
    """Canonical expression node used for fallback compilation."""

    op: ExprOp
    name: str | None = None
    value: object | None = None
    args: tuple[ExprNode, ...] = ()
    options: bytes | None = None

    def validate(self) -> None:
        """Validate that required fields are present for this node.

        Raises
        ------
        ValueError
            Raised when required fields are missing.
        """
        if self.op == "field" and not self.name:
            msg = "ExprNode(field) requires name."
            raise ValueError(msg)
        if self.op == "call" and not self.name:
            msg = "ExprNode(call) requires name."
            raise ValueError(msg)


def expr_from_expr_ir(ir: ExprIR) -> ExprNode:
    """Convert a legacy ExprIR node into a canonical ExprNode.

    Returns
    -------
    ExprNode
        Canonical expression node.

    Raises
    ------
    ValueError
        Raised when ExprIR options cannot be serialized.
    """
    try:
        options = serialize_options(ir.options)
    except TypeError as exc:
        msg = "ExprIR options must be FunctionOptions or serialized bytes."
        raise ValueError(msg) from exc
    return ExprNode(
        op=cast("ExprOp", ir.op),
        name=ir.name,
        value=ir.value,
        args=tuple(expr_from_expr_ir(arg) for arg in ir.args),
        options=options,
    )


__all__ = ["ExprNode", "ExprOp", "expr_from_expr_ir"]
