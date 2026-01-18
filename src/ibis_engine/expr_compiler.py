"""ExprIR to Ibis expression compiler."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol, cast

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
from ibis.expr.types import BooleanValue, Scalar, StringValue, Table, Value

from ibis_engine.builtin_udfs import (
    col_to_byte,
    cpg_score,
    position_encoding_norm,
    stable_hash64,
    stable_hash128,
)

IbisExprFn = Callable[..., Value]


def _fill_null_expr(value: Value, fill_value: Scalar) -> Value:
    return value.fill_null(fill_value)


def _if_else_expr(cond: Value, true_value: Value, false_value: Value) -> Value:
    return ibis.ifelse(cond, true_value, false_value)


def _equal_expr(left: Value, right: Value) -> Value:
    return left == right


def _not_equal_expr(left: Value, right: Value) -> Value:
    return left != right


def _invert_expr(value: BooleanValue) -> BooleanValue:
    return ~value


def _bitwise_and_expr(left: BooleanValue, right: BooleanValue) -> BooleanValue:
    return left & right


def _strip_expr(value: StringValue) -> StringValue:
    return value.strip()


def _stringify_expr(value: Value) -> StringValue:
    return value.cast("string")


def _is_null_expr(value: Value) -> BooleanValue:
    return value.isnull()


class OperationSupportBackend(Protocol):
    """Protocol for backends exposing operation support checks."""

    def has_operation(self, operation: type[ops.Value[dt.DataType]], /) -> bool:
        """Return whether the backend supports an operation type."""
        ...


class ExprIRLike(Protocol):
    """Protocol for ExprIR-compatible nodes."""

    @property
    def op(self) -> str:
        """Return the operation name."""
        ...

    @property
    def name(self) -> str | None:
        """Return the operation name or field identifier."""
        ...

    @property
    def value(self) -> object | None:
        """Return the literal value when present."""
        ...

    @property
    def args(self) -> Sequence[ExprIRLike]:
        """Return child expression nodes."""
        ...

    @property
    def options(self) -> object | None:
        """Return optional call options payload."""
        ...


@dataclass(frozen=True)
class IbisExprRegistry:
    """Registry for mapping ExprIR call names to Ibis functions."""

    functions: Mapping[str, IbisExprFn] = field(default_factory=dict)

    def resolve(self, name: str) -> IbisExprFn:
        """Return the Ibis function for a given call name.

        Returns
        -------
        IbisExprFn
            Callable that produces an Ibis expression.

        Raises
        ------
        KeyError
            Raised when the function name cannot be resolved.
        """
        fn = self.functions.get(name)
        if fn is not None:
            return fn
        fallback = getattr(ibis, name, None)
        if callable(fallback):
            return cast("IbisExprFn", fallback)
        msg = f"Unsupported Ibis function: {name!r}."
        raise KeyError(msg)


def default_expr_registry() -> IbisExprRegistry:
    """Return a registry with project-specific helpers registered.

    Returns
    -------
    IbisExprRegistry
        Registry with default helper mappings.
    """
    return IbisExprRegistry(
        functions={
            "fill_null": _fill_null_expr,
            "if_else": _if_else_expr,
            "equal": _equal_expr,
            "not_equal": _not_equal_expr,
            "invert": _invert_expr,
            "bit_wise_and": _bitwise_and_expr,
            "strip": _strip_expr,
            "stringify": _stringify_expr,
            "is_null": _is_null_expr,
            "cpg_score": cpg_score,
            "stable_hash64": stable_hash64,
            "stable_hash128": stable_hash128,
            "position_encoding_norm": position_encoding_norm,
            "col_to_byte": col_to_byte,
        }
    )


def expr_ir_to_ibis(
    expr: ExprIRLike,
    table: Table,
    *,
    registry: IbisExprRegistry | None = None,
) -> Value:
    """Compile an ExprIR-like node into an Ibis expression.

    Returns
    -------
    ibis.expr.types.Value
        Ibis expression for the provided ExprIR node.

    Raises
    ------
    ValueError
        Raised when the node cannot be compiled.
    """
    registry = registry or default_expr_registry()
    if expr.op == "field":
        if expr.name is None:
            msg = "ExprIR field op requires name."
            raise ValueError(msg)
        return table[expr.name]
    if expr.op == "literal":
        return ibis.literal(expr.value)
    if expr.op == "call":
        if expr.name is None:
            msg = "ExprIR call op requires name."
            raise ValueError(msg)
        if expr.options is not None:
            msg = "ExprIR options are not supported in the Ibis compiler."
            raise ValueError(msg)
        fn = registry.resolve(expr.name)
        args = [expr_ir_to_ibis(arg, table, registry=registry) for arg in expr.args]
        return fn(*args)
    msg = f"Unsupported ExprIR op: {expr.op!r}."
    raise ValueError(msg)


def unsupported_operations(
    expr: Value | Table,
    *,
    backend: OperationSupportBackend,
) -> tuple[str, ...]:
    """Return unsupported operation names for a backend.

    Returns
    -------
    tuple[str, ...]
        Sorted operation class names not supported by the backend.
    """
    has_op = getattr(backend, "has_operation", None)
    if not callable(has_op):
        return ()
    missing: set[str] = set()
    try:
        nodes = expr.op().find(ops.Value)
    except AttributeError:
        return ()
    for node in nodes:
        op_type = type(node)
        try:
            supported = has_op(op_type)
        except NotImplementedError:
            return ()
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return ()
        if not supported:
            missing.add(op_type.__name__)
    return tuple(sorted(missing))


__all__ = [
    "ExprIRLike",
    "IbisExprFn",
    "IbisExprRegistry",
    "OperationSupportBackend",
    "default_expr_registry",
    "expr_ir_to_ibis",
    "unsupported_operations",
]
