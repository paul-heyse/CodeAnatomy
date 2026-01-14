"""ExprIR to Ibis expression compiler."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol, cast

import ibis
from ibis.expr.types import Table, Value

from ibis_engine.builtin_udfs import (
    col_to_byte,
    cpg_score,
    position_encoding_norm,
    stable_hash64,
    stable_hash128,
)

IbisExprFn = Callable[..., Value]


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
            "strip": lambda value: value.strip(),
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
