"""Ibis macro helpers for deferred and selector-based transforms."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import cast

from ibis.expr.types import Table, Value

IbisMacro = Callable[[Table], Table]


@dataclass(frozen=True)
class IbisMacroRewrite:
    """Rewrite macro expressed as an Ibis substitution mapping."""

    substitutions: Mapping[Value, Value]


IbisMacroSpec = IbisMacro | IbisMacroRewrite


def apply_macros(table: Table, *, macros: Sequence[IbisMacroSpec]) -> Table:
    """Apply a sequence of macros to an Ibis table.

    Returns
    -------
    ibis.expr.types.Table
        Table after applying each macro in order.

    Raises
    ------
    TypeError
        Raised when the table lacks required macro helpers.
    """
    out = table
    for macro in macros:
        if isinstance(macro, IbisMacroRewrite):
            substitute = getattr(out, "substitute", None)
            if not callable(substitute):
                msg = "Ibis table does not support substitute."
                raise TypeError(msg)
            out = cast("Table", substitute(macro.substitutions))
            continue
        pipe = getattr(out, "pipe", None)
        if not callable(pipe):
            msg = "Ibis table does not support pipe."
            raise TypeError(msg)
        out = cast("Table", pipe(macro))
    return out


def bind_columns(table: Table, *selectors: object) -> tuple[Value, ...]:
    """Bind deferred/selector expressions to a table.

    Returns
    -------
    tuple[ibis.expr.types.Value, ...]
        Bound expressions for the selectors.
    """
    bound = table.bind(*selectors)
    return cast("tuple[Value, ...]", tuple(bound))


__all__ = ["IbisMacro", "IbisMacroRewrite", "IbisMacroSpec", "apply_macros", "bind_columns"]
