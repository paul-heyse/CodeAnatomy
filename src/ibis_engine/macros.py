"""Ibis macro helpers for deferred and selector-based transforms."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
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


@dataclass
class MacroLibrary:
    """Registry for reusable Ibis macros."""

    macros: dict[str, IbisMacroSpec] = field(default_factory=dict)

    def register(self, name: str, macro: IbisMacroSpec) -> None:
        """Register a macro by name.

        Parameters
        ----------
        name:
            Macro name.
        macro:
            Macro specification to register.

        Raises
        ------
        ValueError
            Raised when the name is empty.
        """
        if not name:
            msg = "MacroLibrary.register requires a non-empty name."
            raise ValueError(msg)
        self.macros[name] = macro

    def resolve(self, name: str) -> IbisMacroSpec:
        """Return a registered macro by name.

        Parameters
        ----------
        name:
            Macro name.

        Returns
        -------
        IbisMacroSpec
            Registered macro specification.

        Raises
        ------
        KeyError
            Raised when the macro name is unknown.
        """
        if name not in self.macros:
            msg = f"MacroLibrary: unknown macro {name!r}."
            raise KeyError(msg)
        return self.macros[name]


_DEFAULT_MACRO_LIBRARY = MacroLibrary()


def macro_library() -> MacroLibrary:
    """Return the shared macro library.

    Returns
    -------
    MacroLibrary
        Shared macro library instance.
    """
    return _DEFAULT_MACRO_LIBRARY


__all__ = [
    "IbisMacro",
    "IbisMacroRewrite",
    "IbisMacroSpec",
    "MacroLibrary",
    "apply_macros",
    "bind_columns",
    "macro_library",
]
