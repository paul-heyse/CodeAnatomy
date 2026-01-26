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

    Supports SelectorPattern objects, Ibis selectors, deferred expressions,
    column names (strings), and column indices (integers).

    Parameters
    ----------
    table:
        Table to bind expressions to.
    *selectors:
        Column selectors - can be SelectorPattern, Ibis selector,
        Deferred expression, string column name, or integer column index.

    Returns
    -------
    tuple[ibis.expr.types.Value, ...]
        Bound expressions for the selectors.

    Examples
    --------
    Bind multiple selector types:

    >>> from ibis_engine.selector_utils import SelectorPattern, ColumnSelector
    >>> from ibis import _
    >>> cs = ColumnSelector()
    >>> # Bind numeric columns, a specific column, and a deferred expression
    >>> bound = bind_columns(table, cs.numeric(), "id", _.x + 1)
    """
    from ibis_engine.selector_utils import SelectorPattern, selector_from_pattern

    # Convert SelectorPattern objects to Ibis selectors
    converted_selectors: list[object] = []
    for selector in selectors:
        if isinstance(selector, SelectorPattern):
            converted_selectors.append(selector_from_pattern(selector))
        else:
            converted_selectors.append(selector)

    bound = table.bind(*converted_selectors)
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


def apply_across(
    table: Table,
    selector: object,
    fn: Callable[[Value], Value],
) -> Table:
    """Apply a transformation across selected columns.

    Parameters
    ----------
    table:
        Table to transform.
    selector:
        Column selector - can be SelectorPattern, Ibis selector,
        or any selector supported by bind_columns.
    fn:
        Function to apply to each selected column.

    Returns
    -------
    Table
        Table with transformed columns added.

    Examples
    --------
    Normalize all numeric columns:

    >>> from ibis_engine.selector_utils import ColumnSelector
    >>> cs = ColumnSelector()
    >>> normalized = apply_across(table, cs.numeric(), lambda col: (col - col.mean()) / col.std())

    Apply transformation with systematic naming:

    >>> import ibis.selectors as s
    >>> from ibis import _
    >>> centered = apply_across(table, s.numeric(), lambda col: col - col.mean())
    """
    from ibis_engine.selector_utils import SelectorPattern, selector_from_pattern

    # Convert SelectorPattern to Ibis selector if needed
    if isinstance(selector, SelectorPattern):
        selector = selector_from_pattern(selector)

    # Bind selector to get columns, then apply function to each
    bound_cols = bind_columns(table, selector)
    mutations: dict[str, Value] = {col.get_name(): fn(col) for col in bound_cols}
    return table.mutate(**mutations)


def with_deferred(table: Table, **named_exprs: object) -> Table:
    """Add computed columns using deferred expressions.

    Parameters
    ----------
    table:
        Table to add columns to.
    **named_exprs:
        Named expressions (deferred or Value) to add as columns.

    Returns
    -------
    Table
        Table with new computed columns.

    Examples
    --------
    Add hash and concatenated columns:

    >>> from ibis_engine.selector_utils import deferred_hash_column, deferred_concat_columns
    >>> table_with_cols = with_deferred(
    ...     table,
    ...     id_hash=deferred_hash_column("id"),
    ...     full_name=deferred_concat_columns("first_name", "last_name", separator=" "),
    ... )
    """
    from ibis import Deferred

    from ibis_engine.selector_utils import bind_deferred

    mutations: dict[str, Value] = {}
    for name, expr in named_exprs.items():
        if isinstance(expr, Deferred):
            mutations[name] = bind_deferred(table, expr)[0]
        else:
            mutations[name] = cast("Value", expr)
    return table.mutate(**mutations)


__all__ = [
    "IbisMacro",
    "IbisMacroRewrite",
    "IbisMacroSpec",
    "MacroLibrary",
    "apply_across",
    "apply_macros",
    "bind_columns",
    "macro_library",
    "with_deferred",
]
