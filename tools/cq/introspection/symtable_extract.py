"""Symtable extraction for Python scope analysis.

Extracts scope information using Python's symtable module.
"""

from __future__ import annotations

import logging
import symtable
import sys
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class ScopeType(Enum):
    """Types of Python scopes."""

    MODULE = "module"
    FUNCTION = "function"
    CLASS = "class"
    ANNOTATION = "annotation"
    TYPE_ALIAS = "type_alias"
    TYPE_PARAMETERS = "type_parameters"
    TYPE_VARIABLE = "type_variable"


@dataclass(frozen=True)
class SymbolFact:
    """Information about a symbol in a scope.

    Attributes
    ----------
    name
        Symbol name
    is_local
        Whether symbol is local to the scope
    is_global
        Whether symbol is declared global
    is_free
        Whether symbol is a free variable (closure capture)
    is_cell
        Whether symbol is a cell variable (captured by nested scope)
    is_referenced
        Whether symbol is referenced
    is_assigned
        Whether symbol is assigned
    is_parameter
        Whether symbol is a function parameter
    is_imported
        Whether symbol is imported
    """

    name: str
    is_local: bool = False
    is_global: bool = False
    is_free: bool = False
    is_cell: bool = False
    is_referenced: bool = False
    is_assigned: bool = False
    is_parameter: bool = False
    is_imported: bool = False


@dataclass(frozen=True)
class ScopeFact:
    """Information about a scope (function, class, module).

    Attributes
    ----------
    name
        Scope name
    scope_type
        Type of scope (function, class, module, etc.)
    lineno
        Line number where scope starts (0 if unknown)
    symbols
        Symbols defined in this scope
    children
        Names of child scopes
    is_nested
        Whether this is a nested scope
    is_optimized
        Whether this scope uses fast locals
    has_free_vars
        Whether this scope has free variables (is a closure)
    has_cell_vars
        Whether this scope has cell variables (captures vars for children)
    free_vars
        Names of free variables
    cell_vars
        Names of cell variables
    """

    name: str
    scope_type: ScopeType
    lineno: int = 0
    symbols: tuple[SymbolFact, ...] = ()
    children: tuple[str, ...] = ()
    is_nested: bool = False
    is_optimized: bool = False
    has_free_vars: bool = False
    has_cell_vars: bool = False
    free_vars: tuple[str, ...] = ()
    cell_vars: tuple[str, ...] = ()


@dataclass
class ScopeGraph:
    """Graph of scopes in a Python file.

    Attributes
    ----------
    filename
        Source filename
    scopes
        All scopes in the file
    scope_by_name
        Lookup by scope name
    root_scope
        The module-level scope
    """

    filename: str
    scopes: list[ScopeFact] = field(default_factory=list)
    scope_by_name: dict[str, ScopeFact] = field(default_factory=dict)
    root_scope: ScopeFact | None = None


def extract_scope_graph(source: str, filename: str) -> ScopeGraph:
    """Extract scope graph from Python source code.

    Parameters
    ----------
    source
        Python source code
    filename
        Filename for error reporting

    Returns
    -------
    ScopeGraph
        Extracted scope information.
    """
    graph = ScopeGraph(filename=filename)

    try:
        table = symtable.symtable(source, filename, "exec")
    except SyntaxError as exc:
        logger.debug("Syntax error in %s: %s", filename, exc)
        return graph
    except (ValueError, TypeError) as exc:
        logger.warning("Failed to build symtable for %s: %s", filename, exc)
        return graph

    # Walk the symbol table
    _walk_table(table, graph, is_nested=False)

    # Set root scope
    if graph.scopes:
        graph.root_scope = graph.scopes[0]

    return graph


def _walk_table(
    table: symtable.SymbolTable,
    graph: ScopeGraph,
    is_nested: bool,
) -> None:
    """Recursively walk symbol table and extract scope facts."""
    # Determine scope type
    scope_type = _get_scope_type(table)

    # Extract symbols
    symbols: list[SymbolFact] = []
    free_vars: list[str] = []
    cell_vars: list[str] = []

    for sym in table.get_symbols():
        fact = _extract_symbol_fact(sym)
        symbols.append(fact)

        if fact.is_free:
            free_vars.append(fact.name)
        if fact.is_cell:
            cell_vars.append(fact.name)

    # Get child scope names
    children = tuple(child.get_name() for child in table.get_children())

    # Get line number if available (Python 3.12+)
    lineno = 0
    if hasattr(table, "get_lineno"):
        try:
            lineno = table.get_lineno()
        except AttributeError:
            pass

    # Determine if scope is optimized (uses fast locals)
    is_optimized = False
    if isinstance(table, symtable.Function):
        is_optimized = table.is_optimized()

    scope = ScopeFact(
        name=table.get_name(),
        scope_type=scope_type,
        lineno=lineno,
        symbols=tuple(symbols),
        children=children,
        is_nested=is_nested,
        is_optimized=is_optimized,
        has_free_vars=len(free_vars) > 0,
        has_cell_vars=len(cell_vars) > 0,
        free_vars=tuple(sorted(free_vars)),
        cell_vars=tuple(sorted(cell_vars)),
    )

    graph.scopes.append(scope)
    graph.scope_by_name[scope.name] = scope

    # Process children
    for child in table.get_children():
        _walk_table(child, graph, is_nested=True)


def _get_scope_type(table: symtable.SymbolTable) -> ScopeType:
    """Determine scope type from symbol table."""
    # Python 3.12+ has get_type() returning an enum
    if sys.version_info >= (3, 12):
        try:
            type_str = table.get_type()
            # Handle both string and enum
            if hasattr(type_str, "value"):
                type_str = type_str.value
            type_str = str(type_str).lower()
        except AttributeError:
            type_str = "module"
    else:
        # Pre-3.12: get_type() returns a string
        type_str = table.get_type().lower()

    type_map = {
        "module": ScopeType.MODULE,
        "function": ScopeType.FUNCTION,
        "class": ScopeType.CLASS,
        "annotation": ScopeType.ANNOTATION,
        "typealias": ScopeType.TYPE_ALIAS,
        "type_alias": ScopeType.TYPE_ALIAS,
        "typeparameters": ScopeType.TYPE_PARAMETERS,
        "type_parameters": ScopeType.TYPE_PARAMETERS,
        "typevariable": ScopeType.TYPE_VARIABLE,
        "type_variable": ScopeType.TYPE_VARIABLE,
    }

    return type_map.get(type_str, ScopeType.MODULE)


def _extract_symbol_fact(sym: symtable.Symbol) -> SymbolFact:
    """Extract fact from a symbol."""
    return SymbolFact(
        name=sym.get_name(),
        is_local=sym.is_local(),
        is_global=sym.is_global(),
        is_free=sym.is_free(),
        is_cell=_is_cell(sym),
        is_referenced=sym.is_referenced(),
        is_assigned=sym.is_assigned(),
        is_parameter=sym.is_parameter(),
        is_imported=sym.is_imported(),
    )


def _is_cell(sym: symtable.Symbol) -> bool:
    """Check if symbol is a cell variable.

    Cell variables are captured by nested scopes (closures).
    """
    # In Python, a cell variable is local but also free in a nested scope
    # We can detect this by checking if it's not free but is referenced
    # in a nested scope's free vars
    # For simplicity, we check if it's a local that is declared nonlocal elsewhere
    # The actual check requires context from parent scope
    # For now, use the namespace check
    try:
        namespaces = sym.get_namespaces()
        return len(namespaces) > 0
    except AttributeError:
        return False


def get_free_vars(scope: ScopeFact) -> tuple[str, ...]:
    """Get free variables (closure captures) for a scope."""
    return scope.free_vars


def get_cell_vars(scope: ScopeFact) -> tuple[str, ...]:
    """Get cell variables (captured by children) for a scope."""
    return scope.cell_vars


def is_closure(scope: ScopeFact) -> bool:
    """Check if scope is a closure (has free variables)."""
    return scope.has_free_vars
