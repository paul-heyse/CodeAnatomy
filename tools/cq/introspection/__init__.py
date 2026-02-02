"""Python introspection tools for cq analysis.

Provides symtable and bytecode analysis for deeper code understanding.
"""

from __future__ import annotations

from tools.cq.introspection.symtable_extract import (
    ScopeFact,
    ScopeGraph,
    ScopeType,
    SymbolFact,
    extract_scope_graph,
    get_cell_vars,
    get_free_vars,
    is_closure,
)

__all__ = [
    "ScopeFact",
    "ScopeGraph",
    "ScopeType",
    "SymbolFact",
    "extract_scope_graph",
    "get_cell_vars",
    "get_free_vars",
    "is_closure",
]
