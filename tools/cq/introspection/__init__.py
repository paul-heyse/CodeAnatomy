"""Python introspection tools for cq analysis.

Provides symtable and bytecode analysis for deeper code understanding.
"""

from __future__ import annotations

from tools.cq.introspection.bytecode_index import (
    BytecodeIndex,
    ExceptionEntry,
    InstructionFact,
    extract_instruction_facts,
    parse_exception_table,
)
from tools.cq.introspection.cfg_builder import (
    CFG,
    BasicBlock,
    CFGEdge,
    build_cfg,
)
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
    # Bytecode
    "BytecodeIndex",
    "ExceptionEntry",
    "InstructionFact",
    "extract_instruction_facts",
    "parse_exception_table",
    # CFG
    "BasicBlock",
    "CFG",
    "CFGEdge",
    "build_cfg",
    # Symtable
    "ScopeFact",
    "ScopeGraph",
    "ScopeType",
    "SymbolFact",
    "extract_scope_graph",
    "get_cell_vars",
    "get_free_vars",
    "is_closure",
]
