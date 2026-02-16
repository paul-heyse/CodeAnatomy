"""Python introspection tools for cq analysis.

Provides symtable and bytecode analysis for deeper code understanding.
"""

from __future__ import annotations

from tools.cq.introspection.bytecode_index import (
    BytecodeIndex,
    InstructionFact,
    extract_instruction_facts,
    parse_exception_table,
)
from tools.cq.introspection.cfg_builder import build_cfg
from tools.cq.introspection.symtable_extract import (
    ScopeType,
    extract_scope_graph,
    get_cell_vars,
    get_free_vars,
    is_closure,
)

__all__ = [
    "BytecodeIndex",
    "InstructionFact",
    "ScopeType",
    "build_cfg",
    "extract_instruction_facts",
    "extract_scope_graph",
    "get_cell_vars",
    "get_free_vars",
    "is_closure",
    "parse_exception_table",
]
