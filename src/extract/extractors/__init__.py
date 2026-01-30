"""Evidence layer implementations (extractors).

This subpackage contains the main extraction modules that produce
evidence tables from Python source code:
- cst_extract.py: LibCST extraction
- ast_extract.py: Python AST extraction
- bytecode_extract.py: Bytecode/disassembly extraction
- symtable_extract.py: Symbol table extraction
- imports_extract.py: Python imports extraction
- external_scope.py: External interface extraction
- tree_sitter/: Tree-sitter subsystem
- scip/: SCIP subsystem
"""

from __future__ import annotations

__all__: list[str] = []
