from __future__ import annotations

from enum import Enum


class EntityKind(str, Enum):
    NODE = "node"
    EDGE = "edge"


class NodeKind(str, Enum):
    """
    Minimal, CPG-focused Python node kinds.

    Design principle:
      - node_id is globally stable across runs
      - spans (path,bstart,bend) exist for code-anchored nodes
      - “semantic payload” lives in cpg_props, not in cpg_nodes
    """
    PY_FILE = "PY_FILE"
    PY_MODULE = "PY_MODULE"                  # optional (from manifest/module_fqn)
    PY_SYMBOL = "PY_SYMBOL"                  # SCIP symbol string
    PY_QUALIFIED_NAME = "PY_QUALIFIED_NAME"  # QNP / dim_qualified_names
    PY_NAME_REF = "PY_NAME_REF"              # LibCST Name token ref
    PY_IMPORT_ALIAS = "PY_IMPORT_ALIAS"      # LibCST ImportAlias/ImportFrom alias
    PY_CALLSITE = "PY_CALLSITE"              # LibCST Call expression
    PY_DEF = "PY_DEF"                        # LibCST FunctionDef/ClassDef


class EdgeKind(str, Enum):
    """
    Edge kinds implementing the “final edge emission rules”.

    These are intentionally small and composable.
    """
    PY_DEFINES_SYMBOL = "PY_DEFINES_SYMBOL"
    PY_REFERENCES_SYMBOL = "PY_REFERENCES_SYMBOL"
    PY_READS_SYMBOL = "PY_READS_SYMBOL"
    PY_WRITES_SYMBOL = "PY_WRITES_SYMBOL"
    PY_IMPORTS_SYMBOL = "PY_IMPORTS_SYMBOL"

    # Call graph
    PY_CALLS_SYMBOL = "PY_CALLS_SYMBOL"                      # preferred
    PY_CALLS_QUALIFIED_NAME = "PY_CALLS_QUALIFIED_NAME"      # fallback (ambiguity-preserving)


# SCIP symbol_roles bitmask constants (as used by scip-python)
SCIP_ROLE_DEFINITION = 1
SCIP_ROLE_IMPORT = 2
SCIP_ROLE_WRITE = 4
SCIP_ROLE_READ = 8
