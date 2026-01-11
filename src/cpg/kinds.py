"""CPG node and edge kind enumerations."""

from __future__ import annotations

from enum import StrEnum


class EntityKind(StrEnum):
    """Define entity kinds for properties tables."""

    NODE = "node"
    EDGE = "edge"


class NodeKind(StrEnum):
    """Define minimal Python node kinds for the CPG."""

    PY_FILE = "PY_FILE"
    PY_MODULE = "PY_MODULE"
    PY_SYMBOL = "PY_SYMBOL"
    PY_QUALIFIED_NAME = "PY_QUALIFIED_NAME"
    PY_NAME_REF = "PY_NAME_REF"
    PY_IMPORT_ALIAS = "PY_IMPORT_ALIAS"
    PY_CALLSITE = "PY_CALLSITE"
    PY_DEF = "PY_DEF"


class EdgeKind(StrEnum):
    """Define edge kinds for CPG relationships."""

    PY_DEFINES_SYMBOL = "PY_DEFINES_SYMBOL"
    PY_REFERENCES_SYMBOL = "PY_REFERENCES_SYMBOL"
    PY_READS_SYMBOL = "PY_READS_SYMBOL"
    PY_WRITES_SYMBOL = "PY_WRITES_SYMBOL"
    PY_IMPORTS_SYMBOL = "PY_IMPORTS_SYMBOL"

    PY_CALLS_SYMBOL = "PY_CALLS_SYMBOL"
    PY_CALLS_QUALIFIED_NAME = "PY_CALLS_QUALIFIED_NAME"


SCIP_ROLE_DEFINITION = 1
SCIP_ROLE_IMPORT = 2
SCIP_ROLE_WRITE = 4
SCIP_ROLE_READ = 8
