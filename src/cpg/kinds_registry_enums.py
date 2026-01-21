"""Enum and constant definitions for the Ultimate CPG registry."""

from __future__ import annotations

from enum import StrEnum


class SourceKind(StrEnum):
    """Sources that can emit nodes or edges."""

    LIBCST = "libcst"
    PY_AST = "py_ast"
    SYMTABLE = "symtable"
    DIS = "dis"
    SCIP = "scip"
    TREESITTER = "treesitter"
    INSPECT = "inspect"
    DERIVED = "derived"
    PIPELINE = "pipeline"  # build steps that aggregate/merge/normalize


class EntityKind(StrEnum):
    """Define entity kinds for properties tables."""

    NODE = "node"
    EDGE = "edge"


SCIP_ROLE_DEFINITION = 1
SCIP_ROLE_IMPORT = 2
SCIP_ROLE_WRITE = 4
SCIP_ROLE_READ = 8
SCIP_ROLE_GENERATED = 16
SCIP_ROLE_TEST = 32
SCIP_ROLE_FORWARD_DEFINITION = 64


class NodeKind(StrEnum):
    """Ultimate Python CPG node kinds."""

    # -------------------------
    # Repository / filesystem
    # -------------------------
    PY_REPO = "PY_REPO"
    PY_FILE = "PY_FILE"
    PY_MODULE = "PY_MODULE"
    PY_PACKAGE = "PY_PACKAGE"

    # -------------------------
    # Concrete syntax (LibCST)
    # -------------------------
    CST_NODE = "CST_NODE"
    CST_DEF = "CST_DEF"
    CST_CLASS_DEF = "CST_CLASS_DEF"
    CST_FUNCTION_DEF = "CST_FUNCTION_DEF"
    CST_LAMBDA = "CST_LAMBDA"
    CST_PARAM = "CST_PARAM"
    CST_DECORATOR = "CST_DECORATOR"
    CST_IMPORT = "CST_IMPORT"
    CST_IMPORT_FROM = "CST_IMPORT_FROM"
    CST_IMPORT_ALIAS = "CST_IMPORT_ALIAS"
    CST_REF = "CST_REF"
    CST_ATTRIBUTE_ACCESS = "CST_ATTRIBUTE_ACCESS"
    CST_SUBSCRIPT = "CST_SUBSCRIPT"
    CST_CALLSITE = "CST_CALLSITE"
    CST_ARG = "CST_ARG"
    CST_LITERAL = "CST_LITERAL"
    CST_DOCSTRING = "CST_DOCSTRING"
    CST_COMMENT = "CST_COMMENT"

    # --------------------------------
    # Abstract syntax (python `ast`)
    # --------------------------------
    AST_NODE = "AST_NODE"
    AST_DEF = "AST_DEF"
    AST_CALL = "AST_CALL"
    AST_NAME = "AST_NAME"
    AST_ATTRIBUTE = "AST_ATTRIBUTE"

    # -------------------------
    # Tree-sitter diagnostics
    # -------------------------
    TS_NODE = "TS_NODE"
    TS_ERROR = "TS_ERROR"
    TS_MISSING = "TS_MISSING"

    # -------------------------
    # Symtable scopes/bindings
    # -------------------------
    SYM_SCOPE = "SYM_SCOPE"
    SYM_SYMBOL = "SYM_SYMBOL"
    PY_SCOPE = "PY_SCOPE"  # normalized scope (symtable + libcst)
    PY_BINDING = "PY_BINDING"  # normalized binding slot (scope,name)
    PY_DEF_SITE = "PY_DEF_SITE"  # anchored definition site (name span)
    PY_USE_SITE = "PY_USE_SITE"  # anchored use site (name span)

    # -------------------------
    # SCIP global symbols
    # -------------------------
    SCIP_INDEX = "SCIP_INDEX"
    SCIP_DOCUMENT = "SCIP_DOCUMENT"
    SCIP_SYMBOL = "SCIP_SYMBOL"
    SCIP_OCCURRENCE = "SCIP_OCCURRENCE"
    SCIP_DIAGNOSTIC = "SCIP_DIAGNOSTIC"

    # -------------------------
    # Qualified-name candidates
    # -------------------------
    PY_QUALIFIED_NAME = "PY_QUALIFIED_NAME"
    PY_MODULE_FQN = "PY_MODULE_FQN"

    # -------------------------
    # Bytecode / control-flow / dataflow
    # -------------------------
    BC_CODE_UNIT = "BC_CODE_UNIT"
    BC_INSTR = "BC_INSTR"
    CFG_BLOCK = "CFG_BLOCK"
    CFG_EXIT = "CFG_EXIT"
    DF_DEF = "DF_DEF"
    DF_USE = "DF_USE"

    TYPE_EXPR = "TYPE_EXPR"
    TYPE = "TYPE"
    TYPE_PARAM = "TYPE_PARAM"
    TYPE_ALIAS = "TYPE_ALIAS"

    # -------------------------
    # Runtime overlay (inspect)
    # -------------------------
    RT_OBJECT = "RT_OBJECT"
    RT_SIGNATURE = "RT_SIGNATURE"
    RT_SIGNATURE_PARAM = "RT_SIGNATURE_PARAM"
    RT_MEMBER = "RT_MEMBER"

    # -------------------------
    # Diagnostics / pipeline
    # -------------------------
    DIAG = "DIAG"


class EdgeKind(StrEnum):
    """Ultimate Python CPG edge kinds."""

    # -------------------------
    # Containment / structure
    # -------------------------
    REPO_CONTAINS = "REPO_CONTAINS"  # repo -> file
    FILE_DECLARES_MODULE = "FILE_DECLARES_MODULE"  # file -> module
    FILE_CONTAINS = "FILE_CONTAINS"  # file -> anchored node

    CST_PARENT_OF = "CST_PARENT_OF"
    AST_PARENT_OF = "AST_PARENT_OF"
    TS_PARENT_OF = "TS_PARENT_OF"

    # -------------------------
    # Names / scopes / bindings
    # -------------------------
    SCOPE_PARENT = "SCOPE_PARENT"  # scope -> parent scope
    SCOPE_BINDS = "SCOPE_BINDS"  # scope -> binding
    BINDING_RESOLVES_TO = "BINDING_RESOLVES_TO"  # binding -> binding (closure/global/nonlocal)

    DEF_SITE_OF = "DEF_SITE_OF"  # def_site -> binding
    USE_SITE_OF = "USE_SITE_OF"  # use_site -> binding

    # -------------------------
    # Qualified name candidates
    # -------------------------
    NAME_REF_CANDIDATE_QNAME = "NAME_REF_CANDIDATE_QNAME"
    CALLSITE_CANDIDATE_QNAME = "CALLSITE_CANDIDATE_QNAME"
    ATTR_CANDIDATE_QNAME = "ATTR_CANDIDATE_QNAME"

    # -------------------------
    # SCIP-backed semantic edges
    # -------------------------
    PY_DEFINES_SYMBOL = "PY_DEFINES_SYMBOL"
    PY_REFERENCES_SYMBOL = "PY_REFERENCES_SYMBOL"
    PY_IMPORTS_SYMBOL = "PY_IMPORTS_SYMBOL"
    PY_READS_SYMBOL = "PY_READS_SYMBOL"
    PY_WRITES_SYMBOL = "PY_WRITES_SYMBOL"

    # -------------------------
    # SCIP symbol relationships
    # -------------------------
    SCIP_SYMBOL_REFERENCE = "SCIP_SYMBOL_REFERENCE"
    SCIP_SYMBOL_IMPLEMENTATION = "SCIP_SYMBOL_IMPLEMENTATION"
    SCIP_SYMBOL_TYPE_DEFINITION = "SCIP_SYMBOL_TYPE_DEFINITION"
    SCIP_SYMBOL_DEFINITION = "SCIP_SYMBOL_DEFINITION"

    # -------------------------
    # Calls (prefer SCIP, fallback to QNAME)
    # -------------------------
    PY_CALLS_SYMBOL = "PY_CALLS_SYMBOL"
    PY_CALLS_QNAME = "PY_CALLS_QNAME"

    # -------------------------
    # Types
    # -------------------------
    HAS_ANNOTATION = "HAS_ANNOTATION"
    INFERRED_TYPE = "INFERRED_TYPE"
    TYPE_PARAM_OF = "TYPE_PARAM_OF"

    # -------------------------
    # Bytecode anchors + flow
    # -------------------------
    BYTECODE_ANCHOR = "BYTECODE_ANCHOR"  # instr -> source anchor

    CFG_NEXT = "CFG_NEXT"
    CFG_JUMP = "CFG_JUMP"
    CFG_BRANCH_TRUE = "CFG_BRANCH_TRUE"
    CFG_BRANCH_FALSE = "CFG_BRANCH_FALSE"
    CFG_BRANCH = "CFG_BRANCH"
    CFG_EXC = "CFG_EXC"

    STEP_DEF = "STEP_DEF"  # df_def/instr -> binding
    STEP_USE = "STEP_USE"  # df_use/instr -> binding
    REACHES = "REACHES"  # df_def -> df_use

    # -------------------------
    # Runtime overlay
    # -------------------------
    RT_HAS_SIGNATURE = "RT_HAS_SIGNATURE"
    RT_HAS_PARAM = "RT_HAS_PARAM"
    RT_HAS_MEMBER = "RT_HAS_MEMBER"
    RT_WRAPS = "RT_WRAPS"

    # -------------------------
    # Diagnostics
    # -------------------------
    HAS_DIAGNOSTIC = "HAS_DIAGNOSTIC"


__all__ = [
    "SCIP_ROLE_DEFINITION",
    "SCIP_ROLE_FORWARD_DEFINITION",
    "SCIP_ROLE_GENERATED",
    "SCIP_ROLE_IMPORT",
    "SCIP_ROLE_READ",
    "SCIP_ROLE_TEST",
    "SCIP_ROLE_WRITE",
    "EdgeKind",
    "EntityKind",
    "NodeKind",
    "SourceKind",
]
