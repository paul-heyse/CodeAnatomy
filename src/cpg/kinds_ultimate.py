"""Ultimate Python CPG kind registry.

This module is intended to be the *single source of truth* for:
  - node_kind / edge_kind enumerations (Ultimate Python CPG)
  - required properties & their types per kind (contracts)
  - derivation manifest per kind (extractor/provider/join/id/confidence/ambiguity)

Design goals:
  - Machine consumable: structured dataclasses + constant dict registries
  - Strict: every enum value must have a contract and >=1 derivation entry
  - Practical: supports "implemented" vs "planned" derivations
  - Portable: stdlib-only (no imports from other project modules)

Your Hamilton integration and relationship compiler can validate against this at build time:
  - validate_registry_completeness()
  - validate_derivations_implemented_only(allowed_sources=..., allow_planned=False)

Note:
  - "props" here refers to logical properties. You can store these either:
      (a) as columns in cpg_nodes/cpg_edges, OR
      (b) as rows in cpg_props.
    Validation helpers can be wired accordingly.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal

# ----------------------------
# Enums
# ----------------------------


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
    CST_NAME_REF = "CST_NAME_REF"
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


# ----------------------------
# Property specs & contracts
# ----------------------------

PropPrimitive = Literal["string", "int", "float", "bool", "json"]


@dataclass(frozen=True)
class PropSpec:
    """
    A strict property specification.

    type:
      One of: string | int | float | bool | json

    enum_values:
      If provided, property is string with constrained values.
    """

    type: PropPrimitive
    description: str = ""
    enum_values: tuple[str, ...] | None = None

    def to_dict(self) -> dict[str, object]:
        """Serialize the property spec to a dictionary.

        Returns
        -------
        dict[str, object]
            JSON-serializable property spec.
        """
        return {
            "type": self.type,
            "description": self.description,
            "enum_values": list(self.enum_values) if self.enum_values else None,
        }


def p_str(desc: str = "", enum: Sequence[str] | None = None) -> PropSpec:
    """Create a string property spec.

    Returns
    -------
    PropSpec
        String property specification.
    """
    return PropSpec("string", desc, tuple(enum) if enum else None)


def p_int(desc: str = "") -> PropSpec:
    """Create an integer property spec.

    Returns
    -------
    PropSpec
        Integer property specification.
    """
    return PropSpec("int", desc)


def p_float(desc: str = "") -> PropSpec:
    """Create a float property spec.

    Returns
    -------
    PropSpec
        Float property specification.
    """
    return PropSpec("float", desc)


def p_bool(desc: str = "") -> PropSpec:
    """Create a boolean property spec.

    Returns
    -------
    PropSpec
        Boolean property specification.
    """
    return PropSpec("bool", desc)


def p_json(desc: str = "") -> PropSpec:
    """Create a JSON property spec.

    Returns
    -------
    PropSpec
        JSON property specification.
    """
    return PropSpec("json", desc)


@dataclass(frozen=True)
class NodeKindContract:
    """
    Contract for a node kind.

    requires_anchor:
      If True, node must have path + byte-span (bstart,bend).

    required_props:
      Properties that MUST exist (either as columns or in cpg_props).

    optional_props:
      Recommended properties for best-in-class completeness.

    allowed_sources:
      Which sources are allowed to emit this kind (useful for validation).
    """

    requires_anchor: bool
    required_props: Mapping[str, PropSpec] = field(default_factory=dict)
    optional_props: Mapping[str, PropSpec] = field(default_factory=dict)
    allowed_sources: tuple[SourceKind, ...] = ()
    description: str = ""

    def to_dict(self) -> dict[str, object]:
        """Serialize the node kind contract to a dictionary.

        Returns
        -------
        dict[str, object]
            JSON-serializable node contract.
        """
        return {
            "requires_anchor": self.requires_anchor,
            "required_props": {k: v.to_dict() for k, v in self.required_props.items()},
            "optional_props": {k: v.to_dict() for k, v in self.optional_props.items()},
            "allowed_sources": [s.value for s in self.allowed_sources],
            "description": self.description,
        }


@dataclass(frozen=True)
class EdgeKindContract:
    """
    Contract for an edge kind.

    requires_evidence_anchor:
      If True, edge should carry evidence anchor span (path,bstart,bend).

    required_props:
      Edge metadata that MUST exist (either as columns or in cpg_props).

    optional_props:
      Additional recommended metadata.
    """

    requires_evidence_anchor: bool
    required_props: Mapping[str, PropSpec] = field(default_factory=dict)
    optional_props: Mapping[str, PropSpec] = field(default_factory=dict)
    allowed_sources: tuple[SourceKind, ...] = ()
    description: str = ""

    def to_dict(self) -> dict[str, object]:
        """Serialize the edge kind contract to a dictionary.

        Returns
        -------
        dict[str, object]
            JSON-serializable edge contract.
        """
        return {
            "requires_evidence_anchor": self.requires_evidence_anchor,
            "required_props": {k: v.to_dict() for k, v in self.required_props.items()},
            "optional_props": {k: v.to_dict() for k, v in self.optional_props.items()},
            "allowed_sources": [s.value for s in self.allowed_sources],
            "description": self.description,
        }


# ----------------------------
# Derivation manifest
# ----------------------------

DerivationStatus = Literal["implemented", "planned"]


@dataclass(frozen=True)
class DerivationSpec:
    """
    Derivation spec for a node_kind or edge_kind.

    extractor:
      A python import path string: "pkg.module:function" (or callable attribute).

    provider_or_field:
      For LibCST: provider names used
      For SCIP: fields (occurrence.symbol_roles, symbol_information.kind, etc.)
      For symtable: flags/methods
      For dis: get_instructions + exception table logic
      For tree-sitter: node flags (is_error, is_missing, etc.)

    join_keys:
      For join-derived edges, list the join keys/conditions.

    id_recipe:
      A human-readable, deterministic ID recipe.

    confidence_policy:
      How to compute confidence (and optional score).

    ambiguity_policy:
      How to represent multi-candidate results & winner selection.

    status:
      "implemented" or "planned" so your pipeline can enforce availability.
    """

    extractor: str
    provider_or_field: str
    join_keys: tuple[str, ...]
    id_recipe: str
    confidence_policy: str
    ambiguity_policy: str
    status: DerivationStatus = "implemented"
    notes: str = ""

    def to_dict(self) -> dict[str, object]:
        """Serialize the derivation spec to a dictionary.

        Returns
        -------
        dict[str, object]
            JSON-serializable derivation spec.
        """
        return {
            "extractor": self.extractor,
            "provider_or_field": self.provider_or_field,
            "join_keys": list(self.join_keys),
            "id_recipe": self.id_recipe,
            "confidence_policy": self.confidence_policy,
            "ambiguity_policy": self.ambiguity_policy,
            "status": self.status,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class CallableRef:
    """Reference a callable attribute via a module path."""

    module: str
    attr: str

    def resolve(self) -> object:
        """Import and resolve the referenced attribute.

        Returns
        -------
        object
            Resolved attribute.
        """
        mod = importlib.import_module(self.module)
        return getattr(mod, self.attr)


def parse_extractor(value: str) -> CallableRef:
    """Parse an extractor string into a callable reference.

    Parameters
    ----------
    value:
        Extractor string in the form "module:attribute".

    Returns
    -------
    CallableRef
        Parsed callable reference.

    Raises
    ------
    ValueError
        Raised when the extractor string is invalid.
    """
    raw = value.strip()
    if ":" not in raw:
        msg = f"Extractor must be in module:attribute form, got {value!r}."
        raise ValueError(msg)
    module, attr = raw.split(":", 1)
    if not module or not attr:
        msg = f"Extractor must include module and attribute, got {value!r}."
        raise ValueError(msg)
    return CallableRef(module=module, attr=attr)


# ----------------------------
# Contracts: node and edge kind contracts
# ----------------------------

NODE_KIND_CONTRACTS: dict[NodeKind, NodeKindContract] = {
    # ---- repo/files ----
    NodeKind.PY_REPO: NodeKindContract(
        requires_anchor=False,
        required_props={
            "repo_root": p_str("Repository root path"),
            "repo_id": p_str("Stable repo identifier (hash of repo_root + git head)"),
        },
        optional_props={
            "git_commit": p_str("Best-effort git commit"),
        },
        allowed_sources=(SourceKind.PIPELINE,),
        description="Repository root entity.",
    ),
    NodeKind.PY_FILE: NodeKindContract(
        requires_anchor=False,
        required_props={"path": p_str("Repo-relative file path")},
        optional_props={
            "size_bytes": p_int("File size in bytes"),
            "file_sha256": p_str("SHA-256 hash (hex)"),
            "mtime_ns": p_int("Modification time (ns)"),
            "encoding": p_str("Encoding guess / decoding policy"),
        },
        allowed_sources=(SourceKind.PIPELINE,),
        description="File entity for a repo-tracked Python file.",
    ),
    NodeKind.PY_MODULE: NodeKindContract(
        requires_anchor=False,
        required_props={
            "path": p_str("Repo-relative file path"),
            "module_fqn": p_str("Fully qualified module name"),
        },
        optional_props={"package_root": p_str("Detected package root")},
        allowed_sources=(SourceKind.LIBCST, SourceKind.DERIVED),
        description="Module identity for a file (module_fqn).",
    ),
    NodeKind.PY_PACKAGE: NodeKindContract(
        requires_anchor=False,
        required_props={"package_fqn": p_str("Package fully-qualified name")},
        optional_props={"path": p_str("Directory path")},
        allowed_sources=(SourceKind.PIPELINE, SourceKind.DERIVED),
        description="Package identity node (optional).",
    ),
    # ---- LibCST ----
    NodeKind.CST_NODE: NodeKindContract(
        requires_anchor=True,
        required_props={"cst_type": p_str("LibCST node type name (e.g., FunctionDef, Name, Call)")},
        optional_props={
            "line_start": p_int(),
            "col_start": p_int(),
            "line_end": p_int(),
            "col_end": p_int(),
            "module_fqn": p_str(),
        },
        allowed_sources=(SourceKind.LIBCST,),
        description="Generic LibCST node (lossless concrete syntax lens).",
    ),
    NodeKind.CST_DEF: NodeKindContract(
        requires_anchor=True,
        required_props={
            "name": p_str("Definition name"),
            "def_kind": p_str(enum=["function", "class", "lambda"]),
        },
        optional_props={
            "is_async": p_bool(),
            "decorator_count": p_int(),
            "docstring": p_str(),
        },
        allowed_sources=(SourceKind.LIBCST,),
        description="High-signal definition node (union of class/function/lambda).",
    ),
    NodeKind.CST_CLASS_DEF: NodeKindContract(
        requires_anchor=True,
        required_props={"name": p_str("Class name")},
        optional_props={"base_exprs_json": p_json("List of base class expr renderings")},
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST ClassDef node.",
    ),
    NodeKind.CST_FUNCTION_DEF: NodeKindContract(
        requires_anchor=True,
        required_props={"name": p_str("Function name")},
        optional_props={"is_async": p_bool(), "returns_annotation": p_str()},
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST FunctionDef node.",
    ),
    NodeKind.CST_LAMBDA: NodeKindContract(
        requires_anchor=True,
        required_props={},
        optional_props={"param_count": p_int()},
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST Lambda node.",
    ),
    NodeKind.CST_PARAM: NodeKindContract(
        requires_anchor=True,
        required_props={"name": p_str("Parameter name")},
        optional_props={
            "kind": p_str(enum=["positional", "vararg", "kwonly", "varkw"]),
            "has_default": p_bool(),
        },
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST parameter node.",
    ),
    NodeKind.CST_DECORATOR: NodeKindContract(
        requires_anchor=True,
        required_props={},
        optional_props={"decorator_text": p_str()},
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST decorator node.",
    ),
    NodeKind.CST_IMPORT: NodeKindContract(
        requires_anchor=True,
        required_props={},
        optional_props={"import_text": p_str()},
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST Import node.",
    ),
    NodeKind.CST_IMPORT_FROM: NodeKindContract(
        requires_anchor=True,
        required_props={},
        optional_props={"module": p_str(), "relative_level": p_int()},
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST ImportFrom node.",
    ),
    NodeKind.CST_IMPORT_ALIAS: NodeKindContract(
        requires_anchor=True,
        required_props={"imported_name": p_str("Imported name (module or member)")},
        optional_props={"asname": p_str("Alias name"), "is_star": p_bool()},
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST ImportAlias node.",
    ),
    NodeKind.CST_NAME_REF: NodeKindContract(
        requires_anchor=True,
        required_props={
            "name": p_str("Referenced identifier"),
            "expr_context": p_str(enum=["LOAD", "STORE", "DEL", "UNKNOWN"]),
        },
        optional_props={"scope_id": p_str("ScopeProvider scope id (if captured)")},
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST Name reference occurrence.",
    ),
    NodeKind.CST_ATTRIBUTE_ACCESS: NodeKindContract(
        requires_anchor=True,
        required_props={"attr": p_str("Attribute name")},
        optional_props={
            "base_shape": p_str(enum=["NAME", "ATTRIBUTE", "CALL", "SUBSCRIPT", "OTHER"])
        },
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST Attribute access occurrence.",
    ),
    NodeKind.CST_SUBSCRIPT: NodeKindContract(
        requires_anchor=True,
        required_props={},
        optional_props={"subscript_text": p_str()},
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST Subscript occurrence.",
    ),
    NodeKind.CST_CALLSITE: NodeKindContract(
        requires_anchor=True,
        required_props={
            "callee_shape": p_str(enum=["NAME", "ATTRIBUTE", "SUBSCRIPT", "OTHER"]),
            "arg_count": p_int("Number of args (positional+keyword)"),
        },
        optional_props={"callee_dotted": p_str("Best-effort dotted callee path")},
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST Call expression occurrence.",
    ),
    NodeKind.CST_ARG: NodeKindContract(
        requires_anchor=True,
        required_props={},
        optional_props={"is_star": p_bool(), "is_kw_star": p_bool()},
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST call argument node.",
    ),
    NodeKind.CST_LITERAL: NodeKindContract(
        requires_anchor=True,
        required_props={
            "literal_kind": p_str(
                enum=[
                    "string",
                    "bytes",
                    "int",
                    "float",
                    "complex",
                    "bool",
                    "none",
                    "ellipsis",
                    "fstring",
                    "other",
                ]
            )
        },
        optional_props={"literal_text": p_str()},
        allowed_sources=(SourceKind.LIBCST,),
        description="Literal node (CST-level).",
    ),
    NodeKind.CST_DOCSTRING: NodeKindContract(
        requires_anchor=True,
        required_props={"docstring": p_str("Docstring text")},
        optional_props={"is_module_docstring": p_bool()},
        allowed_sources=(SourceKind.LIBCST,),
        description="Docstring node (module/class/function).",
    ),
    NodeKind.CST_COMMENT: NodeKindContract(
        requires_anchor=True,
        required_props={"text": p_str("Comment text")},
        optional_props={},
        allowed_sources=(SourceKind.LIBCST,),
        description="Comment node (trivia).",
    ),
    # ---- Python AST ----
    NodeKind.AST_NODE: NodeKindContract(
        requires_anchor=False,  # line/col may be missing for some nodes; normalization can add bytes later
        required_props={"ast_type": p_str("ast.AST type name (e.g., Call, Name, FunctionDef)")},
        optional_props={
            "lineno": p_int(),
            "col_offset": p_int(),
            "end_lineno": p_int(),
            "end_col_offset": p_int(),
        },
        allowed_sources=(SourceKind.PY_AST,),
        description="Generic python ast node (abstract syntax lens).",
    ),
    NodeKind.AST_DEF: NodeKindContract(
        requires_anchor=False,
        required_props={
            "ast_type": p_str(enum=["FunctionDef", "AsyncFunctionDef", "ClassDef", "Lambda"])
        },
        optional_props={"name": p_str(), "lineno": p_int(), "end_lineno": p_int()},
        allowed_sources=(SourceKind.PY_AST,),
        description="AST definition node (def/class/lambda).",
    ),
    NodeKind.AST_CALL: NodeKindContract(
        requires_anchor=False,
        required_props={"ast_type": p_str(enum=["Call"])},
        optional_props={"lineno": p_int(), "end_lineno": p_int()},
        allowed_sources=(SourceKind.PY_AST,),
        description="AST Call node.",
    ),
    NodeKind.AST_NAME: NodeKindContract(
        requires_anchor=False,
        required_props={"ast_type": p_str(enum=["Name"]), "id": p_str("Identifier string")},
        optional_props={"ctx": p_str(enum=["Load", "Store", "Del", "Param"])},
        allowed_sources=(SourceKind.PY_AST,),
        description="AST Name node.",
    ),
    NodeKind.AST_ATTRIBUTE: NodeKindContract(
        requires_anchor=False,
        required_props={"ast_type": p_str(enum=["Attribute"]), "attr": p_str("Attribute name")},
        optional_props={"ctx": p_str(enum=["Load", "Store", "Del"])},
        allowed_sources=(SourceKind.PY_AST,),
        description="AST Attribute node.",
    ),
    # ---- tree-sitter ----
    NodeKind.TS_NODE: NodeKindContract(
        requires_anchor=True,
        required_props={"ts_type": p_str("tree-sitter node type")},
        optional_props={"is_named": p_bool(), "has_error": p_bool()},
        allowed_sources=(SourceKind.TREESITTER,),
        description="Generic tree-sitter node.",
    ),
    NodeKind.TS_ERROR: NodeKindContract(
        requires_anchor=True,
        required_props={"ts_type": p_str(), "is_error": p_bool()},
        optional_props={"has_error": p_bool()},
        allowed_sources=(SourceKind.TREESITTER,),
        description="tree-sitter error node.",
    ),
    NodeKind.TS_MISSING: NodeKindContract(
        requires_anchor=True,
        required_props={"ts_type": p_str(), "is_missing": p_bool()},
        optional_props={},
        allowed_sources=(SourceKind.TREESITTER,),
        description="tree-sitter missing-token node (recovery inserted).",
    ),
    # ---- symtable / bindings ----
    NodeKind.SYM_SCOPE: NodeKindContract(
        requires_anchor=False,
        required_props={
            "scope_id": p_str("Stable constructed scope id"),
            "scope_type": p_str("symtable table.get_type().name"),
            "scope_name": p_str("symtable table.get_name()"),
        },
        optional_props={
            "lineno": p_int(),
            "is_meta_scope": p_bool("True for ANNOTATION/TYPE_* scopes"),
            "path": p_str("File path that owns this scope"),
        },
        allowed_sources=(SourceKind.SYMTABLE,),
        description="Symtable scope node (compiler truth for lexical scopes).",
    ),
    NodeKind.SYM_SYMBOL: NodeKindContract(
        requires_anchor=False,
        required_props={
            "scope_id": p_str(),
            "name": p_str("Symbol name"),
        },
        optional_props={
            "is_local": p_bool(),
            "is_global": p_bool(),
            "is_nonlocal": p_bool(),
            "is_free": p_bool(),
            "is_parameter": p_bool(),
            "is_imported": p_bool(),
            "is_assigned": p_bool(),
            "is_referenced": p_bool(),
            "is_annotated": p_bool(),
            "is_namespace": p_bool(),
        },
        allowed_sources=(SourceKind.SYMTABLE,),
        description="Symtable symbol record within a scope.",
    ),
    NodeKind.PY_SCOPE: NodeKindContract(
        requires_anchor=False,
        required_props={"scope_id": p_str(), "scope_type": p_str()},
        optional_props={"path": p_str(), "module_fqn": p_str()},
        allowed_sources=(SourceKind.DERIVED, SourceKind.SYMTABLE, SourceKind.LIBCST),
        description="Normalized scope node (unifies symtable + libcst scope concepts).",
    ),
    NodeKind.PY_BINDING: NodeKindContract(
        requires_anchor=False,
        required_props={
            "binding_id": p_str("binding_id = f'{scope_id}:BIND:{name}'"),
            "scope_id": p_str(),
            "name": p_str(),
            "binding_kind": p_str(
                enum=[
                    "local",
                    "param",
                    "import",
                    "global_ref",
                    "nonlocal_ref",
                    "free_ref",
                    "namespace",
                    "annot_only",
                    "unknown",
                ]
            ),
        },
        optional_props={
            "declared_here": p_bool(),
            "referenced_here": p_bool(),
            "assigned_here": p_bool(),
            "annotated_here": p_bool(),
        },
        allowed_sources=(SourceKind.DERIVED, SourceKind.SYMTABLE),
        description="Normalized binding slot (scope,name) with classification flags.",
    ),
    NodeKind.PY_DEF_SITE: NodeKindContract(
        requires_anchor=True,
        required_props={"binding_id": p_str("Binding this site defines"), "name": p_str()},
        optional_props={
            "def_site_kind": p_str(enum=["function", "class", "import", "assign", "param", "other"])
        },
        allowed_sources=(SourceKind.DERIVED,),
        description="Anchored definition site (name span) mapped to a binding.",
    ),
    NodeKind.PY_USE_SITE: NodeKindContract(
        requires_anchor=True,
        required_props={"binding_id": p_str("Binding this site uses"), "name": p_str()},
        optional_props={
            "use_kind": p_str(enum=["read", "write", "del", "call", "import", "other"])
        },
        allowed_sources=(SourceKind.DERIVED,),
        description="Anchored use site (name span) mapped to a binding.",
    ),
    # ---- SCIP ----
    NodeKind.SCIP_INDEX: NodeKindContract(
        requires_anchor=False,
        required_props={"index_format": p_str(enum=["scip"]), "tool": p_str()},
        optional_props={"version": p_str(), "project_root": p_str()},
        allowed_sources=(SourceKind.SCIP,),
        description="SCIP index root node (metadata).",
    ),
    NodeKind.SCIP_DOCUMENT: NodeKindContract(
        requires_anchor=False,
        required_props={"path": p_str("Document path relative to repo/project root")},
        optional_props={"language": p_str(), "diagnostic_count": p_int()},
        allowed_sources=(SourceKind.SCIP,),
        description="SCIP document node (per file).",
    ),
    NodeKind.SCIP_SYMBOL: NodeKindContract(
        requires_anchor=False,
        required_props={"symbol": p_str("SCIP symbol string (global identity)")},
        optional_props={
            "display_name": p_str(),
            "symbol_kind": p_str(),
            "enclosing_symbol": p_str(),
        },
        allowed_sources=(SourceKind.SCIP,),
        description="SCIP symbol node.",
    ),
    NodeKind.SCIP_OCCURRENCE: NodeKindContract(
        requires_anchor=True,  # once normalized, occurrences become anchored
        required_props={
            "symbol": p_str(),
            "symbol_roles": p_int("SCIP symbol_roles bitmask"),
        },
        optional_props={"occurrence_role_names_json": p_json("Human-readable role names")},
        allowed_sources=(SourceKind.SCIP, SourceKind.DERIVED),
        description="SCIP occurrence node (symbol + roles + range), normalized to byte span.",
    ),
    NodeKind.SCIP_DIAGNOSTIC: NodeKindContract(
        requires_anchor=True,
        required_props={"severity": p_str(enum=["INFO", "WARNING", "ERROR"]), "message": p_str()},
        optional_props={"code": p_str(), "source": p_str()},
        allowed_sources=(SourceKind.SCIP,),
        description="SCIP diagnostic node (if present).",
    ),
    # ---- qualified names ----
    NodeKind.PY_QUALIFIED_NAME: NodeKindContract(
        requires_anchor=False,
        required_props={"qname": p_str("Qualified name string")},
        optional_props={
            "qname_source": p_str(
                enum=["QualifiedNameProvider", "FullyQualifiedNameProvider", "syntactic", "other"]
            )
        },
        allowed_sources=(SourceKind.LIBCST, SourceKind.DERIVED),
        description="Qualified name candidate/canonical node.",
    ),
    NodeKind.PY_MODULE_FQN: NodeKindContract(
        requires_anchor=False,
        required_props={"module_fqn": p_str()},
        optional_props={},
        allowed_sources=(SourceKind.LIBCST, SourceKind.DERIVED),
        description="Module fully qualified name node (optional).",
    ),
    # ---- bytecode / flow ----
    NodeKind.BC_CODE_UNIT: NodeKindContract(
        requires_anchor=False,
        required_props={
            "code_unit_id": p_str(),
            "path": p_str("File path for code object"),
            "qualname": p_str("co_qualname / derived qualname"),
        },
        optional_props={"co_firstlineno": p_int(), "flags_json": p_json()},
        allowed_sources=(SourceKind.DIS,),
        description="Code object / code unit (function/lambda/comprehension) node.",
    ),
    NodeKind.BC_INSTR: NodeKindContract(
        requires_anchor=False,
        required_props={
            "instr_id": p_str("Stable instruction id"),
            "code_unit_id": p_str(),
            "offset": p_int("Bytecode offset"),
            "baseopname": p_str("Semantic opcode name (base op)"),
        },
        optional_props={"opname": p_str("Raw opname"), "argrepr": p_str(), "starts_line": p_int()},
        allowed_sources=(SourceKind.DIS,),
        description="Bytecode instruction node.",
    ),
    NodeKind.CFG_BLOCK: NodeKindContract(
        requires_anchor=False,
        required_props={
            "block_id": p_str(),
            "code_unit_id": p_str(),
            "start_off": p_int(),
            "end_off": p_int(),
        },
        optional_props={"is_entry": p_bool(), "is_exit": p_bool()},
        allowed_sources=(SourceKind.DERIVED, SourceKind.DIS),
        description="CFG basic block node.",
    ),
    NodeKind.CFG_EXIT: NodeKindContract(
        requires_anchor=False,
        required_props={"code_unit_id": p_str()},
        optional_props={},
        allowed_sources=(SourceKind.DERIVED,),
        description="Synthetic CFG exit node (optional).",
    ),
    NodeKind.DF_DEF: NodeKindContract(
        requires_anchor=False,
        required_props={
            "df_id": p_str(),
            "binding_id": p_str(),
            "instr_id": p_str(),
            "code_unit_id": p_str(),
        },
        optional_props={"def_kind": p_str()},
        allowed_sources=(SourceKind.DERIVED,),
        description="Dataflow definition event node (optional separate node).",
    ),
    NodeKind.DF_USE: NodeKindContract(
        requires_anchor=False,
        required_props={
            "df_id": p_str(),
            "binding_id": p_str(),
            "instr_id": p_str(),
            "code_unit_id": p_str(),
        },
        optional_props={"use_kind": p_str()},
        allowed_sources=(SourceKind.DERIVED,),
        description="Dataflow use event node (optional separate node).",
    ),
    # ---- types ----
    NodeKind.TYPE_EXPR: NodeKindContract(
        requires_anchor=True,
        required_props={"expr_text": p_str("Rendered annotation/type expression")},
        optional_props={
            "expr_kind": p_str(enum=["annotation", "type_comment", "typing_alias", "other"])
        },
        allowed_sources=(SourceKind.LIBCST, SourceKind.PY_AST),
        description="Annotation/type expression node anchored in source.",
    ),
    NodeKind.TYPE: NodeKindContract(
        requires_anchor=False,
        required_props={
            "type_repr": p_str("Normalized type representation (string or JSON)"),
            "type_form": p_str(),
        },
        optional_props={"origin": p_str(enum=["annotation", "inferred", "runtime", "heuristic"])},
        allowed_sources=(SourceKind.DERIVED, SourceKind.LIBCST, SourceKind.INSPECT),
        description="Normalized type node (may represent unions/overloads).",
    ),
    NodeKind.TYPE_PARAM: NodeKindContract(
        requires_anchor=False,
        required_props={"name": p_str("Type parameter name")},
        optional_props={"variance": p_str(enum=["invariant", "covariant", "contravariant"])},
        allowed_sources=(SourceKind.DERIVED, SourceKind.SYMTABLE),
        description="Type parameter node.",
    ),
    NodeKind.TYPE_ALIAS: NodeKindContract(
        requires_anchor=False,
        required_props={"name": p_str("Type alias name"), "target_type_repr": p_str()},
        optional_props={},
        allowed_sources=(SourceKind.DERIVED, SourceKind.SYMTABLE),
        description="Type alias node (typing scopes).",
    ),
    # ---- runtime overlay ----
    NodeKind.RT_OBJECT: NodeKindContract(
        requires_anchor=False,
        required_props={
            "rt_id": p_str("Runtime object identity"),
            "qualname": p_str(),
            "module": p_str(),
        },
        optional_props={"object_type": p_str(), "file": p_str(), "line": p_int()},
        allowed_sources=(SourceKind.INSPECT,),
        description="Runtime object node (optional overlay).",
    ),
    NodeKind.RT_SIGNATURE: NodeKindContract(
        requires_anchor=False,
        required_props={"sig_id": p_str(), "signature_str": p_str()},
        optional_props={},
        allowed_sources=(SourceKind.INSPECT,),
        description="Runtime signature node.",
    ),
    NodeKind.RT_SIGNATURE_PARAM: NodeKindContract(
        requires_anchor=False,
        required_props={"param_id": p_str(), "name": p_str(), "kind": p_str()},
        optional_props={"default_json": p_json(), "annotation_json": p_json()},
        allowed_sources=(SourceKind.INSPECT,),
        description="Runtime signature parameter node.",
    ),
    NodeKind.RT_MEMBER: NodeKindContract(
        requires_anchor=False,
        required_props={"member_id": p_str(), "name": p_str(), "member_kind": p_str()},
        optional_props={"declaring_type": p_str(), "value_repr": p_str()},
        allowed_sources=(SourceKind.INSPECT,),
        description="Runtime member node (getmembers_static row).",
    ),
    # ---- diagnostics ----
    NodeKind.DIAG: NodeKindContract(
        requires_anchor=True,
        required_props={
            "severity": p_str(enum=["INFO", "WARNING", "ERROR"]),
            "message": p_str(),
            "diag_source": p_str(),
        },
        optional_props={"code": p_str(), "details_json": p_json()},
        allowed_sources=(
            SourceKind.TREESITTER,
            SourceKind.SCIP,
            SourceKind.PIPELINE,
            SourceKind.DERIVED,
        ),
        description="Generic diagnostic node (any stage/source).",
    ),
}


EDGE_KIND_CONTRACTS: dict[EdgeKind, EdgeKindContract] = {
    # ---- structure ----
    EdgeKind.REPO_CONTAINS: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={},
        optional_props={"role": p_str()},
        allowed_sources=(SourceKind.PIPELINE,),
        description="Repository contains file edge.",
    ),
    EdgeKind.FILE_DECLARES_MODULE: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={"module_fqn": p_str()},
        optional_props={},
        allowed_sources=(SourceKind.DERIVED, SourceKind.LIBCST),
        description="File declares module edge.",
    ),
    EdgeKind.FILE_CONTAINS: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={
            "role": p_str(enum=["syntax", "scope", "symbol", "flow", "diag", "runtime", "type"])
        },
        optional_props={},
        allowed_sources=(SourceKind.PIPELINE, SourceKind.DERIVED),
        description="File contains an anchored node (typed by role).",
    ),
    EdgeKind.CST_PARENT_OF: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={},
        optional_props={"field": p_str(), "index": p_int()},
        allowed_sources=(SourceKind.LIBCST,),
        description="LibCST structural parent edge.",
    ),
    EdgeKind.AST_PARENT_OF: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={},
        optional_props={"field": p_str(), "index": p_int()},
        allowed_sources=(SourceKind.PY_AST,),
        description="AST structural parent edge.",
    ),
    EdgeKind.TS_PARENT_OF: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={},
        optional_props={"field": p_str(), "index": p_int()},
        allowed_sources=(SourceKind.TREESITTER,),
        description="tree-sitter structural parent edge.",
    ),
    # ---- scopes/bindings ----
    EdgeKind.SCOPE_PARENT: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={},
        optional_props={},
        allowed_sources=(SourceKind.SYMTABLE, SourceKind.DERIVED),
        description="Scope parent relationship.",
    ),
    EdgeKind.SCOPE_BINDS: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={},
        optional_props={"binding_kind": p_str()},
        allowed_sources=(SourceKind.SYMTABLE, SourceKind.DERIVED),
        description="Scope binds a binding slot.",
    ),
    EdgeKind.BINDING_RESOLVES_TO: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={"kind": p_str(enum=["GLOBAL", "NONLOCAL", "FREE", "UNKNOWN"])},
        optional_props={"reason": p_str()},
        allowed_sources=(SourceKind.DERIVED, SourceKind.SYMTABLE),
        description="Binding resolves to outer binding (closure/global/nonlocal).",
    ),
    EdgeKind.DEF_SITE_OF: EdgeKindContract(
        requires_evidence_anchor=True,
        required_props={
            "def_site_kind": p_str(enum=["function", "class", "import", "assign", "param", "other"])
        },
        optional_props={},
        allowed_sources=(SourceKind.DERIVED,),
        description="Definition site establishes a binding (anchored evidence).",
    ),
    EdgeKind.USE_SITE_OF: EdgeKindContract(
        requires_evidence_anchor=True,
        required_props={
            "use_kind": p_str(enum=["read", "write", "del", "call", "import", "other"])
        },
        optional_props={},
        allowed_sources=(SourceKind.DERIVED,),
        description="Use site consumes a binding (anchored evidence).",
    ),
    # ---- qname candidates ----
    EdgeKind.NAME_REF_CANDIDATE_QNAME: EdgeKindContract(
        requires_evidence_anchor=True,
        required_props={"qname_source": p_str(), "score": p_float()},
        optional_props={"ambiguity_group_id": p_str()},
        allowed_sources=(SourceKind.LIBCST, SourceKind.DERIVED),
        description="Name ref -> qualified name candidate edge (multi-valued).",
    ),
    EdgeKind.CALLSITE_CANDIDATE_QNAME: EdgeKindContract(
        requires_evidence_anchor=True,
        required_props={"qname_source": p_str(), "score": p_float()},
        optional_props={"ambiguity_group_id": p_str()},
        allowed_sources=(SourceKind.LIBCST, SourceKind.DERIVED),
        description="Callsite -> qualified name candidate edge (multi-valued).",
    ),
    EdgeKind.ATTR_CANDIDATE_QNAME: EdgeKindContract(
        requires_evidence_anchor=True,
        required_props={"qname_source": p_str(), "score": p_float()},
        optional_props={"ambiguity_group_id": p_str()},
        allowed_sources=(SourceKind.LIBCST, SourceKind.DERIVED),
        description="Attribute access -> qualified name candidate edge (multi-valued).",
    ),
    # ---- SCIP semantic edges ----
    EdgeKind.PY_DEFINES_SYMBOL: EdgeKindContract(
        requires_evidence_anchor=True,
        required_props={
            "symbol_roles": p_int(),
            "origin": p_str(enum=["scip"]),
            "resolution_method": p_str(),
        },
        optional_props={"score": p_float(), "rule_priority": p_int(), "rule_name": p_str()},
        allowed_sources=(SourceKind.SCIP, SourceKind.DERIVED),
        description="Definition occurrence defines SCIP symbol.",
    ),
    EdgeKind.PY_REFERENCES_SYMBOL: EdgeKindContract(
        requires_evidence_anchor=True,
        required_props={
            "symbol_roles": p_int(),
            "origin": p_str(enum=["scip"]),
            "resolution_method": p_str(),
        },
        optional_props={"score": p_float(), "rule_priority": p_int(), "rule_name": p_str()},
        allowed_sources=(SourceKind.SCIP, SourceKind.DERIVED),
        description="Reference occurrence references SCIP symbol.",
    ),
    EdgeKind.PY_IMPORTS_SYMBOL: EdgeKindContract(
        requires_evidence_anchor=True,
        required_props={
            "symbol_roles": p_int(),
            "origin": p_str(enum=["scip"]),
            "resolution_method": p_str(),
        },
        optional_props={"score": p_float(), "rule_priority": p_int(), "rule_name": p_str()},
        allowed_sources=(SourceKind.SCIP, SourceKind.DERIVED),
        description="Import occurrence imports SCIP symbol.",
    ),
    EdgeKind.PY_READS_SYMBOL: EdgeKindContract(
        requires_evidence_anchor=True,
        required_props={
            "symbol_roles": p_int(),
            "origin": p_str(enum=["scip"]),
            "resolution_method": p_str(),
        },
        optional_props={},
        allowed_sources=(SourceKind.SCIP, SourceKind.DERIVED),
        description="Occurrence reads SCIP symbol (role bit).",
    ),
    EdgeKind.PY_WRITES_SYMBOL: EdgeKindContract(
        requires_evidence_anchor=True,
        required_props={
            "symbol_roles": p_int(),
            "origin": p_str(enum=["scip"]),
            "resolution_method": p_str(),
        },
        optional_props={},
        allowed_sources=(SourceKind.SCIP, SourceKind.DERIVED),
        description="Occurrence writes SCIP symbol (role bit).",
    ),
    # ---- calls ----
    EdgeKind.PY_CALLS_SYMBOL: EdgeKindContract(
        requires_evidence_anchor=True,
        required_props={
            "origin": p_str(enum=["scip"]),
            "resolution_method": p_str(),
            "score": p_float(),
        },
        optional_props={"symbol_roles": p_int(), "rule_priority": p_int()},
        allowed_sources=(SourceKind.SCIP, SourceKind.DERIVED),
        description="Callsite resolved to SCIP symbol (preferred).",
    ),
    EdgeKind.PY_CALLS_QNAME: EdgeKindContract(
        requires_evidence_anchor=True,
        required_props={
            "origin": p_str(enum=["qname"]),
            "qname_source": p_str(),
            "score": p_float(),
        },
        optional_props={"ambiguity_group_id": p_str()},
        allowed_sources=(SourceKind.LIBCST, SourceKind.DERIVED),
        description="Callsite resolved to qualified name candidate (fallback).",
    ),
    # ---- types ----
    EdgeKind.HAS_ANNOTATION: EdgeKindContract(
        requires_evidence_anchor=True,
        required_props={"origin": p_str(enum=["annotation", "type_comment", "other"])},
        optional_props={},
        allowed_sources=(SourceKind.LIBCST, SourceKind.PY_AST, SourceKind.DERIVED),
        description="Definition/parameter has a type expression annotation.",
    ),
    EdgeKind.INFERRED_TYPE: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={
            "origin": p_str(enum=["inferred", "runtime", "heuristic"]),
            "score": p_float(),
        },
        optional_props={"engine": p_str()},
        allowed_sources=(SourceKind.LIBCST, SourceKind.INSPECT, SourceKind.DERIVED),
        description="Node inferred to have a type.",
    ),
    EdgeKind.TYPE_PARAM_OF: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={},
        optional_props={"scope_type": p_str()},
        allowed_sources=(SourceKind.DERIVED, SourceKind.SYMTABLE),
        description="Type parameter belongs to a def/class/type.",
    ),
    # ---- bytecode/flow/dfg ----
    EdgeKind.BYTECODE_ANCHOR: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={
            "mapping_method": p_str(enum=["line_table", "offset_span", "heuristic"]),
            "score": p_float(),
        },
        optional_props={"anchor_kind": p_str(), "notes": p_str()},
        allowed_sources=(SourceKind.DERIVED, SourceKind.DIS),
        description="Bytecode instruction anchored to a source span node.",
    ),
    EdgeKind.CFG_NEXT: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={"code_unit_id": p_str()},
        optional_props={},
        allowed_sources=(SourceKind.DERIVED,),
        description="CFG fallthrough edge.",
    ),
    EdgeKind.CFG_JUMP: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={"code_unit_id": p_str()},
        optional_props={"jump_off": p_int()},
        allowed_sources=(SourceKind.DERIVED,),
        description="CFG unconditional jump edge.",
    ),
    EdgeKind.CFG_BRANCH_TRUE: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={"code_unit_id": p_str()},
        optional_props={"cond_instr_id": p_str()},
        allowed_sources=(SourceKind.DERIVED,),
        description="CFG conditional true branch.",
    ),
    EdgeKind.CFG_BRANCH_FALSE: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={"code_unit_id": p_str()},
        optional_props={"cond_instr_id": p_str()},
        allowed_sources=(SourceKind.DERIVED,),
        description="CFG conditional false branch.",
    ),
    EdgeKind.CFG_BRANCH: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={"code_unit_id": p_str()},
        optional_props={"cond_instr_id": p_str()},
        allowed_sources=(SourceKind.DERIVED,),
        description="CFG conditional branch (polarity unknown or merged).",
    ),
    EdgeKind.CFG_EXC: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={"code_unit_id": p_str(), "exc_entry_index": p_int()},
        optional_props={"depth": p_int(), "lasti": p_int()},
        allowed_sources=(SourceKind.DERIVED,),
        description="CFG exception edge derived from exception table.",
    ),
    EdgeKind.STEP_DEF: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={"code_unit_id": p_str(), "instr_id": p_str(), "binding_id": p_str()},
        optional_props={"def_kind": p_str()},
        allowed_sources=(SourceKind.DERIVED,),
        description="Instruction defines a binding (def-use event).",
    ),
    EdgeKind.STEP_USE: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={"code_unit_id": p_str(), "instr_id": p_str(), "binding_id": p_str()},
        optional_props={"use_kind": p_str()},
        allowed_sources=(SourceKind.DERIVED,),
        description="Instruction uses a binding (def-use event).",
    ),
    EdgeKind.REACHES: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={"code_unit_id": p_str()},
        optional_props={"path_length": p_int()},
        allowed_sources=(SourceKind.DERIVED,),
        description="Dataflow reaching-definitions edge: def reaches use.",
    ),
    # ---- runtime overlay ----
    EdgeKind.RT_HAS_SIGNATURE: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={},
        optional_props={},
        allowed_sources=(SourceKind.INSPECT,),
        description="Runtime object has a signature.",
    ),
    EdgeKind.RT_HAS_PARAM: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={},
        optional_props={"position": p_int()},
        allowed_sources=(SourceKind.INSPECT,),
        description="Signature has parameter.",
    ),
    EdgeKind.RT_HAS_MEMBER: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={},
        optional_props={"is_descriptor": p_bool()},
        allowed_sources=(SourceKind.INSPECT,),
        description="Runtime object has member.",
    ),
    EdgeKind.RT_WRAPS: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={},
        optional_props={"unwrap_depth": p_int()},
        allowed_sources=(SourceKind.INSPECT,),
        description="Runtime object wraps another object (unwrap chain).",
    ),
    # ---- diagnostics ----
    EdgeKind.HAS_DIAGNOSTIC: EdgeKindContract(
        requires_evidence_anchor=False,
        required_props={"diag_source": p_str()},
        optional_props={"severity": p_str()},
        allowed_sources=(SourceKind.TREESITTER, SourceKind.SCIP, SourceKind.DERIVED),
        description="Attach diagnostic node to file/module/other entity.",
    ),
}


# ----------------------------
# Derivation Manifest (NODE_DERIVATIONS / EDGE_DERIVATIONS)
# ----------------------------

NODE_DERIVATIONS: dict[NodeKind, list[DerivationSpec]] = {
    NodeKind.PY_REPO: [
        DerivationSpec(
            extractor="extract.repo_scan:scan_repo",
            provider_or_field="repo_root + optional git HEAD sniff",
            join_keys=(),
            id_recipe="repo_id = sha256(repo_root + git_commit?)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.PY_FILE: [
        DerivationSpec(
            extractor="extract.repo_scan:scan_repo",
            provider_or_field="filesystem walk + include/exclude globs",
            join_keys=(),
            id_recipe="file_id = sha256('FILE:' + repo_rel_path)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.PY_MODULE: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST FullRepoManager + FullyQualifiedNameProvider",
            join_keys=("path",),
            id_recipe="module_id = sha256('MOD:' + module_fqn)[:16] (or stable_id(path,0,0,'PY_MODULE'))",
            confidence_policy="confidence=0.95 when repo/module roots configured; else 0.7",
            ambiguity_policy="if multiple roots produce multiple fqns, store all module nodes (ambiguity_group_id=path)",
            status="implemented",
        )
    ],
    NodeKind.PY_PACKAGE: [
        DerivationSpec(
            extractor="extract.repo_scan:scan_repo",
            provider_or_field="derive from __init__.py presence + module_fqn mapping",
            join_keys=("path",),
            id_recipe="package_id = sha256('PKG:' + package_fqn)[:16]",
            confidence_policy="confidence=0.8-0.95 depending on packaging heuristics",
            ambiguity_policy="store multiple candidates if package roots ambiguous",
            status="planned",
        )
    ],
    # LibCST nodes (all use ByteSpanPositionProvider)
    NodeKind.CST_NODE: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST parse_module + MetadataWrapper(ByteSpanPositionProvider, PositionProvider, ParentNodeProvider)",
            join_keys=(),
            id_recipe="node_id = stable_id(path, bstart, bend, 'CST_NODE')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.CST_DEF: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST FunctionDef/ClassDef/Lambda nodes + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="def_id = stable_id(path, bstart, bend, 'CST_DEF')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.CST_CLASS_DEF: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST ClassDef + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="def_id = stable_id(path, bstart, bend, 'CST_CLASS_DEF')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.CST_FUNCTION_DEF: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST FunctionDef/AsyncFunctionDef + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="def_id = stable_id(path, bstart, bend, 'CST_FUNCTION_DEF')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.CST_LAMBDA: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST Lambda + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="lambda_id = stable_id(path, bstart, bend, 'CST_LAMBDA')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="planned",
            notes="Often worth emitting only if you need lambda-level flow/type edges.",
        )
    ],
    NodeKind.CST_PARAM: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST Param + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="param_id = stable_id(path, bstart, bend, 'CST_PARAM')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="planned",
        )
    ],
    NodeKind.CST_DECORATOR: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST Decorator + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="decorator_id = stable_id(path, bstart, bend, 'CST_DECORATOR')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="planned",
        )
    ],
    NodeKind.CST_IMPORT: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST Import + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="import_id = stable_id(path, bstart, bend, 'CST_IMPORT')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.CST_IMPORT_FROM: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST ImportFrom + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="import_id = stable_id(path, bstart, bend, 'CST_IMPORT_FROM')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.CST_IMPORT_ALIAS: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST ImportAlias + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="import_alias_id = stable_id(path, bstart, bend, 'CST_IMPORT_ALIAS')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.CST_NAME_REF: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST Name nodes + ExpressionContextProvider + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="name_ref_id = stable_id(path, bstart, bend, 'CST_NAME_REF')",
            confidence_policy="confidence=1.0 for span/name; 0.9 for expr_context when provider yields context; else expr_context='UNKNOWN'",
            ambiguity_policy="none at syntax level; ambiguity appears in resolution edges",
            status="implemented",
        )
    ],
    NodeKind.CST_ATTRIBUTE_ACCESS: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST Attribute + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="attr_id = stable_id(path, bstart, bend, 'CST_ATTRIBUTE_ACCESS')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="resolution ambiguity handled via ATTR_CANDIDATE_QNAME edges",
            status="planned",
        )
    ],
    NodeKind.CST_SUBSCRIPT: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST Subscript + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="sub_id = stable_id(path, bstart, bend, 'CST_SUBSCRIPT')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="n/a",
            status="planned",
        )
    ],
    NodeKind.CST_CALLSITE: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST Call + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="call_id = stable_id(path, bstart, bend, 'CST_CALLSITE')",
            confidence_policy="confidence=1.0 for syntax; call target resolution edges carry confidence/score",
            ambiguity_policy="store multi candidate call edges; winner selection is contract-driven",
            status="implemented",
        )
    ],
    NodeKind.CST_ARG: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST Arg + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="arg_id = stable_id(path, bstart, bend, 'CST_ARG')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="n/a",
            status="planned",
        )
    ],
    NodeKind.CST_LITERAL: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST literal nodes (SimpleString, Integer, Float, etc.) + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="lit_id = stable_id(path, bstart, bend, 'CST_LITERAL')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="n/a",
            status="planned",
        )
    ],
    NodeKind.CST_DOCSTRING: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST docstring detection (first statement string literal in suite) + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="doc_id = stable_id(path, bstart, bend, 'CST_DOCSTRING')",
            confidence_policy="confidence=0.95 (docstring heuristics are conventional but reliable)",
            ambiguity_policy="n/a",
            status="planned",
        )
    ],
    NodeKind.CST_COMMENT: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST Comment nodes + ByteSpanPositionProvider",
            join_keys=(),
            id_recipe="comment_id = stable_id(path, bstart, bend, 'CST_COMMENT')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="n/a",
            status="planned",
        )
    ],
    # AST
    NodeKind.AST_NODE: [
        DerivationSpec(
            extractor="extract.ast_extract:extract_ast_tables",
            provider_or_field="python ast.parse + node walk; (lineno,col)->bytes via normalize.spans",
            join_keys=("path",),
            id_recipe="ast_id = stable_id(path, bstart, bend, 'AST_NODE') when bytes available; else sha(ast_type+lineno+col+path)",
            confidence_policy="confidence=0.95 when end positions present and byte mapping succeeds; else 0.7",
            ambiguity_policy="n/a",
            status="implemented",
        )
    ],
    NodeKind.AST_DEF: [
        DerivationSpec(
            extractor="extract.ast_extract:extract_ast_tables",
            provider_or_field="ast.FunctionDef/AsyncFunctionDef/ClassDef/Lambda",
            join_keys=("path", "lineno", "col_offset"),
            id_recipe="ast_def_id = sha('AST_DEF:'+path+lineno+col)",
            confidence_policy="confidence=0.9 (AST def positions reliable but not trivia-exact)",
            ambiguity_policy="n/a",
            status="planned",
        )
    ],
    NodeKind.AST_CALL: [
        DerivationSpec(
            extractor="extract.ast_extract:extract_ast_tables",
            provider_or_field="ast.Call nodes",
            join_keys=("path", "lineno", "col_offset"),
            id_recipe="ast_call_id = sha('AST_CALL:'+path+lineno+col)",
            confidence_policy="confidence=0.9",
            ambiguity_policy="n/a",
            status="planned",
        )
    ],
    NodeKind.AST_NAME: [
        DerivationSpec(
            extractor="extract.ast_extract:extract_ast_tables",
            provider_or_field="ast.Name nodes",
            join_keys=("path", "lineno", "col_offset"),
            id_recipe="ast_name_id = sha('AST_NAME:'+path+lineno+col)",
            confidence_policy="confidence=0.9",
            ambiguity_policy="n/a",
            status="planned",
        )
    ],
    NodeKind.AST_ATTRIBUTE: [
        DerivationSpec(
            extractor="extract.ast_extract:extract_ast_tables",
            provider_or_field="ast.Attribute nodes",
            join_keys=("path", "lineno", "col_offset"),
            id_recipe="ast_attr_id = sha('AST_ATTRIBUTE:'+path+lineno+col)",
            confidence_policy="confidence=0.9",
            ambiguity_policy="n/a",
            status="planned",
        )
    ],
    # tree-sitter
    NodeKind.TS_NODE: [
        DerivationSpec(
            extractor="extract.tree_sitter_extract:extract_ts_tables",
            provider_or_field="tree-sitter nodes (start_byte/end_byte, is_error/is_missing/has_error)",
            join_keys=(),
            id_recipe="ts_id = stable_id(path, start_byte, end_byte, 'TS_NODE')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="n/a",
            status="implemented",
        )
    ],
    NodeKind.TS_ERROR: [
        DerivationSpec(
            extractor="extract.tree_sitter_extract:extract_ts_tables",
            provider_or_field="node.is_error == True",
            join_keys=(),
            id_recipe="ts_err_id = stable_id(path, start_byte, end_byte, 'TS_ERROR')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="n/a",
            status="implemented",
        )
    ],
    NodeKind.TS_MISSING: [
        DerivationSpec(
            extractor="extract.tree_sitter_extract:extract_ts_tables",
            provider_or_field="node.is_missing == True",
            join_keys=(),
            id_recipe="ts_missing_id = stable_id(path, start_byte, end_byte, 'TS_MISSING')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="n/a",
            status="implemented",
        )
    ],
    # symtable/bindings
    NodeKind.SYM_SCOPE: [
        DerivationSpec(
            extractor="extract.symtable_extract:extract_symtables_table",
            provider_or_field="symtable.symtable(...); table.get_type().name; table.get_children()",
            join_keys=("path",),
            id_recipe="scope_id = f'{file_id}:SCOPE:{qualpath}:{lineno}:{scope_type}:{ordinal}'",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.SYM_SYMBOL: [
        DerivationSpec(
            extractor="extract.symtable_extract:extract_symtables_table",
            provider_or_field="table.lookup(name) and symbol flags: is_local/is_free/is_global/is_nonlocal/is_parameter...",
            join_keys=("scope_id", "name"),
            id_recipe="sym_symbol_id = sha('SYM_SYMBOL:'+scope_id+':'+name)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.PY_SCOPE: [
        DerivationSpec(
            extractor="normalize.schema_infer:build_scopes",
            provider_or_field="normalize SYM_SCOPE (+ optional LibCST ScopeProvider ids) into a single scope namespace",
            join_keys=("scope_id",),
            id_recipe="py_scope_id = scope_id",
            confidence_policy="confidence=0.95 (symtable authoritative; libcst scope can enrich)",
            ambiguity_policy="if libcst vs symtable mismatch, keep both and link via props or resolution edges",
            status="planned",
        )
    ],
    NodeKind.PY_BINDING: [
        DerivationSpec(
            extractor="normalize.schema_infer:build_bindings",
            provider_or_field="derive from symtable symbol flags per (scope,name)",
            join_keys=("scope_id", "name"),
            id_recipe="binding_id = f'{scope_id}:BIND:{name}'",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="planned",
        )
    ],
    NodeKind.PY_DEF_SITE: [
        DerivationSpec(
            extractor="normalize.schema_infer:build_def_use_sites",
            provider_or_field="LibCST Name spans + symtable binding classification (def sites)",
            join_keys=("path", "bstart", "bend", "name"),
            id_recipe="def_site_id = stable_id(path, bstart, bend, 'PY_DEF_SITE')",
            confidence_policy="confidence=0.85-0.95 depending on def-site classifier maturity",
            ambiguity_policy="if multiple bindings match, emit multiple DEF_SITE_OF edges (ambiguity_group_id=def_site_id)",
            status="planned",
        )
    ],
    NodeKind.PY_USE_SITE: [
        DerivationSpec(
            extractor="normalize.schema_infer:build_def_use_sites",
            provider_or_field="LibCST Name spans + expr_context + symtable binding resolution",
            join_keys=("path", "bstart", "bend", "name"),
            id_recipe="use_site_id = stable_id(path, bstart, bend, 'PY_USE_SITE')",
            confidence_policy="confidence=0.85-0.95 depending on binding resolution maturity",
            ambiguity_policy="emit multiple USE_SITE_OF edges if needed; dedupe by winner policy",
            status="planned",
        )
    ],
    # SCIP
    NodeKind.SCIP_INDEX: [
        DerivationSpec(
            extractor="extract.scip_extract:extract_scip_tables",
            provider_or_field="protobuf decode of index.scip + Index metadata",
            join_keys=(),
            id_recipe="scip_index_id = sha('SCIP_INDEX:'+scip_index_path)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.SCIP_DOCUMENT: [
        DerivationSpec(
            extractor="extract.scip_extract:extract_scip_tables",
            provider_or_field="SCIP Document.relative_path",
            join_keys=("path",),
            id_recipe="doc_id = sha('SCIP_DOC:'+path)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.SCIP_SYMBOL: [
        DerivationSpec(
            extractor="extract.scip_extract:extract_scip_tables",
            provider_or_field="SCIP SymbolInformation.symbol + fields",
            join_keys=("symbol",),
            id_recipe="symbol_node_id = symbol (or sha('SCIP_SYMBOL:'+symbol)[:16])",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.SCIP_OCCURRENCE: [
        DerivationSpec(
            extractor="normalize.spans:add_scip_occurrence_byte_spans",
            provider_or_field="Occurrence(symbol, symbol_roles, range) normalized to bytes",
            join_keys=("path", "range"),
            id_recipe="occ_id = stable_id(path, bstart, bend, 'SCIP_OCCURRENCE')",
            confidence_policy="confidence=1.0 for symbol+roles; 0.9 for byte span mapping when encoding tricky",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.SCIP_DIAGNOSTIC: [
        DerivationSpec(
            extractor="extract.scip_extract:extract_scip_tables",
            provider_or_field="Document.diagnostics (if present) normalized to bytes",
            join_keys=("path", "range"),
            id_recipe="diag_id = stable_id(path, bstart, bend, 'SCIP_DIAGNOSTIC')",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="planned",
        )
    ],
    # qualified names
    NodeKind.PY_QUALIFIED_NAME: [
        DerivationSpec(
            extractor="hamilton_pipeline.modules.normalization:dim_qualified_names",
            provider_or_field="LibCST QualifiedNameProvider/FullyQualifiedNameProvider candidate strings",
            join_keys=("qname",),
            id_recipe="qname_id = sha('QNAME:'+qname)[:16]",
            confidence_policy="confidence=0.6-0.9 depending on provider (FullyQualified > Qualified)",
            ambiguity_policy="qname nodes are distinct; ambiguity lives on edges from ref->qname",
            status="implemented",
        )
    ],
    NodeKind.PY_MODULE_FQN: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="FullyQualifiedNameProvider on Module",
            join_keys=("module_fqn",),
            id_recipe="module_fqn_id = sha('MODFQN:'+module_fqn)[:16]",
            confidence_policy="confidence=0.95",
            ambiguity_policy="if multiple fqns, keep them and attach ambiguity_group_id=path",
            status="planned",
        )
    ],
    # bytecode / flow
    NodeKind.BC_CODE_UNIT: [
        DerivationSpec(
            extractor="extract.bytecode_extract:extract_bytecode_table",
            provider_or_field="compile + traverse code objects; record co_qualname/co_firstlineno; dis-compatible",
            join_keys=("path", "qualname", "co_firstlineno"),
            id_recipe="code_unit_id = sha('BC:'+path+':'+qualname+':'+firstlineno)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.BC_INSTR: [
        DerivationSpec(
            extractor="extract.bytecode_extract:extract_bytecode_table",
            provider_or_field="dis.get_instructions(codeobj) + store baseopname",
            join_keys=("code_unit_id", "offset"),
            id_recipe="instr_id = sha(code_unit_id+':OFF:'+offset)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.CFG_BLOCK: [
        DerivationSpec(
            extractor="normalize.bytecode_cfg:build_cfg_blocks",
            provider_or_field="basic block formation + jump semantics + exception table edges",
            join_keys=("code_unit_id",),
            id_recipe="block_id = f'{code_unit_id}:B:{start_off}'",
            confidence_policy="confidence=0.95",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.CFG_EXIT: [
        DerivationSpec(
            extractor="normalize.bytecode_cfg:build_cfg",
            provider_or_field="synthetic per code_unit_id",
            join_keys=("code_unit_id",),
            id_recipe="exit_id = sha(code_unit_id+':EXIT')[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="planned",
        )
    ],
    NodeKind.DF_DEF: [
        DerivationSpec(
            extractor="normalize.bytecode_dfg:build_def_use_events",
            provider_or_field="opcode classifier -> def events; stack/locals/globals model",
            join_keys=("code_unit_id", "instr_id", "binding_id"),
            id_recipe="df_id = sha('DEF:'+code_unit_id+':'+instr_id+':'+binding_id)[:16]",
            confidence_policy="confidence=0.8-0.95 depending on opcode coverage",
            ambiguity_policy="if binding ambiguous, emit multiple DF_DEF candidates with scores",
            status="implemented",
        )
    ],
    NodeKind.DF_USE: [
        DerivationSpec(
            extractor="normalize.bytecode_dfg:build_def_use_events",
            provider_or_field="opcode classifier -> use events",
            join_keys=("code_unit_id", "instr_id", "binding_id"),
            id_recipe="df_id = sha('USE:'+code_unit_id+':'+instr_id+':'+binding_id)[:16]",
            confidence_policy="confidence=0.8-0.95 depending on opcode coverage",
            ambiguity_policy="if binding ambiguous, emit multiple DF_USE candidates with scores",
            status="implemented",
        )
    ],
    # types
    NodeKind.TYPE_EXPR: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST annotation nodes + ByteSpanPositionProvider (or AST AnnAssign/arg.annotation)",
            join_keys=(),
            id_recipe="type_expr_id = stable_id(path, bstart, bend, 'TYPE_EXPR')",
            confidence_policy="confidence=1.0 for syntax capture",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.TYPE: [
        DerivationSpec(
            extractor="normalize.types:normalize_types",
            provider_or_field="normalize annotation strings + optional inference (Pyre/Watchman via LibCST TypeInferenceProvider)",
            join_keys=("type_repr",),
            id_recipe="type_id = sha('TYPE:'+type_repr)[:16]",
            confidence_policy="annotation-derived=1.0; inferred=0.7-0.9; runtime=0.6-0.9",
            ambiguity_policy="multi-valued types allowed (union/overload); store multiple TYPE nodes/edges",
            status="implemented",
        )
    ],
    NodeKind.TYPE_PARAM: [
        DerivationSpec(
            extractor="extract.symtable_extract:extract_symtables_table",
            provider_or_field="symtable meta scopes TYPE_PARAMETERS/TYPE_VARIABLE",
            join_keys=("scope_id", "name"),
            id_recipe="type_param_id = sha('TP:'+scope_id+':'+name)[:16]",
            confidence_policy="confidence=0.9",
            ambiguity_policy="none",
            status="planned",
        )
    ],
    NodeKind.TYPE_ALIAS: [
        DerivationSpec(
            extractor="extract.symtable_extract:extract_symtables_table",
            provider_or_field="symtable TYPE_ALIAS scope + extracted assignments",
            join_keys=("scope_id", "name"),
            id_recipe="type_alias_id = sha('TA:'+scope_id+':'+name)[:16]",
            confidence_policy="confidence=0.7-0.9 (depends on alias extraction fidelity)",
            ambiguity_policy="if alias target ambiguous, keep multiple targets with scores",
            status="planned",
        )
    ],
    # runtime overlay
    NodeKind.RT_OBJECT: [
        DerivationSpec(
            extractor="extract.runtime_inspect_extract:extract_runtime_objects",
            provider_or_field="inspect.getmembers_static + inspect.unwrap + safe signature extraction",
            join_keys=("module", "qualname"),
            id_recipe="rt_id = sha('RT:'+module+':'+qualname)[:16]",
            confidence_policy="confidence=0.6-0.9 depending on ability to map back to source",
            ambiguity_policy="if multiple objects share qualname, include module+id() fingerprint in rt_id",
            status="implemented",
        )
    ],
    NodeKind.RT_SIGNATURE: [
        DerivationSpec(
            extractor="extract.runtime_inspect_extract:extract_runtime_signatures",
            provider_or_field="inspect.signature(obj) (safe objects only)",
            join_keys=("rt_id",),
            id_recipe="sig_id = sha(rt_id+':SIG:'+signature_str)[:16]",
            confidence_policy="confidence=0.8-1.0 depending on callable type",
            ambiguity_policy="if signature fails, emit DIAG instead",
            status="implemented",
        )
    ],
    NodeKind.RT_SIGNATURE_PARAM: [
        DerivationSpec(
            extractor="extract.runtime_inspect_extract:extract_runtime_signatures",
            provider_or_field="signature.parameters items",
            join_keys=("sig_id", "name"),
            id_recipe="param_id = sha(sig_id+':P:'+name)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.RT_MEMBER: [
        DerivationSpec(
            extractor="extract.runtime_inspect_extract:extract_runtime_members",
            provider_or_field="inspect.getmembers_static(obj)",
            join_keys=("rt_id", "name"),
            id_recipe="member_id = sha(rt_id+':M:'+name)[:16]",
            confidence_policy="confidence=0.9",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    NodeKind.DIAG: [
        DerivationSpec(
            extractor="normalize.diagnostics:collect_diags",
            provider_or_field="aggregate parse errors (tree-sitter), SCIP diagnostics, extraction failures",
            join_keys=("path", "bstart", "bend"),
            id_recipe="diag_id = stable_id(path, bstart, bend, 'DIAG')",
            confidence_policy="confidence=1.0 for source diag; 0.7 for derived diag",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
}


EDGE_DERIVATIONS: dict[EdgeKind, list[DerivationSpec]] = {
    EdgeKind.REPO_CONTAINS: [
        DerivationSpec(
            extractor="extract.repo_scan:scan_repo",
            provider_or_field="repo_files list",
            join_keys=("repo_id -> file_id",),
            id_recipe="edge_id = sha('REPO_CONTAINS:'+repo_id+':'+file_id)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    EdgeKind.FILE_DECLARES_MODULE: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="FullyQualifiedNameProvider on Module",
            join_keys=("file.path == module.path",),
            id_recipe="edge_id = sha('FILE_DECLARES_MODULE:'+file_id+':'+module_id)[:16]",
            confidence_policy="confidence=0.95",
            ambiguity_policy="if multiple module fqns, emit multiple edges with ambiguity_group_id=file_id",
            status="planned",
        )
    ],
    EdgeKind.FILE_CONTAINS: [
        DerivationSpec(
            extractor="cpg.build_nodes:build_cpg_nodes_raw",
            provider_or_field="attach file_id on anchored nodes",
            join_keys=("node.file_id",),
            id_recipe="edge_id = sha('FILE_CONTAINS:'+file_id+':'+node_id+':'+role)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="planned",
        )
    ],
    # structural
    EdgeKind.CST_PARENT_OF: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST ParentNodeProvider (parent pointers) + traversal order",
            join_keys=("parent_node_id -> child_node_id",),
            id_recipe="edge_id = sha('CST_PARENT_OF:'+parent_id+':'+child_id)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="planned",
        )
    ],
    EdgeKind.AST_PARENT_OF: [
        DerivationSpec(
            extractor="extract.ast_extract:extract_ast_tables",
            provider_or_field="AST walk (parent pointers via stack)",
            join_keys=("parent_ast_id -> child_ast_id",),
            id_recipe="edge_id = sha('AST_PARENT_OF:'+p+':'+c)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="planned",
        )
    ],
    EdgeKind.TS_PARENT_OF: [
        DerivationSpec(
            extractor="extract.tree_sitter_extract:extract_ts_tables",
            provider_or_field="tree-sitter node.children",
            join_keys=("parent_ts_id -> child_ts_id",),
            id_recipe="edge_id = sha('TS_PARENT_OF:'+p+':'+c)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="planned",
        )
    ],
    # scopes/bindings
    EdgeKind.SCOPE_PARENT: [
        DerivationSpec(
            extractor="extract.symtable_extract:extract_symtables_table",
            provider_or_field="symtable table.get_children() recursion",
            join_keys=("child.scope_id -> parent.scope_id",),
            id_recipe="edge_id = sha('SCOPE_PARENT:'+child+':'+parent)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    EdgeKind.SCOPE_BINDS: [
        DerivationSpec(
            extractor="normalize.schema_infer:build_bindings",
            provider_or_field="symtable symbols per scope -> PY_BINDING nodes",
            join_keys=("scope_id",),
            id_recipe="edge_id = sha('SCOPE_BINDS:'+scope_id+':'+binding_id)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="planned",
        )
    ],
    EdgeKind.BINDING_RESOLVES_TO: [
        DerivationSpec(
            extractor="normalize.schema_infer:resolve_bindings",
            provider_or_field="symtable flags is_global/is_nonlocal/is_free + parent chain",
            join_keys=("binding_id",),
            id_recipe="edge_id = sha('BINDING_RESOLVES_TO:'+binding_id+':'+outer_binding_id)[:16]",
            confidence_policy="confidence=0.9",
            ambiguity_policy="if multiple candidates, emit multiple edges with scores and ambiguity_group_id=binding_id",
            status="planned",
        )
    ],
    EdgeKind.DEF_SITE_OF: [
        DerivationSpec(
            extractor="normalize.schema_infer:build_def_use_sites",
            provider_or_field="LibCST def sites (Name spans) classified via symtable binding_kind",
            join_keys=("path", "bstart", "bend", "binding_id"),
            id_recipe="edge_id = sha('DEF_SITE_OF:'+def_site_id+':'+binding_id)[:16]",
            confidence_policy="confidence=0.85-0.95",
            ambiguity_policy="multiple edges allowed, winner selection by score",
            status="planned",
        )
    ],
    EdgeKind.USE_SITE_OF: [
        DerivationSpec(
            extractor="normalize.schema_infer:build_def_use_sites",
            provider_or_field="LibCST use sites (Name spans) + expr_context + symtable resolution",
            join_keys=("path", "bstart", "bend", "binding_id"),
            id_recipe="edge_id = sha('USE_SITE_OF:'+use_site_id+':'+binding_id)[:16]",
            confidence_policy="confidence=0.85-0.95",
            ambiguity_policy="multiple edges allowed, winner selection by score",
            status="planned",
        )
    ],
    # qname candidate edges
    EdgeKind.NAME_REF_CANDIDATE_QNAME: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="LibCST QualifiedNameProvider/FullyQualifiedNameProvider mapping for Name nodes",
            join_keys=("name_ref_id", "qname"),
            id_recipe="edge_id = sha('NAME_REF_CAND_QNAME:'+name_ref_id+':'+qname_id)[:16]",
            confidence_policy="FullyQualified=0.9, Qualified=0.7; score based on provider rank",
            ambiguity_policy="multi-valued; ambiguity_group_id = sha(name_ref_id+':QNAME')[:16]",
            status="planned",
        )
    ],
    EdgeKind.CALLSITE_CANDIDATE_QNAME: [
        DerivationSpec(
            extractor="hamilton_pipeline.modules.normalization:callsite_qname_candidates",
            provider_or_field="callee_qnames list explosion from LibCST callsites",
            join_keys=("call_id", "qname"),
            id_recipe="edge_id = sha('CALL_CAND_QNAME:'+call_id+':'+qname_id)[:16]",
            confidence_policy="score based on candidate position; confidence default 0.6",
            ambiguity_policy="multi-valued; ambiguity_group_id = call_id",
            status="implemented",
        )
    ],
    EdgeKind.ATTR_CANDIDATE_QNAME: [
        DerivationSpec(
            extractor="extract.cst_extract:extract_cst_tables",
            provider_or_field="QualifiedNameProvider mapping for Attribute nodes",
            join_keys=("attr_id", "qname"),
            id_recipe="edge_id = sha('ATTR_CAND_QNAME:'+attr_id+':'+qname_id)[:16]",
            confidence_policy="confidence=0.6-0.8",
            ambiguity_policy="multi-valued; ambiguity_group_id=attr_id",
            status="planned",
        )
    ],
    # SCIP semantic edges (span join)
    EdgeKind.PY_DEFINES_SYMBOL: [
        DerivationSpec(
            extractor="hamilton_pipeline.modules.cpg_build:relationship_registry",
            provider_or_field="SCIP Occurrence.symbol_roles bitmask & Definition",
            join_keys=(
                "interval_align: left.path == right.path",
                "interval_align: right.bstart>=left.bstart AND right.bend<=left.bend (CONTAINED_BEST)",
            ),
            id_recipe="edge_id = sha('PY_DEFINES_SYMBOL:'+src_id+':'+symbol+':'+path+':'+bstart+':'+bend)[:16]",
            confidence_policy="confidence=1.0; score=-span_len (more specific wins); tie-break rule_priority asc",
            ambiguity_policy="if multiple overlaps, group by src_id and keep-first-after-sort by score/confidence/rule_priority",
            status="implemented",
        )
    ],
    EdgeKind.PY_REFERENCES_SYMBOL: [
        DerivationSpec(
            extractor="hamilton_pipeline.modules.cpg_build:relationship_registry",
            provider_or_field="SCIP Occurrence.symbol_roles without Definition bit",
            join_keys=(
                "interval_align: left.path == right.path",
                "interval_align: right span contained in left span (CONTAINED_BEST)",
            ),
            id_recipe="edge_id = sha('PY_REFERENCES_SYMBOL:'+src_id+':'+symbol+':'+path+':'+bstart+':'+bend)[:16]",
            confidence_policy="confidence=1.0; score=-span_len; tie-break rule_priority asc",
            ambiguity_policy="same as defines",
            status="implemented",
        )
    ],
    EdgeKind.PY_IMPORTS_SYMBOL: [
        DerivationSpec(
            extractor="hamilton_pipeline.modules.cpg_build:relationship_registry",
            provider_or_field="SCIP Occurrence.symbol_roles bitmask & Import",
            join_keys=("interval_align: left import alias span contains occurrence span",),
            id_recipe="edge_id = sha('PY_IMPORTS_SYMBOL:'+src_id+':'+symbol+':'+path+':'+bstart+':'+bend)[:16]",
            confidence_policy="confidence=1.0; score=-span_len",
            ambiguity_policy="dedupe by (src_id,symbol,span) + tie-break by rule_priority",
            status="implemented",
        )
    ],
    EdgeKind.PY_READS_SYMBOL: [
        DerivationSpec(
            extractor="cpg.build_edges:build_cpg_edges_raw",
            provider_or_field="filter rel_name_symbol by symbol_roles & READ bit",
            join_keys=("name_ref_id", "symbol"),
            id_recipe="edge_id = sha('PY_READS_SYMBOL:'+name_ref_id+':'+symbol+':'+span)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="same as rel_name_symbol dedupe",
            status="implemented",
        )
    ],
    EdgeKind.PY_WRITES_SYMBOL: [
        DerivationSpec(
            extractor="cpg.build_edges:build_cpg_edges_raw",
            provider_or_field="filter rel_name_symbol by symbol_roles & WRITE bit",
            join_keys=("name_ref_id", "symbol"),
            id_recipe="edge_id = sha('PY_WRITES_SYMBOL:'+name_ref_id+':'+symbol+':'+span)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="same as rel_name_symbol dedupe",
            status="implemented",
        )
    ],
    # Calls
    EdgeKind.PY_CALLS_SYMBOL: [
        DerivationSpec(
            extractor="hamilton_pipeline.modules.cpg_build:relationship_registry",
            provider_or_field="SCIP occurrence aligned to callee span",
            join_keys=("interval_align: callsite.callee_span contains occurrence span",),
            id_recipe="edge_id = sha('PY_CALLS_SYMBOL:'+call_id+':'+symbol+':'+call_span)[:16]",
            confidence_policy="confidence=1.0; score=-span_len",
            ambiguity_policy="if multiple, keep best by score then rule_priority",
            status="implemented",
        )
    ],
    EdgeKind.PY_CALLS_QNAME: [
        DerivationSpec(
            extractor="cpg.build_edges:build_cpg_edges_raw",
            provider_or_field="fallback: callsite_qname_candidates join dim_qualified_names (only when no callsite->symbol)",
            join_keys=("call_id", "qname"),
            id_recipe="edge_id = sha('PY_CALLS_QNAME:'+call_id+':'+qname_id)[:16]",
            confidence_policy="confidence=0.5-0.7; score from candidate ranking",
            ambiguity_policy="multi-valued; ambiguity_group_id=call_id; may keep top-K",
            status="implemented",
        )
    ],
    # Types
    EdgeKind.HAS_ANNOTATION: [
        DerivationSpec(
            extractor="cpg.build_edges:build_cpg_edges_raw",
            provider_or_field="LibCST annotation nodes (param.annotation / returns annotation)",
            join_keys=("def/param -> TYPE_EXPR span",),
            id_recipe="edge_id = sha('HAS_ANNOTATION:'+src_id+':'+type_expr_id)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    EdgeKind.INFERRED_TYPE: [
        DerivationSpec(
            extractor="cpg.build_edges:build_cpg_edges_raw",
            provider_or_field="LibCST TypeInferenceProvider (Pyre/Watchman) OR heuristic OR runtime",
            join_keys=("src_id",),
            id_recipe="edge_id = sha('INFERRED_TYPE:'+src_id+':'+type_id+':'+origin)[:16]",
            confidence_policy="pyre=0.8-0.9; runtime=0.6-0.9; heuristic=0.5-0.7",
            ambiguity_policy="multi-valued types allowed; dedupe by type_id with score",
            status="implemented",
        )
    ],
    EdgeKind.TYPE_PARAM_OF: [
        DerivationSpec(
            extractor="extract.symtable_extract:extract_symtables_table",
            provider_or_field="symtable TYPE_PARAMETERS meta scopes -> attach to owning def/class",
            join_keys=("type_param.scope_id -> owner.scope_id",),
            id_recipe="edge_id = sha('TYPE_PARAM_OF:'+type_param_id+':'+owner_id)[:16]",
            confidence_policy="confidence=0.8-0.95",
            ambiguity_policy="none",
            status="planned",
        )
    ],
    # Bytecode/flow/dfg
    EdgeKind.BYTECODE_ANCHOR: [
        DerivationSpec(
            extractor="normalize.bytecode_anchor:anchor_instructions",
            provider_or_field="instruction.starts_line / line table / heuristic span mapping",
            join_keys=("instr_id -> anchor span",),
            id_recipe="edge_id = sha('BYTECODE_ANCHOR:'+instr_id+':'+anchor_id)[:16]",
            confidence_policy="line_table=0.9, offset_span=0.8, heuristic=0.6",
            ambiguity_policy="if multiple anchors, keep all with scores; winner selection by score",
            status="implemented",
        )
    ],
    EdgeKind.CFG_NEXT: [
        DerivationSpec(
            extractor="normalize.bytecode_cfg:build_cfg_edges",
            provider_or_field="fallthrough edges between basic blocks",
            join_keys=("block_id",),
            id_recipe="edge_id = sha('CFG_NEXT:'+src_block+':'+dst_block)[:16]",
            confidence_policy="confidence=0.95",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    EdgeKind.CFG_JUMP: [
        DerivationSpec(
            extractor="normalize.bytecode_cfg:build_cfg_edges",
            provider_or_field="unconditional jump semantics",
            join_keys=("block_id",),
            id_recipe="edge_id = sha('CFG_JUMP:'+src_block+':'+dst_block)[:16]",
            confidence_policy="confidence=0.95",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    EdgeKind.CFG_BRANCH_TRUE: [
        DerivationSpec(
            extractor="normalize.bytecode_cfg:build_cfg_edges",
            provider_or_field="conditional jump true target",
            join_keys=("block_id",),
            id_recipe="edge_id = sha('CFG_BRANCH_TRUE:'+src_block+':'+dst_block)[:16]",
            confidence_policy="confidence=0.95",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    EdgeKind.CFG_BRANCH_FALSE: [
        DerivationSpec(
            extractor="normalize.bytecode_cfg:build_cfg_edges",
            provider_or_field="conditional jump false target (fallthrough)",
            join_keys=("block_id",),
            id_recipe="edge_id = sha('CFG_BRANCH_FALSE:'+src_block+':'+dst_block)[:16]",
            confidence_policy="confidence=0.95",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    EdgeKind.CFG_BRANCH: [
        DerivationSpec(
            extractor="normalize.bytecode_cfg:build_cfg_edges",
            provider_or_field="merged branch edges when polarity not tracked",
            join_keys=("block_id",),
            id_recipe="edge_id = sha('CFG_BRANCH:'+src_block+':'+dst_block)[:16]",
            confidence_policy="confidence=0.9",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    EdgeKind.CFG_EXC: [
        DerivationSpec(
            extractor="normalize.bytecode_cfg:build_cfg_edges",
            provider_or_field="exception table projection -> handler entry edges",
            join_keys=("block_id",),
            id_recipe="edge_id = sha('CFG_EXC:'+src_block+':'+handler_block+':'+exc_entry_index)[:16]",
            confidence_policy="confidence=0.9-0.95",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    EdgeKind.STEP_DEF: [
        DerivationSpec(
            extractor="normalize.bytecode_dfg:build_def_use_events",
            provider_or_field="opcode classifier -> def event",
            join_keys=("instr_id", "binding_id"),
            id_recipe="edge_id = sha('STEP_DEF:'+instr_id+':'+binding_id)[:16]",
            confidence_policy="confidence=0.8-0.95",
            ambiguity_policy="if binding ambiguous, keep multiple with scores",
            status="implemented",
        )
    ],
    EdgeKind.STEP_USE: [
        DerivationSpec(
            extractor="normalize.bytecode_dfg:build_def_use_events",
            provider_or_field="opcode classifier -> use event",
            join_keys=("instr_id", "binding_id"),
            id_recipe="edge_id = sha('STEP_USE:'+instr_id+':'+binding_id)[:16]",
            confidence_policy="confidence=0.8-0.95",
            ambiguity_policy="if binding ambiguous, keep multiple with scores",
            status="implemented",
        )
    ],
    EdgeKind.REACHES: [
        DerivationSpec(
            extractor="normalize.bytecode_dfg:run_reaching_defs",
            provider_or_field="reaching definitions analysis over CFG",
            join_keys=("code_unit_id",),
            id_recipe="edge_id = sha('REACHES:'+def_id+':'+use_id)[:16]",
            confidence_policy="confidence=0.8-0.95",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    # Runtime overlay
    EdgeKind.RT_HAS_SIGNATURE: [
        DerivationSpec(
            extractor="cpg.build_edges:build_cpg_edges_raw",
            provider_or_field="inspect.signature(obj)",
            join_keys=("rt_id",),
            id_recipe="edge_id = sha('RT_HAS_SIGNATURE:'+rt_id+':'+sig_id)[:16]",
            confidence_policy="confidence=0.9",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    EdgeKind.RT_HAS_PARAM: [
        DerivationSpec(
            extractor="cpg.build_edges:build_cpg_edges_raw",
            provider_or_field="signature.parameters iteration",
            join_keys=("sig_id",),
            id_recipe="edge_id = sha('RT_HAS_PARAM:'+sig_id+':'+param_id)[:16]",
            confidence_policy="confidence=1.0",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    EdgeKind.RT_HAS_MEMBER: [
        DerivationSpec(
            extractor="cpg.build_edges:build_cpg_edges_raw",
            provider_or_field="inspect.getmembers_static(obj)",
            join_keys=("rt_id",),
            id_recipe="edge_id = sha('RT_HAS_MEMBER:'+rt_id+':'+member_id)[:16]",
            confidence_policy="confidence=0.9",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
    EdgeKind.RT_WRAPS: [
        DerivationSpec(
            extractor="extract.runtime_inspect_extract:extract_runtime_objects",
            provider_or_field="inspect.unwrap chain",
            join_keys=("rt_id",),
            id_recipe="edge_id = sha('RT_WRAPS:'+wrapper_id+':'+wrapped_id)[:16]",
            confidence_policy="confidence=0.9",
            ambiguity_policy="none",
            status="planned",
        )
    ],
    # Diagnostics
    EdgeKind.HAS_DIAGNOSTIC: [
        DerivationSpec(
            extractor="cpg.build_edges:build_cpg_edges_raw",
            provider_or_field="attach DIAG nodes to owning file/module",
            join_keys=("path",),
            id_recipe="edge_id = sha('HAS_DIAG:'+owner_id+':'+diag_id)[:16]",
            confidence_policy="confidence=1.0 if from source tool; else 0.7",
            ambiguity_policy="none",
            status="implemented",
        )
    ],
}


# ----------------------------
# Validation helpers
# ----------------------------


def validate_registry_completeness() -> None:
    """Ensure every enum value has a contract and derivation entry.

    Raises
    ------
    ValueError
        Raised when contracts or derivations are missing.
    """
    missing_node_contracts = [k for k in NodeKind if k not in NODE_KIND_CONTRACTS]
    missing_edge_contracts = [k for k in EdgeKind if k not in EDGE_KIND_CONTRACTS]
    missing_node_derivs = [
        k for k in NodeKind if k not in NODE_DERIVATIONS or not NODE_DERIVATIONS[k]
    ]
    missing_edge_derivs = [
        k for k in EdgeKind if k not in EDGE_DERIVATIONS or not EDGE_DERIVATIONS[k]
    ]

    errs = []
    if missing_node_contracts:
        errs.append(f"Missing NODE_KIND_CONTRACTS for: {[k.value for k in missing_node_contracts]}")
    if missing_edge_contracts:
        errs.append(f"Missing EDGE_KIND_CONTRACTS for: {[k.value for k in missing_edge_contracts]}")
    if missing_node_derivs:
        errs.append(f"Missing NODE_DERIVATIONS for: {[k.value for k in missing_node_derivs]}")
    if missing_edge_derivs:
        errs.append(f"Missing EDGE_DERIVATIONS for: {[k.value for k in missing_edge_derivs]}")

    if errs:
        raise ValueError("Ultimate kind registry incomplete:\n- " + "\n- ".join(errs))


def _has_acceptable_derivation(
    derivations: Sequence[DerivationSpec],
    contract: NodeKindContract | EdgeKindContract,
    *,
    allowed_sources: set[SourceKind] | None,
    allow_planned: bool,
) -> bool:
    if allow_planned:
        return bool(derivations)
    implemented = [d for d in derivations if d.status == "implemented"]
    if not implemented:
        return False
    if allowed_sources is None:
        return True
    return any(src in allowed_sources for src in contract.allowed_sources) or not (
        contract.allowed_sources
    )


def validate_derivations_implemented_only(
    *,
    allowed_sources: Sequence[SourceKind] | None = None,
    allow_planned: bool = False,
) -> None:
    """Run compile-time pipeline compatibility checks.

    - If allow_planned=False: every kind must have at least one derivation with status="implemented"
    - If allowed_sources provided: that implemented derivation must be from an allowed source set
      (based on contract.allowed_sources; derivation itself is source-agnostic metadata).

    Raises
    ------
    ValueError
        Raised when no acceptable derivations are found.
    """
    allowed_sources_set = set(allowed_sources) if allowed_sources else None

    missing_nodes = [
        k.value
        for k in NodeKind
        if not _has_acceptable_derivation(
            NODE_DERIVATIONS.get(k, []),
            NODE_KIND_CONTRACTS[k],
            allowed_sources=allowed_sources_set,
            allow_planned=allow_planned,
        )
    ]
    missing_edges = [
        k.value
        for k in EdgeKind
        if not _has_acceptable_derivation(
            EDGE_DERIVATIONS.get(k, []),
            EDGE_KIND_CONTRACTS[k],
            allowed_sources=allowed_sources_set,
            allow_planned=allow_planned,
        )
    ]

    if missing_nodes or missing_edges:
        msg = []
        if missing_nodes:
            msg.append(
                "No acceptable implemented derivation for node kinds: " + ", ".join(missing_nodes)
            )
        if missing_edges:
            msg.append(
                "No acceptable implemented derivation for edge kinds: " + ", ".join(missing_edges)
            )
        raise ValueError("\n".join(msg))


def _iter_derivation_specs(*, allow_planned: bool) -> Iterator[DerivationSpec]:
    for derivations in NODE_DERIVATIONS.values():
        for spec in derivations:
            if allow_planned or spec.status == "implemented":
                yield spec
    for derivations in EDGE_DERIVATIONS.values():
        for spec in derivations:
            if allow_planned or spec.status == "implemented":
                yield spec


def validate_derivation_extractors(*, allow_planned: bool = False) -> None:
    """Validate that derivation extractors resolve to real callables.

    Parameters
    ----------
    allow_planned:
        When ``True``, validate both planned and implemented derivations.

    Raises
    ------
    ValueError
        Raised when one or more extractors cannot be resolved.
    """
    errors: list[str] = []
    for spec in _iter_derivation_specs(allow_planned=allow_planned):
        try:
            parse_extractor(spec.extractor).resolve()
        except (ModuleNotFoundError, AttributeError, ValueError) as exc:
            errors.append(f"{spec.extractor}: {exc}")

    if errors:
        msg = "Derivation extractor validation failed:\n- " + "\n- ".join(errors)
        raise ValueError(msg)


def registry_to_jsonable() -> dict[str, object]:
    """Return a JSON-serializable view of the registry.

    Useful for run bundles, manifests, and validation reports.

    Returns
    -------
    dict[str, object]
        JSON-serializable registry data.
    """
    return {
        "node_kinds": [k.value for k in NodeKind],
        "edge_kinds": [k.value for k in EdgeKind],
        "node_kind_contracts": {k.value: v.to_dict() for k, v in NODE_KIND_CONTRACTS.items()},
        "edge_kind_contracts": {k.value: v.to_dict() for k, v in EDGE_KIND_CONTRACTS.items()},
        "node_derivations": {
            k.value: [d.to_dict() for d in v] for k, v in NODE_DERIVATIONS.items()
        },
        "edge_derivations": {
            k.value: [d.to_dict() for d in v] for k, v in EDGE_DERIVATIONS.items()
        },
    }


# Run completeness checks at import time (fail-fast).
validate_registry_completeness()
