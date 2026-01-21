"""Ultimate Python CPG kind registry.

This module is intended to be the *single source of truth* for:
  - node_kind / edge_kind enumerations (Ultimate Python CPG)
  - required properties & their types per kind (contracts)
  - derivation manifest per kind (extractor/provider/join/id/confidence/ambiguity)

Design goals:
  - Machine consumable: structured dataclasses + constant dict registries
  - Strict: every enum value must have a contract and >=1 derivation entry
  - Practical: supports "implemented" vs "planned" derivations
  - Portable: stdlib-only registry components (ArrowDSL exports live elsewhere)

Your Hamilton integration and relationship compiler can validate against this at build time:
  - validate_registry_completeness()
  - validate_derivations_implemented_only(allowed_sources=..., allow_planned=False)
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence

from cpg.kinds_registry_builders import (
    EdgeContractRow,
    EdgeContractTemplate,
    NodeContractRow,
    NodeContractTemplate,
    build_edge_contracts,
    build_node_contracts,
)
from cpg.kinds_registry_derivations import (
    DerivationRow,
    DerivationTemplate,
    build_derivations,
)
from cpg.kinds_registry_enums import (
    SCIP_ROLE_DEFINITION,
    SCIP_ROLE_FORWARD_DEFINITION,
    SCIP_ROLE_GENERATED,
    SCIP_ROLE_IMPORT,
    SCIP_ROLE_READ,
    SCIP_ROLE_TEST,
    SCIP_ROLE_WRITE,
    EdgeKind,
    EntityKind,
    NodeKind,
    SourceKind,
)
from cpg.kinds_registry_ids import SPAN_CONTAINED_BEST, sha_id, stable_span_id
from cpg.kinds_registry_models import (
    CallableRef,
    DerivationSpec,
    DerivationStatus,
    EdgeKindContract,
    NodeKindContract,
    parse_extractor,
)
from cpg.kinds_registry_props import (
    PROP_ENUMS,
    PropPrimitive,
    PropSpec,
    p_bool,
    p_float,
    p_int,
    p_json,
    p_str,
)

# ----------------------------
# Contract templates
# ----------------------------

NODE_T_LIBCST_ANCHOR = NodeContractTemplate(
    requires_anchor=True,
    allowed_sources=(SourceKind.LIBCST,),
)
NODE_T_PY_AST = NodeContractTemplate(
    requires_anchor=False,
    allowed_sources=(SourceKind.PY_AST,),
)
NODE_T_TREESITTER_ANCHOR = NodeContractTemplate(
    requires_anchor=True,
    allowed_sources=(SourceKind.TREESITTER,),
)
NODE_T_SYMTABLE = NodeContractTemplate(
    requires_anchor=False,
    allowed_sources=(SourceKind.SYMTABLE,),
)
NODE_T_INSPECT = NodeContractTemplate(
    requires_anchor=False,
    allowed_sources=(SourceKind.INSPECT,),
)
NODE_T_PIPELINE = NodeContractTemplate(
    requires_anchor=False,
    allowed_sources=(SourceKind.PIPELINE,),
)
NODE_T_PIPELINE_DERIVED = NodeContractTemplate(
    requires_anchor=False,
    allowed_sources=(SourceKind.PIPELINE, SourceKind.DERIVED),
)
NODE_T_LIBCST_DERIVED = NodeContractTemplate(
    requires_anchor=False,
    allowed_sources=(SourceKind.LIBCST, SourceKind.DERIVED),
)
NODE_T_DERIVED = NodeContractTemplate(
    requires_anchor=False,
    allowed_sources=(SourceKind.DERIVED,),
)
NODE_T_DERIVED_ANCHOR = NodeContractTemplate(
    requires_anchor=True,
    allowed_sources=(SourceKind.DERIVED,),
)
NODE_T_DERIVED_SYMTABLE = NodeContractTemplate(
    requires_anchor=False,
    allowed_sources=(SourceKind.DERIVED, SourceKind.SYMTABLE),
)
NODE_T_DERIVED_SYMTABLE_LIBCST = NodeContractTemplate(
    requires_anchor=False,
    allowed_sources=(SourceKind.DERIVED, SourceKind.SYMTABLE, SourceKind.LIBCST),
)
NODE_T_DIS = NodeContractTemplate(
    requires_anchor=False,
    allowed_sources=(SourceKind.DIS,),
)
NODE_T_DERIVED_DIS = NodeContractTemplate(
    requires_anchor=False,
    allowed_sources=(SourceKind.DERIVED, SourceKind.DIS),
)
NODE_T_SCIP = NodeContractTemplate(
    requires_anchor=False,
    allowed_sources=(SourceKind.SCIP,),
)
NODE_T_SCIP_ANCHOR = NodeContractTemplate(
    requires_anchor=True,
    allowed_sources=(SourceKind.SCIP,),
)
NODE_T_SCIP_DERIVED_ANCHOR = NodeContractTemplate(
    requires_anchor=True,
    allowed_sources=(SourceKind.SCIP, SourceKind.DERIVED),
)
NODE_T_LIBCST_PYAST_ANCHOR = NodeContractTemplate(
    requires_anchor=True,
    allowed_sources=(SourceKind.LIBCST, SourceKind.PY_AST),
)
NODE_T_TYPE = NodeContractTemplate(
    requires_anchor=False,
    allowed_sources=(SourceKind.DERIVED, SourceKind.LIBCST, SourceKind.INSPECT),
)
NODE_T_DIAG = NodeContractTemplate(
    requires_anchor=True,
    allowed_sources=(
        SourceKind.TREESITTER,
        SourceKind.SCIP,
        SourceKind.PIPELINE,
        SourceKind.DERIVED,
    ),
)

EDGE_T_PIPELINE = EdgeContractTemplate(
    requires_evidence_anchor=False,
    allowed_sources=(SourceKind.PIPELINE,),
)
EDGE_T_PIPELINE_DERIVED = EdgeContractTemplate(
    requires_evidence_anchor=False,
    allowed_sources=(SourceKind.PIPELINE, SourceKind.DERIVED),
)
EDGE_T_LIBCST = EdgeContractTemplate(
    requires_evidence_anchor=False,
    allowed_sources=(SourceKind.LIBCST,),
)
EDGE_T_PY_AST = EdgeContractTemplate(
    requires_evidence_anchor=False,
    allowed_sources=(SourceKind.PY_AST,),
)
EDGE_T_TREESITTER = EdgeContractTemplate(
    requires_evidence_anchor=False,
    allowed_sources=(SourceKind.TREESITTER,),
)
EDGE_T_SYMTABLE_DERIVED = EdgeContractTemplate(
    requires_evidence_anchor=False,
    allowed_sources=(SourceKind.SYMTABLE, SourceKind.DERIVED),
)
EDGE_T_DERIVED = EdgeContractTemplate(
    requires_evidence_anchor=False,
    allowed_sources=(SourceKind.DERIVED,),
)
EDGE_T_DERIVED_ANCHOR = EdgeContractTemplate(
    requires_evidence_anchor=True,
    allowed_sources=(SourceKind.DERIVED,),
)
EDGE_T_DERIVED_DIS = EdgeContractTemplate(
    requires_evidence_anchor=False,
    allowed_sources=(SourceKind.DERIVED, SourceKind.DIS),
)
EDGE_T_LIBCST_DERIVED = EdgeContractTemplate(
    requires_evidence_anchor=False,
    allowed_sources=(SourceKind.LIBCST, SourceKind.DERIVED),
)
EDGE_T_LIBCST_DERIVED_ANCHOR = EdgeContractTemplate(
    requires_evidence_anchor=True,
    allowed_sources=(SourceKind.LIBCST, SourceKind.DERIVED),
)
EDGE_T_LIBCST_PYAST_DERIVED_ANCHOR = EdgeContractTemplate(
    requires_evidence_anchor=True,
    allowed_sources=(SourceKind.LIBCST, SourceKind.PY_AST, SourceKind.DERIVED),
)
EDGE_T_LIBCST_INSPECT_DERIVED = EdgeContractTemplate(
    requires_evidence_anchor=False,
    allowed_sources=(SourceKind.LIBCST, SourceKind.INSPECT, SourceKind.DERIVED),
)
EDGE_T_SCIP = EdgeContractTemplate(
    requires_evidence_anchor=False,
    allowed_sources=(SourceKind.SCIP,),
)
EDGE_T_SCIP_DERIVED_ANCHOR = EdgeContractTemplate(
    requires_evidence_anchor=True,
    allowed_sources=(SourceKind.SCIP, SourceKind.DERIVED),
)
EDGE_T_TREESITTER_SCIP_DERIVED = EdgeContractTemplate(
    requires_evidence_anchor=False,
    allowed_sources=(SourceKind.TREESITTER, SourceKind.SCIP, SourceKind.DERIVED),
)

# ----------------------------
# Contracts: node and edge kind contracts
# ----------------------------

NODE_CONTRACT_ROWS: tuple[NodeContractRow, ...] = (
    NodeContractRow(
        kind=NodeKind.AST_ATTRIBUTE,
        required=(
            "ast_type",
            "attr",
        ),
        optional=("ctx",),
        template=NODE_T_PY_AST,
        required_overrides=(("ast_type", p_str(enum=PROP_ENUMS["ast_attr_type"])),),
        optional_overrides=(("ctx", p_str(enum=PROP_ENUMS["ctx_basic"])),),
        description="AST Attribute node.",
    ),
    NodeContractRow(
        kind=NodeKind.AST_CALL,
        required=("ast_type",),
        optional=(
            "lineno",
            "end_lineno",
        ),
        template=NODE_T_PY_AST,
        required_overrides=(("ast_type", p_str(enum=PROP_ENUMS["ast_call_type"])),),
        description="AST Call node.",
    ),
    NodeContractRow(
        kind=NodeKind.AST_DEF,
        required=("ast_type",),
        optional=(
            "name",
            "lineno",
            "end_lineno",
        ),
        template=NODE_T_PY_AST,
        required_overrides=(("ast_type", p_str(enum=PROP_ENUMS["ast_def_type"])),),
        description="AST definition node (def/class/lambda).",
    ),
    NodeContractRow(
        kind=NodeKind.AST_NAME,
        required=(
            "ast_type",
            "id",
        ),
        optional=("ctx",),
        template=NODE_T_PY_AST,
        required_overrides=(("ast_type", p_str(enum=PROP_ENUMS["ast_name_type"])),),
        optional_overrides=(("ctx", p_str(enum=PROP_ENUMS["ctx_full"])),),
        description="AST Name node.",
    ),
    NodeContractRow(
        kind=NodeKind.AST_NODE,
        required=("ast_type",),
        optional=(
            "lineno",
            "col_offset",
            "end_lineno",
            "end_col_offset",
        ),
        template=NODE_T_PY_AST,
        description="Generic python ast node (abstract syntax lens).",
    ),
    NodeContractRow(
        kind=NodeKind.BC_CODE_UNIT,
        required=(
            "code_unit_id",
            "path",
            "qualname",
        ),
        optional=(
            "co_firstlineno",
            "flags_json",
        ),
        template=NODE_T_DIS,
        description="Code object / code unit (function/lambda/comprehension) node.",
    ),
    NodeContractRow(
        kind=NodeKind.BC_INSTR,
        required=(
            "instr_id",
            "code_unit_id",
            "offset",
            "baseopname",
        ),
        optional=(
            "opname",
            "argrepr",
            "starts_line",
        ),
        template=NODE_T_DIS,
        description="Bytecode instruction node.",
    ),
    NodeContractRow(
        kind=NodeKind.CFG_BLOCK,
        required=(
            "block_id",
            "code_unit_id",
            "start_off",
            "end_off",
        ),
        optional=(
            "is_entry",
            "is_exit",
        ),
        template=NODE_T_DERIVED_DIS,
        description="CFG basic block node.",
    ),
    NodeContractRow(
        kind=NodeKind.CFG_EXIT,
        required=("code_unit_id",),
        optional=(),
        template=NODE_T_DERIVED,
        description="Synthetic CFG exit node (optional).",
    ),
    NodeContractRow(
        kind=NodeKind.CST_ARG,
        required=(),
        optional=(
            "is_star",
            "is_kw_star",
        ),
        template=NODE_T_LIBCST_ANCHOR,
        description="LibCST call argument node.",
    ),
    NodeContractRow(
        kind=NodeKind.CST_ATTRIBUTE_ACCESS,
        required=("attr",),
        optional=("base_shape",),
        template=NODE_T_LIBCST_ANCHOR,
        optional_overrides=(("base_shape", p_str(enum=PROP_ENUMS["base_shape"])),),
        description="LibCST Attribute access occurrence.",
    ),
    NodeContractRow(
        kind=NodeKind.CST_CALLSITE,
        required=(
            "callee_shape",
            "arg_count",
        ),
        optional=(
            "callee_dotted",
            "callee_text",
            "callee_qnames",
        ),
        template=NODE_T_LIBCST_ANCHOR,
        required_overrides=(("callee_shape", p_str(enum=PROP_ENUMS["callee_shape"])),),
        description="LibCST Call expression occurrence.",
    ),
    NodeContractRow(
        kind=NodeKind.CST_CLASS_DEF,
        required=("name",),
        optional=("base_exprs_json",),
        template=NODE_T_LIBCST_ANCHOR,
        description="LibCST ClassDef node.",
    ),
    NodeContractRow(
        kind=NodeKind.CST_COMMENT,
        required=("text",),
        optional=(),
        template=NODE_T_LIBCST_ANCHOR,
        description="Comment node (trivia).",
    ),
    NodeContractRow(
        kind=NodeKind.CST_DECORATOR,
        required=(),
        optional=("decorator_text",),
        template=NODE_T_LIBCST_ANCHOR,
        description="LibCST decorator node.",
    ),
    NodeContractRow(
        kind=NodeKind.CST_DEF,
        required=(
            "name",
            "def_kind",
        ),
        optional=(
            "is_async",
            "decorator_count",
            "docstring",
            "container_def_id",
            "qnames",
        ),
        template=NODE_T_LIBCST_ANCHOR,
        required_overrides=(("def_kind", p_str(enum=PROP_ENUMS["def_kind"])),),
        description="High-signal definition node (union of class/function/lambda).",
    ),
    NodeContractRow(
        kind=NodeKind.CST_DOCSTRING,
        required=("docstring",),
        optional=("is_module_docstring",),
        template=NODE_T_LIBCST_ANCHOR,
        description="Docstring node (module/class/function).",
    ),
    NodeContractRow(
        kind=NodeKind.CST_FUNCTION_DEF,
        required=("name",),
        optional=(
            "is_async",
            "returns_annotation",
        ),
        template=NODE_T_LIBCST_ANCHOR,
        description="LibCST FunctionDef node.",
    ),
    NodeContractRow(
        kind=NodeKind.CST_IMPORT,
        required=(),
        optional=("import_text",),
        template=NODE_T_LIBCST_ANCHOR,
        description="LibCST Import node.",
    ),
    NodeContractRow(
        kind=NodeKind.CST_IMPORT_ALIAS,
        required=("imported_name",),
        optional=(
            "asname",
            "is_star",
            "import_kind",
            "module",
            "relative_level",
            "name",
        ),
        template=NODE_T_LIBCST_ANCHOR,
        description="LibCST ImportAlias node.",
    ),
    NodeContractRow(
        kind=NodeKind.CST_IMPORT_FROM,
        required=(),
        optional=(
            "module",
            "relative_level",
        ),
        template=NODE_T_LIBCST_ANCHOR,
        description="LibCST ImportFrom node.",
    ),
    NodeContractRow(
        kind=NodeKind.CST_LAMBDA,
        required=(),
        optional=("param_count",),
        template=NODE_T_LIBCST_ANCHOR,
        description="LibCST Lambda node.",
    ),
    NodeContractRow(
        kind=NodeKind.CST_LITERAL,
        required=("literal_kind",),
        optional=("literal_text",),
        template=NODE_T_LIBCST_ANCHOR,
        required_overrides=(("literal_kind", p_str(enum=PROP_ENUMS["literal_kind"])),),
        description="Literal node (CST-level).",
    ),
    NodeContractRow(
        kind=NodeKind.CST_REF,
        required=(
            "ref_text",
            "ref_kind",
        ),
        optional=(
            "expr_context",
            "scope_type",
            "scope_name",
            "scope_role",
            "parent_kind",
            "inferred_type",
        ),
        template=NODE_T_LIBCST_ANCHOR,
        optional_overrides=(("expr_context", p_str(enum=PROP_ENUMS["expr_context"])),),
        description="LibCST reference occurrence.",
    ),
    NodeContractRow(
        kind=NodeKind.CST_NODE,
        required=("cst_type",),
        optional=(
            "line_start",
            "col_start",
            "line_end",
            "col_end",
            "module_fqn",
        ),
        template=NODE_T_LIBCST_ANCHOR,
        description="Generic LibCST node (lossless concrete syntax lens).",
    ),
    NodeContractRow(
        kind=NodeKind.CST_PARAM,
        required=("name",),
        optional=(
            "kind",
            "has_default",
        ),
        template=NODE_T_LIBCST_ANCHOR,
        optional_overrides=(("kind", p_str(enum=PROP_ENUMS["param_kind"])),),
        description="LibCST parameter node.",
    ),
    NodeContractRow(
        kind=NodeKind.CST_SUBSCRIPT,
        required=(),
        optional=("subscript_text",),
        template=NODE_T_LIBCST_ANCHOR,
        description="LibCST Subscript occurrence.",
    ),
    NodeContractRow(
        kind=NodeKind.DF_DEF,
        required=(
            "df_id",
            "binding_id",
            "instr_id",
            "code_unit_id",
        ),
        optional=("def_kind",),
        template=NODE_T_DERIVED,
        description="Dataflow definition event node (optional separate node).",
    ),
    NodeContractRow(
        kind=NodeKind.DF_USE,
        required=(
            "df_id",
            "binding_id",
            "instr_id",
            "code_unit_id",
        ),
        optional=("use_kind",),
        template=NODE_T_DERIVED,
        description="Dataflow use event node (optional separate node).",
    ),
    NodeContractRow(
        kind=NodeKind.DIAG,
        required=(
            "severity",
            "message",
            "diag_source",
        ),
        optional=(
            "code",
            "details",
        ),
        template=NODE_T_DIAG,
        required_overrides=(("severity", p_str(enum=PROP_ENUMS["severity"])),),
        description="Generic diagnostic node (any stage/source).",
    ),
    NodeContractRow(
        kind=NodeKind.PY_BINDING,
        required=(
            "binding_id",
            "scope_id",
            "name",
            "binding_kind",
        ),
        optional=(
            "declared_here",
            "referenced_here",
            "assigned_here",
            "annotated_here",
        ),
        template=NODE_T_DERIVED_SYMTABLE,
        required_overrides=(("binding_kind", p_str(enum=PROP_ENUMS["binding_kind"])),),
        description="Normalized binding slot (scope,name) with classification flags.",
    ),
    NodeContractRow(
        kind=NodeKind.PY_DEF_SITE,
        required=(
            "binding_id",
            "name",
        ),
        optional=(
            "def_site_kind",
            "anchor_confidence",
            "anchor_reason",
            "ambiguity_group_id",
        ),
        template=NODE_T_DERIVED_ANCHOR,
        optional_overrides=(("def_site_kind", p_str(enum=PROP_ENUMS["def_site_kind"])),),
        description="Anchored definition site (name span) mapped to a binding.",
    ),
    NodeContractRow(
        kind=NodeKind.PY_FILE,
        required=("path",),
        optional=(
            "size_bytes",
            "file_sha256",
            "mtime_ns",
            "encoding",
        ),
        template=NODE_T_PIPELINE,
        description="File entity for a repo-tracked Python file.",
    ),
    NodeContractRow(
        kind=NodeKind.PY_MODULE,
        required=(
            "path",
            "module_fqn",
        ),
        optional=("package_root",),
        template=NODE_T_LIBCST_DERIVED,
        description="Module identity for a file (module_fqn).",
    ),
    NodeContractRow(
        kind=NodeKind.PY_MODULE_FQN,
        required=("module_fqn",),
        optional=(),
        template=NODE_T_LIBCST_DERIVED,
        description="Module fully qualified name node (optional).",
    ),
    NodeContractRow(
        kind=NodeKind.PY_PACKAGE,
        required=("package_fqn",),
        optional=("path",),
        template=NODE_T_PIPELINE_DERIVED,
        description="Package identity node (optional).",
    ),
    NodeContractRow(
        kind=NodeKind.PY_QUALIFIED_NAME,
        required=("qname",),
        optional=("qname_source",),
        template=NODE_T_LIBCST_DERIVED,
        optional_overrides=(("qname_source", p_str(enum=PROP_ENUMS["qname_source"])),),
        description="Qualified name candidate/canonical node.",
    ),
    NodeContractRow(
        kind=NodeKind.PY_REPO,
        required=(
            "repo_root",
            "repo_id",
        ),
        optional=("git_commit",),
        template=NODE_T_PIPELINE,
        description="Repository root entity.",
    ),
    NodeContractRow(
        kind=NodeKind.PY_SCOPE,
        required=(
            "scope_id",
            "scope_type",
        ),
        optional=(
            "path",
            "module_fqn",
        ),
        template=NODE_T_DERIVED_SYMTABLE_LIBCST,
        description="Normalized scope node (unifies symtable + libcst scope concepts).",
    ),
    NodeContractRow(
        kind=NodeKind.PY_USE_SITE,
        required=(
            "binding_id",
            "name",
        ),
        optional=(
            "use_kind",
            "anchor_confidence",
            "anchor_reason",
            "ambiguity_group_id",
        ),
        template=NODE_T_DERIVED_ANCHOR,
        optional_overrides=(("use_kind", p_str(enum=PROP_ENUMS["use_kind"])),),
        description="Anchored use site (name span) mapped to a binding.",
    ),
    NodeContractRow(
        kind=NodeKind.RT_MEMBER,
        required=(
            "member_id",
            "name",
            "member_kind",
        ),
        optional=(
            "declaring_type",
            "value_repr",
            "value_module",
            "value_qualname",
        ),
        template=NODE_T_INSPECT,
        description="Runtime member node (getmembers_static row).",
    ),
    NodeContractRow(
        kind=NodeKind.RT_OBJECT,
        required=(
            "rt_id",
            "qualname",
            "module",
        ),
        optional=(
            "object_type",
            "file",
            "line",
            "name",
            "obj_type",
            "source_path",
            "source_line",
        ),
        template=NODE_T_INSPECT,
        description="Runtime object node (optional overlay).",
    ),
    NodeContractRow(
        kind=NodeKind.RT_SIGNATURE,
        required=(
            "sig_id",
            "signature",
        ),
        optional=("return_annotation",),
        template=NODE_T_INSPECT,
        description="Runtime signature node.",
    ),
    NodeContractRow(
        kind=NodeKind.RT_SIGNATURE_PARAM,
        required=(
            "param_id",
            "name",
            "kind",
        ),
        optional=(
            "default_json",
            "annotation_json",
            "default_repr",
            "annotation_repr",
            "position",
        ),
        template=NODE_T_INSPECT,
        description="Runtime signature parameter node.",
    ),
    NodeContractRow(
        kind=NodeKind.SCIP_DIAGNOSTIC,
        required=(
            "severity",
            "message",
        ),
        optional=(
            "code",
            "source",
        ),
        template=NODE_T_SCIP_ANCHOR,
        required_overrides=(("severity", p_str(enum=PROP_ENUMS["severity"])),),
        description="SCIP diagnostic node (if present).",
    ),
    NodeContractRow(
        kind=NodeKind.SCIP_DOCUMENT,
        required=("path",),
        optional=(
            "language",
            "diagnostic_count",
        ),
        template=NODE_T_SCIP,
        description="SCIP document node (per file).",
    ),
    NodeContractRow(
        kind=NodeKind.SCIP_INDEX,
        required=(
            "index_format",
            "tool",
        ),
        optional=(
            "version",
            "project_root",
        ),
        template=NODE_T_SCIP,
        required_overrides=(("index_format", p_str(enum=PROP_ENUMS["index_format"])),),
        description="SCIP index root node (metadata).",
    ),
    NodeContractRow(
        kind=NodeKind.SCIP_OCCURRENCE,
        required=(
            "symbol",
            "symbol_roles",
        ),
        optional=("occurrence_role_names_json",),
        template=NODE_T_SCIP_DERIVED_ANCHOR,
        description="SCIP occurrence node (symbol + roles + range), normalized to byte span.",
    ),
    NodeContractRow(
        kind=NodeKind.SCIP_SYMBOL,
        required=("symbol",),
        optional=(
            "display_name",
            "symbol_kind",
            "enclosing_symbol",
            "documentation",
            "signature_text",
            "signature_language",
        ),
        template=NODE_T_SCIP,
        description="SCIP symbol node.",
    ),
    NodeContractRow(
        kind=NodeKind.SYM_SCOPE,
        required=(
            "scope_id",
            "scope_type",
            "scope_name",
        ),
        optional=(
            "function_partitions",
            "class_methods",
            "lineno",
            "is_meta_scope",
            "path",
        ),
        template=NODE_T_SYMTABLE,
        description="Symtable scope node (compiler truth for lexical scopes).",
    ),
    NodeContractRow(
        kind=NodeKind.SYM_SYMBOL,
        required=(
            "scope_id",
            "name",
        ),
        optional=(
            "namespace_count",
            "namespace_block_ids",
            "is_local",
            "is_global",
            "is_nonlocal",
            "is_free",
            "is_parameter",
            "is_imported",
            "is_assigned",
            "is_referenced",
            "is_annotated",
            "is_namespace",
        ),
        template=NODE_T_SYMTABLE,
        description="Symtable symbol record within a scope.",
    ),
    NodeContractRow(
        kind=NodeKind.TS_ERROR,
        required=(
            "ts_type",
            "is_error",
        ),
        optional=("has_error",),
        template=NODE_T_TREESITTER_ANCHOR,
        description="tree-sitter error node.",
    ),
    NodeContractRow(
        kind=NodeKind.TS_MISSING,
        required=(
            "ts_type",
            "is_missing",
        ),
        optional=(),
        template=NODE_T_TREESITTER_ANCHOR,
        description="tree-sitter missing-token node (recovery inserted).",
    ),
    NodeContractRow(
        kind=NodeKind.TS_NODE,
        required=("ts_type",),
        optional=(
            "is_named",
            "has_error",
            "is_error",
            "is_missing",
            "is_extra",
            "has_changes",
            "ts_kind_id",
            "ts_grammar_id",
            "ts_grammar_name",
            "ts_node_uid",
        ),
        template=NODE_T_TREESITTER_ANCHOR,
        description="Generic tree-sitter node.",
    ),
    NodeContractRow(
        kind=NodeKind.TYPE,
        required=(
            "type_repr",
            "type_form",
        ),
        optional=("origin",),
        template=NODE_T_TYPE,
        optional_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_type_node"])),),
        description="Normalized type node (may represent unions/overloads).",
    ),
    NodeContractRow(
        kind=NodeKind.TYPE_ALIAS,
        required=(
            "name",
            "target_type_repr",
        ),
        optional=(),
        template=NODE_T_DERIVED_SYMTABLE,
        description="Type alias node (typing scopes).",
    ),
    NodeContractRow(
        kind=NodeKind.TYPE_EXPR,
        required=("expr_text",),
        optional=(
            "expr_kind",
            "expr_role",
            "param_name",
        ),
        template=NODE_T_LIBCST_PYAST_ANCHOR,
        optional_overrides=(("expr_kind", p_str(enum=PROP_ENUMS["expr_kind"])),),
        description="Annotation/type expression node anchored in source.",
    ),
    NodeContractRow(
        kind=NodeKind.TYPE_PARAM,
        required=("name",),
        optional=("variance",),
        template=NODE_T_DERIVED_SYMTABLE,
        optional_overrides=(("variance", p_str(enum=PROP_ENUMS["variance"])),),
        description="Type parameter node.",
    ),
)

NODE_KIND_CONTRACTS: dict[NodeKind, NodeKindContract] = build_node_contracts(NODE_CONTRACT_ROWS)

EDGE_CONTRACT_ROWS: tuple[EdgeContractRow, ...] = (
    EdgeContractRow(
        kind=EdgeKind.AST_PARENT_OF,
        required=(),
        optional=(
            "field",
            "index",
        ),
        template=EDGE_T_PY_AST,
        description="AST structural parent edge.",
    ),
    EdgeContractRow(
        kind=EdgeKind.ATTR_CANDIDATE_QNAME,
        required=(
            "qname_source",
            "score",
        ),
        optional=("ambiguity_group_id",),
        template=EDGE_T_LIBCST_DERIVED_ANCHOR,
        description="Attribute access -> qualified name candidate edge (multi-valued).",
    ),
    EdgeContractRow(
        kind=EdgeKind.BINDING_RESOLVES_TO,
        required=("kind",),
        optional=("reason",),
        requires_evidence_anchor=False,
        allowed_sources=(
            SourceKind.DERIVED,
            SourceKind.SYMTABLE,
        ),
        required_overrides=(("kind", p_str(enum=PROP_ENUMS["resolution_kind"])),),
        description="Binding resolves to outer binding (closure/global/nonlocal).",
    ),
    EdgeContractRow(
        kind=EdgeKind.BYTECODE_ANCHOR,
        required=(
            "mapping_method",
            "score",
        ),
        optional=(
            "anchor_kind",
            "notes",
        ),
        template=EDGE_T_DERIVED_DIS,
        required_overrides=(("mapping_method", p_str(enum=PROP_ENUMS["mapping_method"])),),
        description="Bytecode instruction anchored to a source span node.",
    ),
    EdgeContractRow(
        kind=EdgeKind.CALLSITE_CANDIDATE_QNAME,
        required=(
            "qname_source",
            "score",
        ),
        optional=("ambiguity_group_id",),
        template=EDGE_T_LIBCST_DERIVED_ANCHOR,
        description="Callsite -> qualified name candidate edge (multi-valued).",
    ),
    EdgeContractRow(
        kind=EdgeKind.CFG_BRANCH,
        required=("code_unit_id",),
        optional=("cond_instr_id",),
        template=EDGE_T_DERIVED,
        description="CFG conditional branch (polarity unknown or merged).",
    ),
    EdgeContractRow(
        kind=EdgeKind.CFG_BRANCH_FALSE,
        required=("code_unit_id",),
        optional=("cond_instr_id",),
        template=EDGE_T_DERIVED,
        description="CFG conditional false branch.",
    ),
    EdgeContractRow(
        kind=EdgeKind.CFG_BRANCH_TRUE,
        required=("code_unit_id",),
        optional=("cond_instr_id",),
        template=EDGE_T_DERIVED,
        description="CFG conditional true branch.",
    ),
    EdgeContractRow(
        kind=EdgeKind.CFG_EXC,
        required=(
            "code_unit_id",
            "exc_entry_index",
        ),
        optional=(
            "depth",
            "lasti",
        ),
        template=EDGE_T_DERIVED,
        description="CFG exception edge derived from exception table.",
    ),
    EdgeContractRow(
        kind=EdgeKind.CFG_JUMP,
        required=("code_unit_id",),
        optional=("jump_off",),
        template=EDGE_T_DERIVED,
        description="CFG unconditional jump edge.",
    ),
    EdgeContractRow(
        kind=EdgeKind.CFG_NEXT,
        required=("code_unit_id",),
        optional=(),
        template=EDGE_T_DERIVED,
        description="CFG fallthrough edge.",
    ),
    EdgeContractRow(
        kind=EdgeKind.CST_PARENT_OF,
        required=(),
        optional=(
            "field",
            "index",
        ),
        template=EDGE_T_LIBCST,
        description="LibCST structural parent edge.",
    ),
    EdgeContractRow(
        kind=EdgeKind.DEF_SITE_OF,
        required=("def_site_kind",),
        optional=(),
        template=EDGE_T_DERIVED_ANCHOR,
        required_overrides=(("def_site_kind", p_str(enum=PROP_ENUMS["def_site_kind"])),),
        description="Definition site establishes a binding (anchored evidence).",
    ),
    EdgeContractRow(
        kind=EdgeKind.FILE_CONTAINS,
        required=("role",),
        optional=(),
        template=EDGE_T_PIPELINE_DERIVED,
        required_overrides=(("role", p_str(enum=PROP_ENUMS["role"])),),
        description="File contains an anchored node (typed by role).",
    ),
    EdgeContractRow(
        kind=EdgeKind.FILE_DECLARES_MODULE,
        required=("module_fqn",),
        optional=(),
        requires_evidence_anchor=False,
        allowed_sources=(
            SourceKind.DERIVED,
            SourceKind.LIBCST,
        ),
        description="File declares module edge.",
    ),
    EdgeContractRow(
        kind=EdgeKind.HAS_ANNOTATION,
        required=("origin",),
        optional=(),
        template=EDGE_T_LIBCST_PYAST_DERIVED_ANCHOR,
        required_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_type_expr"])),),
        description="Definition/parameter has a type expression annotation.",
    ),
    EdgeContractRow(
        kind=EdgeKind.HAS_DIAGNOSTIC,
        required=("diag_source",),
        optional=("severity",),
        template=EDGE_T_TREESITTER_SCIP_DERIVED,
        description="Attach diagnostic node to file/module/other entity.",
    ),
    EdgeContractRow(
        kind=EdgeKind.INFERRED_TYPE,
        required=(
            "origin",
            "score",
        ),
        optional=("engine",),
        template=EDGE_T_LIBCST_INSPECT_DERIVED,
        required_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_type"])),),
        description="Node inferred to have a type.",
    ),
    EdgeContractRow(
        kind=EdgeKind.NAME_REF_CANDIDATE_QNAME,
        required=(
            "qname_source",
            "score",
        ),
        optional=("ambiguity_group_id",),
        template=EDGE_T_LIBCST_DERIVED_ANCHOR,
        description="Name ref -> qualified name candidate edge (multi-valued).",
    ),
    EdgeContractRow(
        kind=EdgeKind.PY_CALLS_QNAME,
        required=(
            "origin",
            "qname_source",
            "score",
        ),
        optional=("ambiguity_group_id",),
        template=EDGE_T_LIBCST_DERIVED_ANCHOR,
        required_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_qname"])),),
        description="Callsite resolved to qualified name candidate (fallback).",
    ),
    EdgeContractRow(
        kind=EdgeKind.PY_CALLS_SYMBOL,
        required=(
            "origin",
            "resolution_method",
            "score",
        ),
        optional=(
            "symbol_roles",
            "rule_priority",
        ),
        template=EDGE_T_SCIP_DERIVED_ANCHOR,
        required_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_scip"])),),
        description="Callsite resolved to SCIP symbol (preferred).",
    ),
    EdgeContractRow(
        kind=EdgeKind.PY_DEFINES_SYMBOL,
        required=(
            "symbol_roles",
            "origin",
            "resolution_method",
        ),
        optional=(
            "score",
            "rule_priority",
            "rule_name",
        ),
        template=EDGE_T_SCIP_DERIVED_ANCHOR,
        required_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_scip"])),),
        description="Definition occurrence defines SCIP symbol.",
    ),
    EdgeContractRow(
        kind=EdgeKind.PY_IMPORTS_SYMBOL,
        required=(
            "symbol_roles",
            "origin",
            "resolution_method",
        ),
        optional=(
            "score",
            "rule_priority",
            "rule_name",
        ),
        template=EDGE_T_SCIP_DERIVED_ANCHOR,
        required_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_scip"])),),
        description="Import occurrence imports SCIP symbol.",
    ),
    EdgeContractRow(
        kind=EdgeKind.PY_READS_SYMBOL,
        required=(
            "symbol_roles",
            "origin",
            "resolution_method",
        ),
        optional=(),
        template=EDGE_T_SCIP_DERIVED_ANCHOR,
        required_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_scip"])),),
        description="Occurrence reads SCIP symbol (role bit).",
    ),
    EdgeContractRow(
        kind=EdgeKind.PY_REFERENCES_SYMBOL,
        required=(
            "symbol_roles",
            "origin",
            "resolution_method",
        ),
        optional=(
            "score",
            "rule_priority",
            "rule_name",
        ),
        template=EDGE_T_SCIP_DERIVED_ANCHOR,
        required_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_scip"])),),
        description="Reference occurrence references SCIP symbol.",
    ),
    EdgeContractRow(
        kind=EdgeKind.PY_WRITES_SYMBOL,
        required=(
            "symbol_roles",
            "origin",
            "resolution_method",
        ),
        optional=(),
        template=EDGE_T_SCIP_DERIVED_ANCHOR,
        required_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_scip"])),),
        description="Occurrence writes SCIP symbol (role bit).",
    ),
    EdgeContractRow(
        kind=EdgeKind.REACHES,
        required=("code_unit_id",),
        optional=("path_length",),
        template=EDGE_T_DERIVED,
        description="Dataflow reaching-definitions edge: def reaches use.",
    ),
    EdgeContractRow(
        kind=EdgeKind.REPO_CONTAINS,
        required=(),
        optional=("role",),
        template=EDGE_T_PIPELINE,
        description="Repository contains file edge.",
    ),
    EdgeContractRow(
        kind=EdgeKind.RT_HAS_MEMBER,
        required=(),
        optional=("is_descriptor",),
        requires_evidence_anchor=False,
        allowed_sources=(SourceKind.INSPECT,),
        description="Runtime object has member.",
    ),
    EdgeContractRow(
        kind=EdgeKind.RT_HAS_PARAM,
        required=(),
        optional=("position",),
        requires_evidence_anchor=False,
        allowed_sources=(SourceKind.INSPECT,),
        description="Signature has parameter.",
    ),
    EdgeContractRow(
        kind=EdgeKind.RT_HAS_SIGNATURE,
        required=(),
        optional=(),
        requires_evidence_anchor=False,
        allowed_sources=(SourceKind.INSPECT,),
        description="Runtime object has a signature.",
    ),
    EdgeContractRow(
        kind=EdgeKind.RT_WRAPS,
        required=(),
        optional=("unwrap_depth",),
        requires_evidence_anchor=False,
        allowed_sources=(SourceKind.INSPECT,),
        description="Runtime object wraps another object (unwrap chain).",
    ),
    EdgeContractRow(
        kind=EdgeKind.SCIP_SYMBOL_DEFINITION,
        required=(
            "origin",
            "resolution_method",
        ),
        optional=(
            "score",
            "rule_priority",
            "rule_name",
        ),
        template=EDGE_T_SCIP,
        required_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_scip"])),),
        description="SCIP symbol relationship: definition.",
    ),
    EdgeContractRow(
        kind=EdgeKind.SCIP_SYMBOL_IMPLEMENTATION,
        required=(
            "origin",
            "resolution_method",
        ),
        optional=(
            "score",
            "rule_priority",
            "rule_name",
        ),
        template=EDGE_T_SCIP,
        required_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_scip"])),),
        description="SCIP symbol relationship: implementation.",
    ),
    EdgeContractRow(
        kind=EdgeKind.SCIP_SYMBOL_REFERENCE,
        required=(
            "origin",
            "resolution_method",
        ),
        optional=(
            "score",
            "rule_priority",
            "rule_name",
        ),
        template=EDGE_T_SCIP,
        required_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_scip"])),),
        description="SCIP symbol relationship: reference.",
    ),
    EdgeContractRow(
        kind=EdgeKind.SCIP_SYMBOL_TYPE_DEFINITION,
        required=(
            "origin",
            "resolution_method",
        ),
        optional=(
            "score",
            "rule_priority",
            "rule_name",
        ),
        template=EDGE_T_SCIP,
        required_overrides=(("origin", p_str(enum=PROP_ENUMS["origin_scip"])),),
        description="SCIP symbol relationship: type definition.",
    ),
    EdgeContractRow(
        kind=EdgeKind.SCOPE_BINDS,
        required=(),
        optional=("binding_kind",),
        template=EDGE_T_SYMTABLE_DERIVED,
        description="Scope binds a binding slot.",
    ),
    EdgeContractRow(
        kind=EdgeKind.SCOPE_PARENT,
        required=(),
        optional=(),
        template=EDGE_T_SYMTABLE_DERIVED,
        description="Scope parent relationship.",
    ),
    EdgeContractRow(
        kind=EdgeKind.STEP_DEF,
        required=(
            "code_unit_id",
            "instr_id",
            "binding_id",
        ),
        optional=("def_kind",),
        template=EDGE_T_DERIVED,
        description="Instruction defines a binding (def-use event).",
    ),
    EdgeContractRow(
        kind=EdgeKind.STEP_USE,
        required=(
            "code_unit_id",
            "instr_id",
            "binding_id",
        ),
        optional=("use_kind",),
        template=EDGE_T_DERIVED,
        description="Instruction uses a binding (def-use event).",
    ),
    EdgeContractRow(
        kind=EdgeKind.TS_PARENT_OF,
        required=(),
        optional=(
            "field",
            "index",
        ),
        template=EDGE_T_TREESITTER,
        description="tree-sitter structural parent edge.",
    ),
    EdgeContractRow(
        kind=EdgeKind.TYPE_PARAM_OF,
        required=(),
        optional=("scope_type",),
        requires_evidence_anchor=False,
        allowed_sources=(
            SourceKind.DERIVED,
            SourceKind.SYMTABLE,
        ),
        description="Type parameter belongs to a def/class/type.",
    ),
    EdgeContractRow(
        kind=EdgeKind.USE_SITE_OF,
        required=("use_kind",),
        optional=(),
        template=EDGE_T_DERIVED_ANCHOR,
        required_overrides=(("use_kind", p_str(enum=PROP_ENUMS["use_kind"])),),
        description="Use site consumes a binding (anchored evidence).",
    ),
)

EDGE_KIND_CONTRACTS: dict[EdgeKind, EdgeKindContract] = build_edge_contracts(EDGE_CONTRACT_ROWS)

# ----------------------------
# Derivation templates
# ----------------------------

DERIV_T_AST = DerivationTemplate(
    extractor="extract.ast_extract:extract_ast_tables",
    confidence_policy="confidence=1.0",
    ambiguity_policy="none",
)
DERIV_T_CST = DerivationTemplate(
    extractor="extract.cst_extract:extract_cst_tables",
    confidence_policy="confidence=1.0",
    ambiguity_policy="none",
)
DERIV_T_REPO_SCAN = DerivationTemplate(
    extractor="extract.repo_scan:scan_repo",
    confidence_policy="confidence=1.0",
    ambiguity_policy="none",
)
DERIV_T_SCIP_EXTRACT = DerivationTemplate(
    extractor="extract.scip_extract:extract_scip_tables",
    confidence_policy="confidence=1.0",
    ambiguity_policy="none",
)
DERIV_T_SYMTABLE = DerivationTemplate(
    extractor="extract.symtable_extract:extract_symtables_table",
    confidence_policy="confidence=1.0",
    ambiguity_policy="none",
)
DERIV_T_CPG_EDGES = DerivationTemplate(
    extractor="relspec.cpg.build_edges:build_cpg_edges_raw",
    confidence_policy="confidence=1.0",
    ambiguity_policy="none",
)
DERIV_T_CPG_NODES = DerivationTemplate(
    extractor="relspec.cpg.build_nodes:build_cpg_nodes_raw",
    confidence_policy="confidence=1.0",
    ambiguity_policy="none",
)
DERIV_T_REL_REGISTRY = DerivationTemplate(
    extractor="hamilton_pipeline.modules.cpg_build:rule_registry",
    confidence_policy="confidence=1.0",
    ambiguity_policy="none",
)
DERIV_T_RUNTIME_OBJECTS = DerivationTemplate(
    extractor="extract.runtime_inspect_extract:extract_runtime_objects",
    confidence_policy="confidence=0.9",
    ambiguity_policy="none",
)
DERIV_T_RUNTIME_SIGNATURES = DerivationTemplate(
    extractor="extract.runtime_inspect_extract:extract_runtime_signatures",
    confidence_policy="confidence=1.0",
    ambiguity_policy="none",
)
DERIV_T_RUNTIME_MEMBERS = DerivationTemplate(
    extractor="extract.runtime_inspect_extract:extract_runtime_members",
    confidence_policy="confidence=0.9",
    ambiguity_policy="none",
)

# ----------------------------
# Derivation Manifest (NODE_DERIVATIONS / EDGE_DERIVATIONS)
# ----------------------------

NODE_DERIVATION_ROWS: tuple[DerivationRow[NodeKind], ...] = (
    DerivationRow(
        kind=NodeKind.AST_ATTRIBUTE,
        provider_or_field="ast.Attribute nodes",
        join_keys=(
            "path",
            "lineno",
            "col_offset",
        ),
        id_recipe="ast_attr_id = sha('AST_ATTRIBUTE:'+path+lineno+col)",
        confidence_policy="confidence=0.9",
        ambiguity_policy="n/a",
        status="planned",
        template=DERIV_T_AST,
    ),
    DerivationRow(
        kind=NodeKind.AST_CALL,
        provider_or_field="ast.Call nodes",
        join_keys=(
            "path",
            "lineno",
            "col_offset",
        ),
        id_recipe="ast_call_id = sha('AST_CALL:'+path+lineno+col)",
        confidence_policy="confidence=0.9",
        ambiguity_policy="n/a",
        status="planned",
        template=DERIV_T_AST,
    ),
    DerivationRow(
        kind=NodeKind.AST_DEF,
        provider_or_field="ast.FunctionDef/AsyncFunctionDef/ClassDef/Lambda",
        join_keys=(
            "path",
            "lineno",
            "col_offset",
        ),
        id_recipe="ast_def_id = sha('AST_DEF:'+path+lineno+col)",
        confidence_policy="confidence=0.9 (AST def positions reliable but not trivia-exact)",
        ambiguity_policy="n/a",
        status="planned",
        template=DERIV_T_AST,
    ),
    DerivationRow(
        kind=NodeKind.AST_NAME,
        provider_or_field="ast.Name nodes",
        join_keys=(
            "path",
            "lineno",
            "col_offset",
        ),
        id_recipe="ast_name_id = sha('AST_NAME:'+path+lineno+col)",
        confidence_policy="confidence=0.9",
        ambiguity_policy="n/a",
        status="planned",
        template=DERIV_T_AST,
    ),
    DerivationRow(
        kind=NodeKind.AST_NODE,
        provider_or_field=("python ast.parse + node walk; (lineno,col)->bytes via normalize.spans"),
        join_keys=("path",),
        id_recipe=(
            "ast_id = stable_id(path, bstart, bend, 'AST_NODE') when bytes available; "
            "else sha(ast_type+lineno+col+path)"
        ),
        confidence_policy=(
            "confidence=0.95 when end positions present and byte mapping succeeds; else 0.7"
        ),
        ambiguity_policy="n/a",
        template=DERIV_T_AST,
    ),
    DerivationRow(
        kind=NodeKind.BC_CODE_UNIT,
        provider_or_field=(
            "compile + traverse code objects; record co_qualname/co_firstlineno; dis-compatible"
        ),
        join_keys=(
            "path",
            "qualname",
            "co_firstlineno",
        ),
        id_recipe="code_unit_id = sha('BC:'+path+':'+qualname+':'+firstlineno)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        extractor="extract.bytecode_extract:extract_bytecode_table",
        status="planned",
    ),
    DerivationRow(
        kind=NodeKind.BC_INSTR,
        provider_or_field="dis.get_instructions(codeobj) + store baseopname",
        join_keys=(
            "code_unit_id",
            "offset",
        ),
        id_recipe="instr_id = sha(code_unit_id+':OFF:'+offset)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        extractor="extract.bytecode_extract:extract_bytecode_table",
        status="planned",
    ),
    DerivationRow(
        kind=NodeKind.CFG_BLOCK,
        provider_or_field="basic block formation + jump semantics + exception table edges",
        join_keys=("code_unit_id",),
        id_recipe="block_id = f'{code_unit_id}:B:{start_off}'",
        confidence_policy="confidence=0.95",
        ambiguity_policy="none",
        extractor="normalize.ibis_api:build_cfg_blocks",
        status="planned",
    ),
    DerivationRow(
        kind=NodeKind.CFG_EXIT,
        provider_or_field="synthetic per code_unit_id",
        join_keys=("code_unit_id",),
        id_recipe="exit_id = sha(code_unit_id+':EXIT')[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        status="planned",
        extractor="normalize.ibis_api:build_cfg_edges",
    ),
    DerivationRow(
        kind=NodeKind.CST_ARG,
        provider_or_field="LibCST Arg + ByteSpanPositionProvider",
        id_recipe=stable_span_id("arg_id", "CST_ARG"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="n/a",
        status="planned",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_ATTRIBUTE_ACCESS,
        provider_or_field="LibCST Attribute + ByteSpanPositionProvider",
        id_recipe=stable_span_id("attr_id", "CST_ATTRIBUTE_ACCESS"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="resolution ambiguity handled via ATTR_CANDIDATE_QNAME edges",
        status="planned",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_CALLSITE,
        provider_or_field="LibCST Call + ByteSpanPositionProvider",
        id_recipe=stable_span_id("call_id", "CST_CALLSITE"),
        confidence_policy=(
            "confidence=1.0 for syntax; call target resolution edges carry confidence/score"
        ),
        ambiguity_policy="store multi candidate call edges; winner selection is contract-driven",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_CLASS_DEF,
        provider_or_field="LibCST ClassDef + ByteSpanPositionProvider",
        id_recipe=stable_span_id("def_id", "CST_CLASS_DEF"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_COMMENT,
        provider_or_field="LibCST Comment nodes + ByteSpanPositionProvider",
        id_recipe=stable_span_id("comment_id", "CST_COMMENT"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="n/a",
        status="planned",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_DECORATOR,
        provider_or_field="LibCST Decorator + ByteSpanPositionProvider",
        id_recipe=stable_span_id("decorator_id", "CST_DECORATOR"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        status="planned",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_DEF,
        provider_or_field="LibCST FunctionDef/ClassDef/Lambda nodes + ByteSpanPositionProvider",
        id_recipe=stable_span_id("def_id", "CST_DEF"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_DOCSTRING,
        provider_or_field=(
            "LibCST docstring detection (first statement string literal in suite) "
            "+ ByteSpanPositionProvider"
        ),
        id_recipe=stable_span_id("doc_id", "CST_DOCSTRING"),
        confidence_policy="confidence=0.95 (docstring heuristics are conventional but reliable)",
        ambiguity_policy="n/a",
        status="planned",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_FUNCTION_DEF,
        provider_or_field="LibCST FunctionDef/AsyncFunctionDef + ByteSpanPositionProvider",
        id_recipe=stable_span_id("def_id", "CST_FUNCTION_DEF"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_IMPORT,
        provider_or_field="LibCST Import + ByteSpanPositionProvider",
        id_recipe=stable_span_id("import_id", "CST_IMPORT"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_IMPORT_ALIAS,
        provider_or_field="LibCST ImportAlias + ByteSpanPositionProvider",
        id_recipe=stable_span_id("import_alias_id", "CST_IMPORT_ALIAS"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_IMPORT_FROM,
        provider_or_field="LibCST ImportFrom + ByteSpanPositionProvider",
        id_recipe=stable_span_id("import_id", "CST_IMPORT_FROM"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_LAMBDA,
        provider_or_field="LibCST Lambda + ByteSpanPositionProvider",
        id_recipe=stable_span_id("lambda_id", "CST_LAMBDA"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        status="planned",
        notes="Often worth emitting only if you need lambda-level flow/type edges.",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_LITERAL,
        provider_or_field=(
            "LibCST literal nodes (SimpleString, Integer, Float, etc.) + ByteSpanPositionProvider"
        ),
        id_recipe=stable_span_id("lit_id", "CST_LITERAL"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="n/a",
        status="planned",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_REF,
        provider_or_field=(
            "LibCST ref nodes + ExpressionContextProvider + ByteSpanPositionProvider"
        ),
        id_recipe=stable_span_id("ref_id", "CST_REF"),
        confidence_policy=(
            "confidence=1.0 for span/ref; 0.9 for expr_context when provider yields context; "
            "else expr_context='UNKNOWN'"
        ),
        ambiguity_policy="none at syntax level; ambiguity appears in resolution edges",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_NODE,
        provider_or_field=(
            "LibCST parse_module + MetadataWrapper(ByteSpanPositionProvider, PositionProvider, "
            "ParentNodeProvider)"
        ),
        id_recipe=stable_span_id("node_id", "CST_NODE"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_PARAM,
        provider_or_field="LibCST Param + ByteSpanPositionProvider",
        id_recipe=stable_span_id("param_id", "CST_PARAM"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        status="planned",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.CST_SUBSCRIPT,
        provider_or_field="LibCST Subscript + ByteSpanPositionProvider",
        id_recipe=stable_span_id("sub_id", "CST_SUBSCRIPT"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="n/a",
        status="planned",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.DF_DEF,
        provider_or_field="opcode classifier -> def events; stack/locals/globals model",
        join_keys=(
            "code_unit_id",
            "instr_id",
            "binding_id",
        ),
        id_recipe="df_id = sha('DEF:'+code_unit_id+':'+instr_id+':'+binding_id)[:16]",
        confidence_policy="confidence=0.8-0.95 depending on opcode coverage",
        ambiguity_policy="if binding ambiguous, emit multiple DF_DEF candidates with scores",
        extractor="normalize.ibis_api:build_def_use_events",
        status="planned",
    ),
    DerivationRow(
        kind=NodeKind.DF_USE,
        provider_or_field="opcode classifier -> use events",
        join_keys=(
            "code_unit_id",
            "instr_id",
            "binding_id",
        ),
        id_recipe="df_id = sha('USE:'+code_unit_id+':'+instr_id+':'+binding_id)[:16]",
        confidence_policy="confidence=0.8-0.95 depending on opcode coverage",
        ambiguity_policy="if binding ambiguous, emit multiple DF_USE candidates with scores",
        extractor="normalize.ibis_api:build_def_use_events",
        status="planned",
    ),
    DerivationRow(
        kind=NodeKind.DIAG,
        provider_or_field=(
            "aggregate parse errors (tree-sitter), SCIP diagnostics, extraction failures"
        ),
        join_keys=(
            "path",
            "bstart",
            "bend",
        ),
        id_recipe=stable_span_id("diag_id", "DIAG"),
        confidence_policy="confidence=1.0 for source diag; 0.7 for derived diag",
        ambiguity_policy="none",
        extractor="normalize.ibis_api:collect_diags",
        status="planned",
    ),
    DerivationRow(
        kind=NodeKind.PY_BINDING,
        provider_or_field="derive from symtable symbol flags per (scope,name)",
        join_keys=(
            "scope_id",
            "name",
        ),
        id_recipe="binding_id = f'{scope_id}:BIND:{name}'",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        status="implemented",
        extractor="hamilton_pipeline.modules.cpg_build:symtable_bindings",
    ),
    DerivationRow(
        kind=NodeKind.PY_DEF_SITE,
        provider_or_field="LibCST Name spans + symtable binding classification (def sites)",
        join_keys=(
            "path",
            "bstart",
            "bend",
            "name",
        ),
        id_recipe="def_site_id = sha('PY_DEF_SITE:'+binding_id+':'+bstart+':'+bend)[:16]",
        confidence_policy="confidence=0.85-0.95 depending on def-site classifier maturity",
        ambiguity_policy=(
            "if multiple bindings match, emit multiple DEF_SITE_OF edges "
            "(ambiguity_group_id=def_site_id)"
        ),
        status="implemented",
        extractor="hamilton_pipeline.modules.cpg_build:symtable_def_sites",
    ),
    DerivationRow(
        kind=NodeKind.PY_FILE,
        provider_or_field="filesystem walk + include/exclude globs",
        id_recipe="file_id = sha256('FILE:' + repo_rel_path)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_REPO_SCAN,
    ),
    DerivationRow(
        kind=NodeKind.PY_MODULE,
        provider_or_field="LibCST FullRepoManager + FullyQualifiedNameProvider",
        join_keys=("path",),
        id_recipe=(
            "module_id = sha256('MOD:' + module_fqn)[:16] (or stable_id(path,0,0,'PY_MODULE'))"
        ),
        confidence_policy="confidence=0.95 when repo/module roots configured; else 0.7",
        ambiguity_policy=(
            "if multiple roots produce multiple fqns, store all module nodes "
            "(ambiguity_group_id=path)"
        ),
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.PY_MODULE_FQN,
        provider_or_field="FullyQualifiedNameProvider on Module",
        join_keys=("module_fqn",),
        id_recipe="module_fqn_id = sha('MODFQN:'+module_fqn)[:16]",
        confidence_policy="confidence=0.95",
        ambiguity_policy="if multiple fqns, keep them and attach ambiguity_group_id=path",
        status="planned",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.PY_PACKAGE,
        provider_or_field="derive from __init__.py presence + module_fqn mapping",
        join_keys=("path",),
        id_recipe="package_id = sha256('PKG:' + package_fqn)[:16]",
        confidence_policy="confidence=0.8-0.95 depending on packaging heuristics",
        ambiguity_policy="store multiple candidates if package roots ambiguous",
        status="planned",
        template=DERIV_T_REPO_SCAN,
    ),
    DerivationRow(
        kind=NodeKind.PY_QUALIFIED_NAME,
        provider_or_field=(
            "LibCST QualifiedNameProvider/FullyQualifiedNameProvider candidate strings"
        ),
        join_keys=("qname",),
        id_recipe="qname_id = sha('QNAME:'+qname)[:16]",
        confidence_policy="confidence=0.6-0.9 depending on provider (FullyQualified > Qualified)",
        ambiguity_policy="qname nodes are distinct; ambiguity lives on edges from ref->qname",
        extractor="hamilton_pipeline.modules.normalization:dim_qualified_names",
        status="planned",
    ),
    DerivationRow(
        kind=NodeKind.PY_REPO,
        provider_or_field="repo_root + optional git HEAD sniff",
        id_recipe="repo_id = sha256(repo_root + git_commit?)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_REPO_SCAN,
    ),
    DerivationRow(
        kind=NodeKind.PY_SCOPE,
        provider_or_field=(
            "normalize SYM_SCOPE (+ optional LibCST ScopeProvider ids) into a single "
            "scope namespace"
        ),
        join_keys=("scope_id",),
        id_recipe="py_scope_id = scope_id",
        confidence_policy="confidence=0.95 (symtable authoritative; libcst scope can enrich)",
        ambiguity_policy=(
            "if libcst vs symtable mismatch, keep both and link via props or resolution edges"
        ),
        status="implemented",
        extractor="hamilton_pipeline.modules.cpg_build:symtable_scopes",
    ),
    DerivationRow(
        kind=NodeKind.PY_USE_SITE,
        provider_or_field="LibCST Name spans + expr_context + symtable binding resolution",
        join_keys=(
            "path",
            "bstart",
            "bend",
            "name",
        ),
        id_recipe="use_site_id = sha('PY_USE_SITE:'+binding_id+':'+bstart+':'+bend)[:16]",
        confidence_policy="confidence=0.85-0.95 depending on binding resolution maturity",
        ambiguity_policy="emit multiple USE_SITE_OF edges if needed; dedupe by winner policy",
        status="implemented",
        extractor="hamilton_pipeline.modules.cpg_build:symtable_use_sites",
    ),
    DerivationRow(
        kind=NodeKind.RT_MEMBER,
        provider_or_field="inspect.getmembers_static(obj)",
        join_keys=(
            "rt_id",
            "name",
        ),
        id_recipe="member_id = sha(rt_id+':M:'+name)[:16]",
        confidence_policy="confidence=0.9",
        ambiguity_policy="none",
        template=DERIV_T_RUNTIME_MEMBERS,
    ),
    DerivationRow(
        kind=NodeKind.RT_OBJECT,
        provider_or_field="inspect.getmembers_static + inspect.unwrap + safe signature extraction",
        join_keys=(
            "module",
            "qualname",
        ),
        id_recipe="rt_id = sha('RT:'+module+':'+qualname)[:16]",
        confidence_policy="confidence=0.6-0.9 depending on ability to map back to source",
        ambiguity_policy=(
            "if multiple objects share qualname, include module+id() fingerprint in rt_id"
        ),
        template=DERIV_T_RUNTIME_OBJECTS,
    ),
    DerivationRow(
        kind=NodeKind.RT_SIGNATURE,
        provider_or_field="inspect.signature(obj) (safe objects only)",
        join_keys=("rt_id",),
        id_recipe="sig_id = sha(rt_id+':SIG:'+signature_str)[:16]",
        confidence_policy="confidence=0.8-1.0 depending on callable type",
        ambiguity_policy="if signature fails, emit DIAG instead",
        template=DERIV_T_RUNTIME_SIGNATURES,
    ),
    DerivationRow(
        kind=NodeKind.RT_SIGNATURE_PARAM,
        provider_or_field="signature.parameters items",
        join_keys=(
            "sig_id",
            "name",
        ),
        id_recipe="param_id = sha(sig_id+':P:'+name)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_RUNTIME_SIGNATURES,
    ),
    DerivationRow(
        kind=NodeKind.SCIP_DIAGNOSTIC,
        provider_or_field="Document.diagnostics (if present) normalized to bytes",
        join_keys=(
            "path",
            "range",
        ),
        id_recipe=stable_span_id("diag_id", "SCIP_DIAGNOSTIC"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        status="planned",
        template=DERIV_T_SCIP_EXTRACT,
    ),
    DerivationRow(
        kind=NodeKind.SCIP_DOCUMENT,
        provider_or_field="SCIP Document.relative_path",
        join_keys=("path",),
        id_recipe="doc_id = sha('SCIP_DOC:'+path)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_SCIP_EXTRACT,
    ),
    DerivationRow(
        kind=NodeKind.SCIP_INDEX,
        provider_or_field="scip_metadata_v1 index metadata rows",
        id_recipe="scip_index_id = index_id",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_SCIP_EXTRACT,
    ),
    DerivationRow(
        kind=NodeKind.SCIP_OCCURRENCE,
        provider_or_field="Occurrence(symbol, symbol_roles, range) normalized to bytes",
        join_keys=(
            "path",
            "range",
        ),
        id_recipe=stable_span_id("occ_id", "SCIP_OCCURRENCE"),
        confidence_policy=(
            "confidence=1.0 for symbol+roles; 0.9 for byte span mapping when encoding tricky"
        ),
        ambiguity_policy="none",
        extractor="normalize.ibis_api:add_scip_occurrence_byte_spans",
        status="planned",
    ),
    DerivationRow(
        kind=NodeKind.SCIP_SYMBOL,
        provider_or_field="SCIP SymbolInformation.symbol + fields",
        join_keys=("symbol",),
        id_recipe="symbol_node_id = symbol (or sha('SCIP_SYMBOL:'+symbol)[:16])",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_SCIP_EXTRACT,
    ),
    DerivationRow(
        kind=NodeKind.SYM_SCOPE,
        provider_or_field="symtable.symtable(...); table.get_type().name; table.get_children()",
        join_keys=("path",),
        id_recipe="scope_id = f'{file_id}:SCOPE:{qualpath}:{lineno}:{scope_type}:{ordinal}'",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_SYMTABLE,
    ),
    DerivationRow(
        kind=NodeKind.SYM_SYMBOL,
        provider_or_field=(
            "table.lookup(name) and symbol flags: is_local/is_free/is_global/is_nonlocal/"
            "is_parameter..."
        ),
        join_keys=(
            "scope_id",
            "name",
        ),
        id_recipe="sym_symbol_id = sha('SYM_SYMBOL:'+scope_id+':'+name)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_SYMTABLE,
    ),
    DerivationRow(
        kind=NodeKind.TS_ERROR,
        provider_or_field="node.is_error == True",
        id_recipe="ts_err_id = stable_id(path, start_byte, end_byte, 'TS_ERROR')",
        confidence_policy="confidence=1.0",
        ambiguity_policy="n/a",
        extractor="extract.tree_sitter_extract:extract_ts_tables",
        status="planned",
    ),
    DerivationRow(
        kind=NodeKind.TS_MISSING,
        provider_or_field="node.is_missing == True",
        id_recipe="ts_missing_id = stable_id(path, start_byte, end_byte, 'TS_MISSING')",
        confidence_policy="confidence=1.0",
        ambiguity_policy="n/a",
        extractor="extract.tree_sitter_extract:extract_ts_tables",
        status="planned",
    ),
    DerivationRow(
        kind=NodeKind.TS_NODE,
        provider_or_field="tree-sitter nodes (start_byte/end_byte, is_error/is_missing/has_error)",
        id_recipe="ts_id = stable_id(path, start_byte, end_byte, 'TS_NODE')",
        confidence_policy="confidence=1.0",
        ambiguity_policy="n/a",
        extractor="extract.tree_sitter_extract:extract_ts_tables",
        status="planned",
    ),
    DerivationRow(
        kind=NodeKind.TYPE,
        provider_or_field=(
            "normalize annotation strings + optional inference (Pyre/Watchman via LibCST "
            "TypeInferenceProvider)"
        ),
        join_keys=("type_repr",),
        id_recipe="type_id = sha('TYPE:'+type_repr)[:16]",
        confidence_policy="annotation-derived=1.0; inferred=0.7-0.9; runtime=0.6-0.9",
        ambiguity_policy=(
            "multi-valued types allowed (union/overload); store multiple TYPE nodes/edges"
        ),
        extractor="normalize.ibis_api:normalize_types",
        status="planned",
    ),
    DerivationRow(
        kind=NodeKind.TYPE_ALIAS,
        provider_or_field="symtable TYPE_ALIAS scope + extracted assignments",
        join_keys=(
            "scope_id",
            "name",
        ),
        id_recipe="type_alias_id = sha('TA:'+scope_id+':'+name)[:16]",
        confidence_policy="confidence=0.7-0.9 (depends on alias extraction fidelity)",
        ambiguity_policy="if alias target ambiguous, keep multiple targets with scores",
        status="planned",
        template=DERIV_T_SYMTABLE,
    ),
    DerivationRow(
        kind=NodeKind.TYPE_EXPR,
        provider_or_field=(
            "LibCST annotation nodes + ByteSpanPositionProvider (or AST AnnAssign/arg.annotation)"
        ),
        id_recipe=stable_span_id("type_expr_id", "TYPE_EXPR"),
        confidence_policy="confidence=1.0 for syntax capture",
        ambiguity_policy="none",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=NodeKind.TYPE_PARAM,
        provider_or_field="symtable meta scopes TYPE_PARAMETERS/TYPE_VARIABLE",
        join_keys=(
            "scope_id",
            "name",
        ),
        id_recipe="type_param_id = scope_id",
        confidence_policy="confidence=0.9",
        ambiguity_policy="none",
        status="implemented",
        template=DERIV_T_SYMTABLE,
        extractor="hamilton_pipeline.modules.cpg_build:symtable_type_params",
    ),
)

NODE_DERIVATIONS: dict[NodeKind, list[DerivationSpec]] = build_derivations(NODE_DERIVATION_ROWS)

EDGE_DERIVATION_ROWS: tuple[DerivationRow[EdgeKind], ...] = (
    DerivationRow(
        kind=EdgeKind.AST_PARENT_OF,
        provider_or_field="AST walk (parent pointers via stack)",
        join_keys=("parent_ast_id -> child_ast_id",),
        id_recipe="edge_id = sha('AST_PARENT_OF:'+p+':'+c)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        status="planned",
        template=DERIV_T_AST,
    ),
    DerivationRow(
        kind=EdgeKind.ATTR_CANDIDATE_QNAME,
        provider_or_field="QualifiedNameProvider mapping for Attribute nodes",
        join_keys=(
            "attr_id",
            "qname",
        ),
        id_recipe="edge_id = sha('ATTR_CAND_QNAME:'+attr_id+':'+qname_id)[:16]",
        confidence_policy="confidence=0.6-0.8",
        ambiguity_policy="multi-valued; ambiguity_group_id=attr_id",
        status="planned",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=EdgeKind.BINDING_RESOLVES_TO,
        provider_or_field="symtable flags is_global/is_nonlocal/is_free + parent chain",
        join_keys=("binding_id",),
        id_recipe="edge_id = sha('BINDING_RESOLVES_TO:'+binding_id+':'+outer_binding_id)[:16]",
        confidence_policy="confidence=0.9",
        ambiguity_policy=(
            "if multiple candidates, emit multiple edges with scores and "
            "ambiguity_group_id=binding_id"
        ),
        status="implemented",
        extractor="cpg.symtable_resolution:build_binding_resolution_table",
    ),
    DerivationRow(
        kind=EdgeKind.BYTECODE_ANCHOR,
        provider_or_field="instruction.starts_line / line table / heuristic span mapping",
        join_keys=("instr_id -> anchor span",),
        id_recipe="edge_id = sha('BYTECODE_ANCHOR:'+instr_id+':'+anchor_id)[:16]",
        confidence_policy="line_table=0.9, offset_span=0.8, heuristic=0.6",
        ambiguity_policy="if multiple anchors, keep all with scores; winner selection by score",
        extractor="normalize.ibis_api:anchor_instructions",
        status="planned",
    ),
    DerivationRow(
        kind=EdgeKind.CALLSITE_CANDIDATE_QNAME,
        provider_or_field="callee_qnames list explosion from LibCST callsites",
        join_keys=(
            "call_id",
            "qname",
        ),
        id_recipe="edge_id = sha('CALL_CAND_QNAME:'+call_id+':'+qname_id)[:16]",
        confidence_policy="score based on candidate position; confidence default 0.6",
        ambiguity_policy="multi-valued; ambiguity_group_id = call_id",
        extractor="hamilton_pipeline.modules.normalization:callsite_qname_candidates",
        status="planned",
    ),
    DerivationRow(
        kind=EdgeKind.CFG_BRANCH,
        provider_or_field="merged branch edges when polarity not tracked",
        join_keys=("block_id",),
        id_recipe="edge_id = sha('CFG_BRANCH:'+src_block+':'+dst_block)[:16]",
        confidence_policy="confidence=0.9",
        ambiguity_policy="none",
        extractor="normalize.ibis_api:build_cfg_edges",
        status="planned",
    ),
    DerivationRow(
        kind=EdgeKind.CFG_BRANCH_FALSE,
        provider_or_field="conditional jump false target (fallthrough)",
        join_keys=("block_id",),
        id_recipe="edge_id = sha('CFG_BRANCH_FALSE:'+src_block+':'+dst_block)[:16]",
        confidence_policy="confidence=0.95",
        ambiguity_policy="none",
        extractor="normalize.ibis_api:build_cfg_edges",
        status="planned",
    ),
    DerivationRow(
        kind=EdgeKind.CFG_BRANCH_TRUE,
        provider_or_field="conditional jump true target",
        join_keys=("block_id",),
        id_recipe="edge_id = sha('CFG_BRANCH_TRUE:'+src_block+':'+dst_block)[:16]",
        confidence_policy="confidence=0.95",
        ambiguity_policy="none",
        extractor="normalize.ibis_api:build_cfg_edges",
        status="planned",
    ),
    DerivationRow(
        kind=EdgeKind.CFG_EXC,
        provider_or_field="exception table projection -> handler entry edges",
        join_keys=("block_id",),
        id_recipe="edge_id = sha('CFG_EXC:'+src_block+':'+handler_block+':'+exc_entry_index)[:16]",
        confidence_policy="confidence=0.9-0.95",
        ambiguity_policy="none",
        extractor="normalize.ibis_api:build_cfg_edges",
        status="planned",
    ),
    DerivationRow(
        kind=EdgeKind.CFG_JUMP,
        provider_or_field="unconditional jump semantics",
        join_keys=("block_id",),
        id_recipe="edge_id = sha('CFG_JUMP:'+src_block+':'+dst_block)[:16]",
        confidence_policy="confidence=0.95",
        ambiguity_policy="none",
        extractor="normalize.ibis_api:build_cfg_edges",
        status="planned",
    ),
    DerivationRow(
        kind=EdgeKind.CFG_NEXT,
        provider_or_field="fallthrough edges between basic blocks",
        join_keys=("block_id",),
        id_recipe="edge_id = sha('CFG_NEXT:'+src_block+':'+dst_block)[:16]",
        confidence_policy="confidence=0.95",
        ambiguity_policy="none",
        extractor="normalize.ibis_api:build_cfg_edges",
        status="planned",
    ),
    DerivationRow(
        kind=EdgeKind.CST_PARENT_OF,
        provider_or_field="LibCST ParentNodeProvider (parent pointers) + traversal order",
        join_keys=("parent_node_id -> child_node_id",),
        id_recipe="edge_id = sha('CST_PARENT_OF:'+parent_id+':'+child_id)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        status="planned",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=EdgeKind.DEF_SITE_OF,
        provider_or_field="LibCST def sites (Name spans) classified via symtable binding_kind",
        join_keys=(
            "path",
            "bstart",
            "bend",
            "binding_id",
        ),
        id_recipe="edge_id = sha('DEF_SITE_OF:'+def_site_id+':'+binding_id)[:16]",
        confidence_policy="confidence=0.85-0.95",
        ambiguity_policy="multiple edges allowed, winner selection by score",
        status="implemented",
        extractor="hamilton_pipeline.modules.cpg_build:symtable_def_sites",
    ),
    DerivationRow(
        kind=EdgeKind.FILE_CONTAINS,
        provider_or_field="attach file_id on anchored nodes",
        join_keys=("node.file_id",),
        id_recipe="edge_id = sha('FILE_CONTAINS:'+file_id+':'+node_id+':'+role)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        status="planned",
        template=DERIV_T_CPG_NODES,
    ),
    DerivationRow(
        kind=EdgeKind.FILE_DECLARES_MODULE,
        provider_or_field="FullyQualifiedNameProvider on Module",
        join_keys=("file.path == module.path",),
        id_recipe="edge_id = sha('FILE_DECLARES_MODULE:'+file_id+':'+module_id)[:16]",
        confidence_policy="confidence=0.95",
        ambiguity_policy=(
            "if multiple module fqns, emit multiple edges with ambiguity_group_id=file_id"
        ),
        status="planned",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=EdgeKind.HAS_ANNOTATION,
        provider_or_field="LibCST annotation nodes (param.annotation / returns annotation)",
        join_keys=("def/param -> TYPE_EXPR span",),
        id_recipe="edge_id = sha('HAS_ANNOTATION:'+src_id+':'+type_expr_id)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_CPG_EDGES,
    ),
    DerivationRow(
        kind=EdgeKind.HAS_DIAGNOSTIC,
        provider_or_field="attach DIAG nodes to owning file/module",
        join_keys=("path",),
        id_recipe="edge_id = sha('HAS_DIAG:'+owner_id+':'+diag_id)[:16]",
        confidence_policy="confidence=1.0 if from source tool; else 0.7",
        ambiguity_policy="none",
        template=DERIV_T_CPG_EDGES,
    ),
    DerivationRow(
        kind=EdgeKind.INFERRED_TYPE,
        provider_or_field="LibCST TypeInferenceProvider (Pyre/Watchman) OR heuristic OR runtime",
        join_keys=("src_id",),
        id_recipe="edge_id = sha('INFERRED_TYPE:'+src_id+':'+type_id+':'+origin)[:16]",
        confidence_policy="pyre=0.8-0.9; runtime=0.6-0.9; heuristic=0.5-0.7",
        ambiguity_policy="multi-valued types allowed; dedupe by type_id with score",
        template=DERIV_T_CPG_EDGES,
    ),
    DerivationRow(
        kind=EdgeKind.NAME_REF_CANDIDATE_QNAME,
        provider_or_field=(
            "LibCST QualifiedNameProvider/FullyQualifiedNameProvider mapping for ref nodes"
        ),
        join_keys=(
            "ref_id",
            "qname",
        ),
        id_recipe="edge_id = sha('REF_CAND_QNAME:'+ref_id+':'+qname_id)[:16]",
        confidence_policy="FullyQualified=0.9, Qualified=0.7; score based on provider rank",
        ambiguity_policy="multi-valued; ambiguity_group_id = sha(ref_id+':QNAME')[:16]",
        status="planned",
        template=DERIV_T_CST,
    ),
    DerivationRow(
        kind=EdgeKind.PY_CALLS_QNAME,
        provider_or_field=(
            "fallback: callsite_qname_candidates join dim_qualified_names "
            "(only when no callsite->symbol)"
        ),
        join_keys=(
            "call_id",
            "qname",
        ),
        id_recipe="edge_id = sha('PY_CALLS_QNAME:'+call_id+':'+qname_id)[:16]",
        confidence_policy="confidence=0.5-0.7; score from candidate ranking",
        ambiguity_policy="multi-valued; ambiguity_group_id=call_id; may keep top-K",
        template=DERIV_T_CPG_EDGES,
    ),
    DerivationRow(
        kind=EdgeKind.PY_CALLS_SYMBOL,
        provider_or_field="SCIP occurrence aligned to callee span",
        join_keys=("interval_align: callsite.callee_span contains occurrence span",),
        id_recipe="edge_id = sha('PY_CALLS_SYMBOL:'+call_id+':'+symbol+':'+call_span)[:16]",
        confidence_policy="confidence=1.0; score=-span_len",
        ambiguity_policy="if multiple, keep best by score then rule_priority",
        template=DERIV_T_REL_REGISTRY,
    ),
    DerivationRow(
        kind=EdgeKind.PY_DEFINES_SYMBOL,
        provider_or_field="SCIP Occurrence.symbol_roles bitmask & Definition",
        join_keys=SPAN_CONTAINED_BEST,
        id_recipe=(
            "edge_id = sha('PY_DEFINES_SYMBOL:'+src_id+':'+symbol+':'+path+':'+bstart"
            "+':'+bend)[:16]"
        ),
        confidence_policy=(
            "confidence=1.0; score=-span_len (more specific wins); tie-break rule_priority asc"
        ),
        ambiguity_policy=(
            "if multiple overlaps, group by src_id and keep-first-after-sort by "
            "score/confidence/rule_priority"
        ),
        template=DERIV_T_REL_REGISTRY,
    ),
    DerivationRow(
        kind=EdgeKind.PY_IMPORTS_SYMBOL,
        provider_or_field="SCIP Occurrence.symbol_roles bitmask & Import",
        join_keys=("interval_align: left import alias span contains occurrence span",),
        id_recipe=(
            "edge_id = sha('PY_IMPORTS_SYMBOL:'+src_id+':'+symbol+':'+path+':'+bstart"
            "+':'+bend)[:16]"
        ),
        confidence_policy="confidence=1.0; score=-span_len",
        ambiguity_policy="dedupe by (src_id,symbol,span) + tie-break by rule_priority",
        template=DERIV_T_REL_REGISTRY,
    ),
    DerivationRow(
        kind=EdgeKind.PY_READS_SYMBOL,
        provider_or_field="filter rel_name_symbol by symbol_roles & READ bit",
        join_keys=(
            "ref_id",
            "symbol",
        ),
        id_recipe="edge_id = sha('PY_READS_SYMBOL:'+ref_id+':'+symbol+':'+span)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="same as rel_name_symbol dedupe",
        template=DERIV_T_CPG_EDGES,
    ),
    DerivationRow(
        kind=EdgeKind.PY_REFERENCES_SYMBOL,
        provider_or_field="SCIP Occurrence.symbol_roles without Definition bit",
        join_keys=SPAN_CONTAINED_BEST,
        id_recipe=(
            "edge_id = sha('PY_REFERENCES_SYMBOL:'+src_id+':'+symbol+':'+path+':'+bstart"
            "+':'+bend)[:16]"
        ),
        confidence_policy="confidence=1.0; score=-span_len; tie-break rule_priority asc",
        ambiguity_policy="same as defines",
        template=DERIV_T_REL_REGISTRY,
    ),
    DerivationRow(
        kind=EdgeKind.PY_WRITES_SYMBOL,
        provider_or_field="filter rel_name_symbol by symbol_roles & WRITE bit",
        join_keys=(
            "ref_id",
            "symbol",
        ),
        id_recipe="edge_id = sha('PY_WRITES_SYMBOL:'+ref_id+':'+symbol+':'+span)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="same as rel_name_symbol dedupe",
        template=DERIV_T_CPG_EDGES,
    ),
    DerivationRow(
        kind=EdgeKind.REACHES,
        provider_or_field="reaching definitions analysis over CFG",
        join_keys=("code_unit_id",),
        id_recipe="edge_id = sha('REACHES:'+def_id+':'+use_id)[:16]",
        confidence_policy="confidence=0.8-0.95",
        ambiguity_policy="none",
        extractor="normalize.ibis_api:run_reaching_defs",
        status="planned",
    ),
    DerivationRow(
        kind=EdgeKind.REPO_CONTAINS,
        provider_or_field="repo_files list",
        join_keys=("repo_id -> file_id",),
        id_recipe=sha_id("REPO_CONTAINS", "repo_id", "file_id"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_REPO_SCAN,
    ),
    DerivationRow(
        kind=EdgeKind.RT_HAS_MEMBER,
        provider_or_field="inspect.getmembers_static(obj)",
        join_keys=("rt_id",),
        id_recipe="edge_id = sha('RT_HAS_MEMBER:'+rt_id+':'+member_id)[:16]",
        confidence_policy="confidence=0.9",
        ambiguity_policy="none",
        template=DERIV_T_CPG_EDGES,
    ),
    DerivationRow(
        kind=EdgeKind.RT_HAS_PARAM,
        provider_or_field="signature.parameters iteration",
        join_keys=("sig_id",),
        id_recipe="edge_id = sha('RT_HAS_PARAM:'+sig_id+':'+param_id)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_CPG_EDGES,
    ),
    DerivationRow(
        kind=EdgeKind.RT_HAS_SIGNATURE,
        provider_or_field="inspect.signature(obj)",
        join_keys=("rt_id",),
        id_recipe="edge_id = sha('RT_HAS_SIGNATURE:'+rt_id+':'+sig_id)[:16]",
        confidence_policy="confidence=0.9",
        ambiguity_policy="none",
        template=DERIV_T_CPG_EDGES,
    ),
    DerivationRow(
        kind=EdgeKind.RT_WRAPS,
        provider_or_field="inspect.unwrap chain",
        join_keys=("rt_id",),
        id_recipe="edge_id = sha('RT_WRAPS:'+wrapper_id+':'+wrapped_id)[:16]",
        confidence_policy="confidence=0.9",
        ambiguity_policy="none",
        status="planned",
        template=DERIV_T_RUNTIME_OBJECTS,
    ),
    DerivationRow(
        kind=EdgeKind.SCIP_SYMBOL_DEFINITION,
        provider_or_field="scip_symbol_relationships (is_definition)",
        join_keys=(
            "symbol",
            "related_symbol",
        ),
        id_recipe=sha_id("SCIP_SYMBOL_DEFINITION", "symbol", "related_symbol"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_CPG_EDGES,
    ),
    DerivationRow(
        kind=EdgeKind.SCIP_SYMBOL_IMPLEMENTATION,
        provider_or_field="scip_symbol_relationships (is_implementation)",
        join_keys=(
            "symbol",
            "related_symbol",
        ),
        id_recipe=sha_id("SCIP_SYMBOL_IMPLEMENTATION", "symbol", "related_symbol"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_CPG_EDGES,
    ),
    DerivationRow(
        kind=EdgeKind.SCIP_SYMBOL_REFERENCE,
        provider_or_field="scip_symbol_relationships (is_reference)",
        join_keys=(
            "symbol",
            "related_symbol",
        ),
        id_recipe=sha_id("SCIP_SYMBOL_REFERENCE", "symbol", "related_symbol"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_CPG_EDGES,
    ),
    DerivationRow(
        kind=EdgeKind.SCIP_SYMBOL_TYPE_DEFINITION,
        provider_or_field="scip_symbol_relationships (is_type_definition)",
        join_keys=(
            "symbol",
            "related_symbol",
        ),
        id_recipe=sha_id("SCIP_SYMBOL_TYPE_DEFINITION", "symbol", "related_symbol"),
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_CPG_EDGES,
    ),
    DerivationRow(
        kind=EdgeKind.SCOPE_BINDS,
        provider_or_field="symtable symbols per scope -> PY_BINDING nodes",
        join_keys=("scope_id",),
        id_recipe="edge_id = sha('SCOPE_BINDS:'+scope_id+':'+binding_id)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        status="implemented",
        extractor="hamilton_pipeline.modules.cpg_build:symtable_bindings",
    ),
    DerivationRow(
        kind=EdgeKind.SCOPE_PARENT,
        provider_or_field="symtable table.get_children() recursion",
        join_keys=("child.scope_id -> parent.scope_id",),
        id_recipe="edge_id = sha('SCOPE_PARENT:'+child+':'+parent)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        template=DERIV_T_SYMTABLE,
        status="implemented",
        extractor="hamilton_pipeline.modules.cpg_build:symtable_scope_edges",
    ),
    DerivationRow(
        kind=EdgeKind.STEP_DEF,
        provider_or_field="opcode classifier -> def event",
        join_keys=(
            "instr_id",
            "binding_id",
        ),
        id_recipe="edge_id = sha('STEP_DEF:'+instr_id+':'+binding_id)[:16]",
        confidence_policy="confidence=0.8-0.95",
        ambiguity_policy="if binding ambiguous, keep multiple with scores",
        extractor="normalize.ibis_api:build_def_use_events",
        status="planned",
    ),
    DerivationRow(
        kind=EdgeKind.STEP_USE,
        provider_or_field="opcode classifier -> use event",
        join_keys=(
            "instr_id",
            "binding_id",
        ),
        id_recipe="edge_id = sha('STEP_USE:'+instr_id+':'+binding_id)[:16]",
        confidence_policy="confidence=0.8-0.95",
        ambiguity_policy="if binding ambiguous, keep multiple with scores",
        extractor="normalize.ibis_api:build_def_use_events",
        status="planned",
    ),
    DerivationRow(
        kind=EdgeKind.TS_PARENT_OF,
        provider_or_field="tree-sitter node.children",
        join_keys=("parent_ts_id -> child_ts_id",),
        id_recipe="edge_id = sha('TS_PARENT_OF:'+p+':'+c)[:16]",
        confidence_policy="confidence=1.0",
        ambiguity_policy="none",
        status="planned",
        extractor="extract.tree_sitter_extract:extract_ts_tables",
    ),
    DerivationRow(
        kind=EdgeKind.TYPE_PARAM_OF,
        provider_or_field="symtable TYPE_PARAMETERS meta scopes -> attach to owning def/class",
        join_keys=("type_param.scope_id -> owner.scope_id",),
        id_recipe="edge_id = sha('TYPE_PARAM_OF:'+type_param_id+':'+owner_id)[:16]",
        confidence_policy="confidence=0.8-0.95",
        ambiguity_policy="none",
        status="implemented",
        template=DERIV_T_SYMTABLE,
        extractor="hamilton_pipeline.modules.cpg_build:symtable_type_param_edges",
    ),
    DerivationRow(
        kind=EdgeKind.USE_SITE_OF,
        provider_or_field="LibCST use sites (Name spans) + expr_context + symtable resolution",
        join_keys=(
            "path",
            "bstart",
            "bend",
            "binding_id",
        ),
        id_recipe="edge_id = sha('USE_SITE_OF:'+use_site_id+':'+binding_id)[:16]",
        confidence_policy="confidence=0.85-0.95",
        ambiguity_policy="multiple edges allowed, winner selection by score",
        status="implemented",
        extractor="hamilton_pipeline.modules.cpg_build:symtable_use_sites",
    ),
)

EDGE_DERIVATIONS: dict[EdgeKind, list[DerivationSpec]] = build_derivations(EDGE_DERIVATION_ROWS)

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

    errs: list[str] = []
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


__all__ = [
    "EDGE_DERIVATIONS",
    "EDGE_KIND_CONTRACTS",
    "NODE_DERIVATIONS",
    "NODE_KIND_CONTRACTS",
    "PROP_ENUMS",
    "SCIP_ROLE_DEFINITION",
    "SCIP_ROLE_FORWARD_DEFINITION",
    "SCIP_ROLE_GENERATED",
    "SCIP_ROLE_IMPORT",
    "SCIP_ROLE_READ",
    "SCIP_ROLE_TEST",
    "SCIP_ROLE_WRITE",
    "CallableRef",
    "DerivationSpec",
    "DerivationStatus",
    "EdgeKind",
    "EdgeKindContract",
    "EntityKind",
    "NodeKind",
    "NodeKindContract",
    "PropPrimitive",
    "PropSpec",
    "SourceKind",
    "p_bool",
    "p_float",
    "p_int",
    "p_json",
    "p_str",
    "parse_extractor",
    "registry_to_jsonable",
    "validate_derivation_extractors",
    "validate_derivations_implemented_only",
    "validate_registry_completeness",
]

# Run completeness checks at import time (fail-fast).
validate_registry_completeness()
