"""DataFusion-native nested schemas for canonical datasets."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Literal, TypedDict

import pyarrow as pa

from arrowdsl.core.schema_constants import SCHEMA_META_NAME, SCHEMA_META_VERSION
from schema_spec.view_specs import ViewSpec

if TYPE_CHECKING:
    from datafusion import SessionContext

POS_T = pa.struct([("line0", pa.int32()), ("col", pa.int32())])

BYTE_SPAN_T = pa.struct([("byte_start", pa.int32()), ("byte_len", pa.int32())])

SPAN_T = pa.struct(
    [
        ("start", POS_T),
        ("end", POS_T),
        ("end_exclusive", pa.bool_()),
        ("col_unit", pa.string()),
        ("byte_span", BYTE_SPAN_T),
    ]
)

ATTRS_T = pa.map_(pa.string(), pa.string())

TOOL_INFO_T = pa.struct(
    [
        ("name", pa.string()),
        ("version", pa.string()),
        ("arguments", pa.list_(pa.string())),
    ]
)

SCIP_METADATA_T = pa.struct(
    [
        ("protocol_version", pa.int32()),
        ("tool_info", TOOL_INFO_T),
        ("project_root", pa.string()),
        ("text_document_encoding", pa.int32()),
    ]
)

SCIP_DIAGNOSTIC_T = pa.struct(
    [
        ("severity", pa.int32()),
        ("code", pa.string()),
        ("message", pa.string()),
        ("source", pa.string()),
        ("tags", pa.list_(pa.int32())),
    ]
)

SCIP_SIGNATURE_OCCURRENCE_T = pa.struct(
    [
        ("symbol", pa.string()),
        ("symbol_roles", pa.int32()),
        ("range", pa.list_(pa.int32())),
    ]
)

SCIP_SIGNATURE_DOCUMENTATION_T = pa.struct(
    [
        ("text", pa.string()),
        ("language", pa.string()),
        ("occurrences", pa.list_(SCIP_SIGNATURE_OCCURRENCE_T)),
    ]
)

SCIP_OCCURRENCE_T = pa.struct(
    [
        ("range_raw", pa.list_(pa.int32())),
        ("range", SPAN_T),
        ("symbol", pa.string()),
        ("symbol_roles", pa.int32()),
        ("override_documentation", pa.list_(pa.string())),
        ("syntax_kind", pa.int32()),
        ("diagnostics", pa.list_(SCIP_DIAGNOSTIC_T)),
        ("enclosing_range_raw", pa.list_(pa.int32())),
        ("enclosing_range", SPAN_T),
        ("attrs", ATTRS_T),
    ]
)

SCIP_RELATIONSHIP_T = pa.struct(
    [
        ("symbol", pa.string()),
        ("is_reference", pa.bool_()),
        ("is_implementation", pa.bool_()),
        ("is_type_definition", pa.bool_()),
        ("is_definition", pa.bool_()),
    ]
)

SCIP_SYMBOL_INFO_T = pa.struct(
    [
        ("symbol", pa.string()),
        ("documentation", pa.list_(pa.string())),
        ("signature_documentation", SCIP_SIGNATURE_DOCUMENTATION_T),
        ("relationships", pa.list_(SCIP_RELATIONSHIP_T)),
        ("kind", pa.int32()),
        ("display_name", pa.string()),
        ("enclosing_symbol", pa.string()),
        ("attrs", ATTRS_T),
    ]
)

SCIP_DOCUMENT_T = pa.struct(
    [
        ("relative_path", pa.string()),
        ("language", pa.string()),
        ("text", pa.string()),
        ("position_encoding", pa.int32()),
        ("occurrences", pa.list_(SCIP_OCCURRENCE_T)),
        ("symbols", pa.list_(SCIP_SYMBOL_INFO_T)),
        ("attrs", ATTRS_T),
    ]
)

SCIP_INDEX_SCHEMA = pa.schema(
    [
        ("index_id", pa.string()),
        ("metadata", SCIP_METADATA_T),
        ("documents", pa.list_(SCIP_DOCUMENT_T)),
        ("symbols", pa.list_(SCIP_SYMBOL_INFO_T)),
        ("external_symbols", pa.list_(SCIP_SYMBOL_INFO_T)),
    ]
)

CST_NODE_T = pa.struct(
    [
        ("cst_id", pa.int64()),
        ("kind", pa.string()),
        ("span", SPAN_T),
        ("span_ws", SPAN_T),
        ("attrs", ATTRS_T),
    ]
)

CST_EDGE_T = pa.struct(
    [
        ("src", pa.int64()),
        ("dst", pa.int64()),
        ("kind", pa.string()),
        ("slot", pa.string()),
        ("idx", pa.int32()),
        ("attrs", ATTRS_T),
    ]
)

QNAME_T = pa.struct([("name", pa.string()), ("source", pa.string())])
FQN_LIST = pa.list_(pa.string())

CST_PARSE_MANIFEST_T = pa.struct(
    [
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("encoding", pa.string()),
        ("default_indent", pa.string()),
        ("default_newline", pa.string()),
        ("has_trailing_newline", pa.bool_()),
        ("future_imports", pa.list_(pa.string())),
        ("module_name", pa.string()),
        ("package_name", pa.string()),
        ("libcst_version", pa.string()),
        ("parser_backend", pa.string()),
        ("parsed_python_version", pa.string()),
        ("schema_fingerprint", pa.string()),
    ]
)

CST_PARSE_ERROR_T = pa.struct(
    [
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("error_type", pa.string()),
        ("message", pa.string()),
        ("raw_line", pa.int64()),
        ("raw_column", pa.int64()),
        ("editor_line", pa.int64()),
        ("editor_column", pa.int64()),
        ("context", pa.string()),
        ("line_base", pa.int32()),
        ("col_unit", pa.string()),
        ("end_exclusive", pa.bool_()),
        ("meta", ATTRS_T),
    ]
)

CST_REF_T = pa.struct(
    [
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("ref_id", pa.string()),
        ("ref_kind", pa.string()),
        ("ref_text", pa.string()),
        ("expr_ctx", pa.string()),
        ("scope_type", pa.string()),
        ("scope_name", pa.string()),
        ("scope_role", pa.string()),
        ("parent_kind", pa.string()),
        ("inferred_type", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
    ]
)

CST_IMPORT_T = pa.struct(
    [
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("kind", pa.string()),
        ("module", pa.string()),
        ("relative_level", pa.int32()),
        ("name", pa.string()),
        ("asname", pa.string()),
        ("is_star", pa.bool_()),
        ("stmt_bstart", pa.int64()),
        ("stmt_bend", pa.int64()),
        ("alias_bstart", pa.int64()),
        ("alias_bend", pa.int64()),
    ]
)

CST_CALLSITE_T = pa.struct(
    [
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("call_id", pa.string()),
        ("call_bstart", pa.int64()),
        ("call_bend", pa.int64()),
        ("callee_bstart", pa.int64()),
        ("callee_bend", pa.int64()),
        ("callee_shape", pa.string()),
        ("callee_text", pa.string()),
        ("arg_count", pa.int32()),
        ("callee_dotted", pa.string()),
        ("callee_qnames", pa.list_(QNAME_T)),
        ("callee_fqns", FQN_LIST),
        ("inferred_type", pa.string()),
    ]
)

CST_DEF_T = pa.struct(
    [
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("def_id", pa.string()),
        ("container_def_kind", pa.string()),
        ("container_def_bstart", pa.int64()),
        ("container_def_bend", pa.int64()),
        ("kind", pa.string()),
        ("name", pa.string()),
        ("def_bstart", pa.int64()),
        ("def_bend", pa.int64()),
        ("name_bstart", pa.int64()),
        ("name_bend", pa.int64()),
        ("qnames", pa.list_(QNAME_T)),
        ("def_fqns", FQN_LIST),
        ("docstring", pa.string()),
        ("decorator_count", pa.int32()),
    ]
)

CST_TYPE_EXPR_T = pa.struct(
    [
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("owner_def_kind", pa.string()),
        ("owner_def_bstart", pa.int64()),
        ("owner_def_bend", pa.int64()),
        ("param_name", pa.string()),
        ("expr_kind", pa.string()),
        ("expr_role", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
        ("expr_text", pa.string()),
    ]
)

CST_DOCSTRING_T = pa.struct(
    [
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("owner_def_id", pa.string()),
        ("owner_kind", pa.string()),
        ("docstring", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
    ]
)

CST_DECORATOR_T = pa.struct(
    [
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("owner_def_id", pa.string()),
        ("owner_kind", pa.string()),
        ("decorator_text", pa.string()),
        ("decorator_index", pa.int32()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
    ]
)

CST_CALL_ARG_T = pa.struct(
    [
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("call_id", pa.string()),
        ("arg_index", pa.int32()),
        ("keyword", pa.string()),
        ("star", pa.string()),
        ("arg_text", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
    ]
)

LIBCST_FILES_SCHEMA = pa.schema(
    [
        ("repo", pa.string()),
        ("path", pa.string()),
        ("file_id", pa.string()),
        ("nodes", pa.list_(CST_NODE_T)),
        ("edges", pa.list_(CST_EDGE_T)),
        ("parse_manifest", pa.list_(CST_PARSE_MANIFEST_T)),
        ("parse_errors", pa.list_(CST_PARSE_ERROR_T)),
        ("refs", pa.list_(CST_REF_T)),
        ("imports", pa.list_(CST_IMPORT_T)),
        ("callsites", pa.list_(CST_CALLSITE_T)),
        ("defs", pa.list_(CST_DEF_T)),
        ("type_exprs", pa.list_(CST_TYPE_EXPR_T)),
        ("docstrings", pa.list_(CST_DOCSTRING_T)),
        ("decorators", pa.list_(CST_DECORATOR_T)),
        ("call_args", pa.list_(CST_CALL_ARG_T)),
        ("attrs", ATTRS_T),
    ]
)

AST_NODE_T = pa.struct(
    [
        ("ast_id", pa.int32()),
        ("parent_ast_id", pa.int32()),
        ("kind", pa.string()),
        ("name", pa.string()),
        ("value", pa.string()),
        ("span", SPAN_T),
        ("attrs", ATTRS_T),
    ]
)

AST_EDGE_T = pa.struct(
    [
        ("src", pa.int32()),
        ("dst", pa.int32()),
        ("kind", pa.string()),
        ("slot", pa.string()),
        ("idx", pa.int32()),
        ("attrs", ATTRS_T),
    ]
)

AST_ERROR_T = pa.struct(
    [
        ("error_type", pa.string()),
        ("message", pa.string()),
        ("span", SPAN_T),
        ("attrs", ATTRS_T),
    ]
)

AST_FILES_SCHEMA = pa.schema(
    [
        ("repo", pa.string()),
        ("path", pa.string()),
        ("file_id", pa.string()),
        ("nodes", pa.list_(AST_NODE_T)),
        ("edges", pa.list_(AST_EDGE_T)),
        ("errors", pa.list_(AST_ERROR_T)),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_FLAGS_T = pa.struct(
    [
        ("is_named", pa.bool_()),
        ("has_error", pa.bool_()),
        ("is_error", pa.bool_()),
        ("is_missing", pa.bool_()),
    ]
)

TREE_SITTER_NODE_T = pa.struct(
    [
        ("node_id", pa.string()),
        ("parent_id", pa.string()),
        ("kind", pa.string()),
        ("span", SPAN_T),
        ("flags", TREE_SITTER_FLAGS_T),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_ERROR_T = pa.struct(
    [
        ("error_id", pa.string()),
        ("node_id", pa.string()),
        ("span", SPAN_T),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_MISSING_T = pa.struct(
    [
        ("missing_id", pa.string()),
        ("node_id", pa.string()),
        ("span", SPAN_T),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_FILES_SCHEMA = pa.schema(
    [
        ("repo", pa.string()),
        ("path", pa.string()),
        ("file_id", pa.string()),
        ("nodes", pa.list_(TREE_SITTER_NODE_T)),
        ("errors", pa.list_(TREE_SITTER_ERROR_T)),
        ("missing", pa.list_(TREE_SITTER_MISSING_T)),
        ("attrs", ATTRS_T),
    ]
)

SYM_FLAGS_T = pa.struct(
    [
        ("is_referenced", pa.bool_()),
        ("is_imported", pa.bool_()),
        ("is_parameter", pa.bool_()),
        ("is_type_parameter", pa.bool_()),
        ("is_global", pa.bool_()),
        ("is_nonlocal", pa.bool_()),
        ("is_declared_global", pa.bool_()),
        ("is_local", pa.bool_()),
        ("is_annotated", pa.bool_()),
        ("is_free", pa.bool_()),
        ("is_assigned", pa.bool_()),
        ("is_namespace", pa.bool_()),
    ]
)

SYM_SYMBOL_T = pa.struct(
    [
        ("name", pa.string()),
        ("flags", SYM_FLAGS_T),
        ("namespace_count", pa.int32()),
        ("namespace_block_ids", pa.list_(pa.int64())),
        ("attrs", ATTRS_T),
    ]
)

SYM_FUNCTION_PARTITIONS_T = pa.struct(
    [
        ("parameters", pa.list_(pa.string())),
        ("locals", pa.list_(pa.string())),
        ("globals", pa.list_(pa.string())),
        ("nonlocals", pa.list_(pa.string())),
        ("frees", pa.list_(pa.string())),
    ]
)

SYM_BLOCK_T = pa.struct(
    [
        ("block_id", pa.int64()),
        ("parent_block_id", pa.int64()),
        ("block_type", pa.string()),
        ("name", pa.string()),
        ("lineno1", pa.int32()),
        ("span_hint", SPAN_T),
        ("scope_id", pa.string()),
        ("scope_local_id", pa.int64()),
        ("scope_type_value", pa.int32()),
        ("qualpath", pa.string()),
        ("function_partitions", SYM_FUNCTION_PARTITIONS_T),
        ("class_methods", pa.list_(pa.string())),
        ("symbols", pa.list_(SYM_SYMBOL_T)),
        ("attrs", ATTRS_T),
    ]
)

SYMTABLE_FILES_SCHEMA = pa.schema(
    [
        ("repo", pa.string()),
        ("path", pa.string()),
        ("file_id", pa.string()),
        ("blocks", pa.list_(SYM_BLOCK_T)),
        ("attrs", ATTRS_T),
    ]
)

BYTECODE_INSTR_T = pa.struct(
    [
        ("offset", pa.int32()),
        ("opname", pa.string()),
        ("opcode", pa.int32()),
        ("arg", pa.int32()),
        ("argrepr", pa.string()),
        ("is_jump_target", pa.bool_()),
        ("jump_target", pa.int32()),
        ("span", SPAN_T),
        ("attrs", ATTRS_T),
    ]
)

BYTECODE_EXCEPTION_T = pa.struct(
    [
        ("exc_index", pa.int32()),
        ("start_offset", pa.int32()),
        ("end_offset", pa.int32()),
        ("target_offset", pa.int32()),
        ("depth", pa.int32()),
        ("lasti", pa.bool_()),
        ("attrs", ATTRS_T),
    ]
)

BYTECODE_BLOCK_T = pa.struct(
    [
        ("start_offset", pa.int32()),
        ("end_offset", pa.int32()),
        ("kind", pa.string()),
        ("attrs", ATTRS_T),
    ]
)

BYTECODE_CFG_EDGE_T = pa.struct(
    [
        ("src_block_start", pa.int32()),
        ("src_block_end", pa.int32()),
        ("dst_block_start", pa.int32()),
        ("dst_block_end", pa.int32()),
        ("kind", pa.string()),
        ("edge_key", pa.string()),
        ("cond_instr_index", pa.int32()),
        ("cond_instr_offset", pa.int32()),
        ("exc_index", pa.int32()),
        ("attrs", ATTRS_T),
    ]
)

BYTECODE_ERROR_T = pa.struct(
    [
        ("error_type", pa.string()),
        ("message", pa.string()),
        ("attrs", ATTRS_T),
    ]
)

BYTECODE_CODE_OBJ_T = pa.struct(
    [
        ("code_id", pa.string()),
        ("qualname", pa.string()),
        ("name", pa.string()),
        ("firstlineno1", pa.int32()),
        ("argcount", pa.int32()),
        ("posonlyargcount", pa.int32()),
        ("kwonlyargcount", pa.int32()),
        ("nlocals", pa.int32()),
        ("flags", pa.int32()),
        ("stacksize", pa.int32()),
        ("code_len", pa.int32()),
        ("varnames", pa.list_(pa.string())),
        ("freevars", pa.list_(pa.string())),
        ("cellvars", pa.list_(pa.string())),
        ("names", pa.list_(pa.string())),
        ("consts_json", pa.string()),
        ("instructions", pa.list_(BYTECODE_INSTR_T)),
        ("exception_table", pa.list_(BYTECODE_EXCEPTION_T)),
        ("blocks", pa.list_(BYTECODE_BLOCK_T)),
        ("cfg_edges", pa.list_(BYTECODE_CFG_EDGE_T)),
        ("attrs", ATTRS_T),
    ]
)

BYTECODE_FILES_SCHEMA = pa.schema(
    [
        ("repo", pa.string()),
        ("path", pa.string()),
        ("file_id", pa.string()),
        ("code_objects", pa.list_(BYTECODE_CODE_OBJ_T)),
        ("errors", pa.list_(BYTECODE_ERROR_T)),
        ("attrs", ATTRS_T),
    ]
)


class NestedDatasetSpec(TypedDict):
    root: str
    path: str
    role: Literal["intrinsic", "derived"]
    context: dict[str, str]


def _schema_version_from_name(name: str) -> int | None:
    _, sep, suffix = name.rpartition("_v")
    if sep and suffix.isdigit():
        return int(suffix)
    return None


def _schema_with_metadata(name: str, schema: pa.Schema) -> pa.Schema:
    metadata = dict(schema.metadata or {})
    metadata[SCHEMA_META_NAME] = name.encode("utf-8")
    version = _schema_version_from_name(name)
    if version is not None:
        metadata[SCHEMA_META_VERSION] = str(version).encode("utf-8")
    return schema.with_metadata(metadata)


AST_FILES_SCHEMA = _schema_with_metadata("ast_files_v1", AST_FILES_SCHEMA)
BYTECODE_FILES_SCHEMA = _schema_with_metadata("bytecode_files_v1", BYTECODE_FILES_SCHEMA)
LIBCST_FILES_SCHEMA = _schema_with_metadata("libcst_files_v1", LIBCST_FILES_SCHEMA)
SCIP_INDEX_SCHEMA = _schema_with_metadata("scip_index_v1", SCIP_INDEX_SCHEMA)
SYMTABLE_FILES_SCHEMA = _schema_with_metadata("symtable_files_v1", SYMTABLE_FILES_SCHEMA)
TREE_SITTER_FILES_SCHEMA = _schema_with_metadata("tree_sitter_files_v1", TREE_SITTER_FILES_SCHEMA)

DATAFUSION_FALLBACKS_SCHEMA = _schema_with_metadata(
    "datafusion_fallbacks_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("reason", pa.string(), nullable=False),
            pa.field("error", pa.string(), nullable=False),
            pa.field("expression_type", pa.string(), nullable=False),
            pa.field("sql", pa.string(), nullable=False),
            pa.field("dialect", pa.string(), nullable=False),
            pa.field("policy_violations", pa.list_(pa.string()), nullable=True),
            pa.field("sql_policy_name", pa.string(), nullable=True),
            pa.field("param_mode", pa.string(), nullable=True),
        ]
    ),
)

DATAFUSION_EXPLAINS_SCHEMA = _schema_with_metadata(
    "datafusion_explains_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("sql", pa.string(), nullable=False),
            pa.field("explain_rows_artifact_path", pa.string(), nullable=True),
            pa.field("explain_rows_artifact_format", pa.string(), nullable=True),
            pa.field("explain_rows_schema_fingerprint", pa.string(), nullable=True),
            pa.field("explain_analyze", pa.bool_(), nullable=False),
        ]
    ),
)

DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SCHEMA = _schema_with_metadata(
    "datafusion_schema_registry_validation_v1",
    pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64(), nullable=False),
            pa.field("schema_name", pa.string(), nullable=False),
            pa.field("issue_type", pa.string(), nullable=False),
            pa.field("detail", pa.string(), nullable=True),
        ]
    ),
)

FEATURE_STATE_SCHEMA = _schema_with_metadata(
    "feature_state_v1",
    pa.schema(
        [
            pa.field("profile_name", pa.string(), nullable=False),
            pa.field("determinism_tier", pa.string(), nullable=False),
            pa.field("dynamic_filters_enabled", pa.bool_(), nullable=False),
            pa.field("spill_enabled", pa.bool_(), nullable=False),
            pa.field("named_args_supported", pa.bool_(), nullable=False),
        ]
    ),
)

REPO_SNAPSHOT_SCHEMA = _schema_with_metadata(
    "repo_snapshot_v1",
    pa.schema(
        [
            pa.field("file_id", pa.string()),
            pa.field("path", pa.string()),
            pa.field("file_sha256", pa.string()),
            pa.field("size_bytes", pa.int64()),
            pa.field("mtime_ns", pa.int64()),
        ]
    ),
)

SCHEMA_REGISTRY: dict[str, pa.Schema] = {
    "ast_files_v1": AST_FILES_SCHEMA,
    "bytecode_files_v1": BYTECODE_FILES_SCHEMA,
    "datafusion_explains_v1": DATAFUSION_EXPLAINS_SCHEMA,
    "datafusion_fallbacks_v1": DATAFUSION_FALLBACKS_SCHEMA,
    "datafusion_schema_registry_validation_v1": DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SCHEMA,
    "feature_state_v1": FEATURE_STATE_SCHEMA,
    "libcst_files_v1": LIBCST_FILES_SCHEMA,
    "repo_snapshot_v1": REPO_SNAPSHOT_SCHEMA,
    "scip_index_v1": SCIP_INDEX_SCHEMA,
    "symtable_files_v1": SYMTABLE_FILES_SCHEMA,
    "tree_sitter_files_v1": TREE_SITTER_FILES_SCHEMA,
}

NESTED_DATASET_INDEX: dict[str, NestedDatasetSpec] = {
    "cst_nodes": {
        "root": "libcst_files_v1",
        "path": "nodes",
        "role": "intrinsic",
        "context": {},
    },
    "cst_edges": {
        "root": "libcst_files_v1",
        "path": "edges",
        "role": "intrinsic",
        "context": {},
    },
    "cst_parse_manifest": {
        "root": "libcst_files_v1",
        "path": "parse_manifest",
        "role": "intrinsic",
        "context": {},
    },
    "cst_parse_errors": {
        "root": "libcst_files_v1",
        "path": "parse_errors",
        "role": "derived",
        "context": {},
    },
    "cst_refs": {
        "root": "libcst_files_v1",
        "path": "refs",
        "role": "derived",
        "context": {},
    },
    "cst_imports": {
        "root": "libcst_files_v1",
        "path": "imports",
        "role": "derived",
        "context": {},
    },
    "cst_callsites": {
        "root": "libcst_files_v1",
        "path": "callsites",
        "role": "derived",
        "context": {},
    },
    "cst_defs": {
        "root": "libcst_files_v1",
        "path": "defs",
        "role": "derived",
        "context": {},
    },
    "cst_type_exprs": {
        "root": "libcst_files_v1",
        "path": "type_exprs",
        "role": "derived",
        "context": {},
    },
    "cst_docstrings": {
        "root": "libcst_files_v1",
        "path": "docstrings",
        "role": "derived",
        "context": {},
    },
    "cst_decorators": {
        "root": "libcst_files_v1",
        "path": "decorators",
        "role": "derived",
        "context": {},
    },
    "cst_call_args": {
        "root": "libcst_files_v1",
        "path": "call_args",
        "role": "derived",
        "context": {},
    },
    "ast_nodes": {
        "root": "ast_files_v1",
        "path": "nodes",
        "role": "derived",
        "context": {},
    },
    "ast_edges": {
        "root": "ast_files_v1",
        "path": "edges",
        "role": "derived",
        "context": {},
    },
    "ast_errors": {
        "root": "ast_files_v1",
        "path": "errors",
        "role": "derived",
        "context": {},
    },
    "ts_nodes": {
        "root": "tree_sitter_files_v1",
        "path": "nodes",
        "role": "derived",
        "context": {},
    },
    "ts_errors": {
        "root": "tree_sitter_files_v1",
        "path": "errors",
        "role": "derived",
        "context": {},
    },
    "ts_missing": {
        "root": "tree_sitter_files_v1",
        "path": "missing",
        "role": "derived",
        "context": {},
    },
    "symtable_scopes": {
        "root": "symtable_files_v1",
        "path": "blocks",
        "role": "derived",
        "context": {},
    },
    "symtable_symbols": {
        "root": "symtable_files_v1",
        "path": "blocks.symbols",
        "role": "derived",
        "context": {"block_id": "blocks.block_id", "scope_id": "blocks.scope_id"},
    },
    "symtable_scope_edges": {
        "root": "symtable_files_v1",
        "path": "blocks",
        "role": "derived",
        "context": {},
    },
    "scip_metadata": {
        "root": "scip_index_v1",
        "path": "metadata",
        "role": "derived",
        "context": {},
    },
    "scip_documents": {
        "root": "scip_index_v1",
        "path": "documents",
        "role": "derived",
        "context": {},
    },
    "scip_occurrences": {
        "root": "scip_index_v1",
        "path": "documents.occurrences",
        "role": "derived",
        "context": {"relative_path": "documents.relative_path"},
    },
    "scip_symbol_information": {
        "root": "scip_index_v1",
        "path": "symbols",
        "role": "derived",
        "context": {},
    },
    "scip_external_symbol_information": {
        "root": "scip_index_v1",
        "path": "external_symbols",
        "role": "derived",
        "context": {},
    },
    "scip_symbol_relationships": {
        "root": "scip_index_v1",
        "path": "symbols.relationships",
        "role": "derived",
        "context": {"parent_symbol": "symbols.symbol"},
    },
    "scip_diagnostics": {
        "root": "scip_index_v1",
        "path": "documents.occurrences.diagnostics",
        "role": "derived",
        "context": {
            "relative_path": "documents.relative_path",
            "occ_range": "documents.occurrences.range",
        },
    },
    "py_bc_code_units": {
        "root": "bytecode_files_v1",
        "path": "code_objects",
        "role": "derived",
        "context": {},
    },
    "py_bc_instructions": {
        "root": "bytecode_files_v1",
        "path": "code_objects.instructions",
        "role": "derived",
        "context": {"code_id": "code_objects.code_id"},
    },
    "py_bc_blocks": {
        "root": "bytecode_files_v1",
        "path": "code_objects.blocks",
        "role": "derived",
        "context": {"code_id": "code_objects.code_id"},
    },
    "py_bc_cfg_edges": {
        "root": "bytecode_files_v1",
        "path": "code_objects.cfg_edges",
        "role": "derived",
        "context": {"code_id": "code_objects.code_id"},
    },
    "bytecode_exception_table": {
        "root": "bytecode_files_v1",
        "path": "code_objects.exception_table",
        "role": "derived",
        "context": {"code_id": "code_objects.code_id"},
    },
    "bytecode_errors": {
        "root": "bytecode_files_v1",
        "path": "errors",
        "role": "derived",
        "context": {},
    },
}

ROOT_IDENTITY_FIELDS: dict[str, tuple[str, ...]] = {
    "ast_files_v1": ("file_id", "path"),
    "bytecode_files_v1": ("file_id", "path"),
    "libcst_files_v1": ("file_id", "path"),
    "scip_index_v1": ("index_id",),
    "symtable_files_v1": ("file_id", "path"),
    "tree_sitter_files_v1": ("file_id", "path"),
}

SYMTABLE_VIEW_NAMES: tuple[str, ...] = (
    "symtable_scopes",
    "symtable_symbols",
    "symtable_scope_edges",
    "symtable_namespace_edges",
    "symtable_function_partitions",
    "symtable_class_methods",
    "symtable_symbol_attrs",
)


def nested_dataset_names() -> tuple[str, ...]:
    """Return nested dataset names in sorted order.

    Returns
    -------
    tuple[str, ...]
        Sorted nested dataset name tuple.
    """
    return tuple(sorted(NESTED_DATASET_INDEX))


def nested_schema_names() -> tuple[str, ...]:
    """Return intrinsic nested dataset names in sorted order.

    Returns
    -------
    tuple[str, ...]
        Sorted intrinsic nested dataset name tuple.
    """
    return tuple(
        sorted(name for name, spec in NESTED_DATASET_INDEX.items() if spec["role"] == "intrinsic")
    )


def nested_spec_for(name: str) -> NestedDatasetSpec:
    """Return the nested dataset spec for a name.

    Returns
    -------
    NestedDatasetSpec
        Nested dataset specification mapping.

    Raises
    ------
    KeyError
        Raised when the dataset name is not registered.
    """
    spec = NESTED_DATASET_INDEX.get(name)
    if spec is None:
        msg = f"Unknown nested dataset: {name!r}."
        raise KeyError(msg)
    return spec


def datasets_for_path(root: str, path: str) -> tuple[str, ...]:
    """Return dataset names registered for a root/path pair.

    Returns
    -------
    tuple[str, ...]
        Sorted dataset name tuple for the path.
    """
    return tuple(
        sorted(
            name
            for name, spec in NESTED_DATASET_INDEX.items()
            if spec["root"] == root and spec["path"] == path
        )
    )


def nested_path_for(name: str) -> tuple[str, str]:
    """Return the root schema name and path for a nested dataset.

    Returns
    -------
    tuple[str, str]
        Root schema name and nested path.
    """
    spec = nested_spec_for(name)
    return spec["root"], spec["path"]


def nested_context_for(name: str) -> Mapping[str, str]:
    """Return the context fields for a nested dataset.

    Returns
    -------
    Mapping[str, str]
        Mapping of output column name to nested context path.
    """
    return nested_spec_for(name)["context"]


def nested_role_for(name: str) -> Literal["intrinsic", "derived"]:
    """Return the nested dataset role for a name.

    Returns
    -------
    Literal["intrinsic", "derived"]
        Dataset role label.
    """
    return nested_spec_for(name)["role"]


def is_nested_dataset(name: str) -> bool:
    """Return whether a name is a known nested dataset.

    Returns
    -------
    bool
        ``True`` when the dataset name is registered.
    """
    return name in NESTED_DATASET_INDEX


def is_intrinsic_nested_dataset(name: str) -> bool:
    """Return whether a nested dataset is intrinsic.

    Returns
    -------
    bool
        ``True`` when the dataset is intrinsic.
    """
    spec = NESTED_DATASET_INDEX.get(name)
    return spec is not None and spec["role"] == "intrinsic"


def _is_list_type(dtype: pa.DataType) -> bool:
    return pa.types.is_list(dtype) or pa.types.is_large_list(dtype)


def _field_from_container(container: pa.Schema | pa.StructType, name: str) -> pa.Field:
    try:
        return container.field(name)
    except KeyError as exc:
        msg = f"Unknown nested field: {name!r}."
        raise KeyError(msg) from exc


def struct_for_path(schema: pa.Schema, path: str) -> pa.StructType:
    """Resolve the struct type for a nested path.

    Returns
    -------
    pyarrow.StructType
        Struct type resolved at the nested path.

    Raises
    ------
    TypeError
        Raised when the path does not resolve to a struct.
    ValueError
        Raised when the path is empty.
    """
    if not path:
        msg = "Nested schema path cannot be empty."
        raise ValueError(msg)
    current: pa.Schema | pa.StructType = schema
    for step in path.split("."):
        field = _field_from_container(current, step)
        dtype = field.type
        if _is_list_type(dtype):
            dtype = dtype.value_type
        if not pa.types.is_struct(dtype):
            msg = f"Nested path {path!r} does not resolve to a struct at {step!r}."
            raise TypeError(msg)
        current = dtype
    if isinstance(current, pa.StructType):
        return current
    msg = f"Nested path {path!r} did not resolve to a struct."
    raise TypeError(msg)


def _context_fields_for(name: str, root_schema: pa.Schema) -> tuple[pa.Field, ...]:
    context = nested_context_for(name)
    if not context:
        return ()
    fields: list[pa.Field] = []
    for alias, path in context.items():
        prefix, sep, field_name = path.rpartition(".")
        if not sep:
            field = root_schema.field(field_name)
        else:
            struct_type = struct_for_path(root_schema, prefix)
            field = struct_type.field(field_name)
        fields.append(pa.field(alias, field.type, field.nullable))
    return tuple(fields)


def identity_fields_for(
    name: str,
    root_schema: pa.Schema,
    row_struct: pa.StructType,
) -> tuple[str, ...]:
    """Return identity column names for a nested dataset.

    Returns
    -------
    tuple[str, ...]
        Identity column names for the dataset.
    """
    root, _ = nested_path_for(name)
    identities = ROOT_IDENTITY_FIELDS.get(root, ())
    row_fields = {field.name for field in row_struct}
    resolved: list[str] = []
    for field_name in identities:
        if field_name in row_fields:
            continue
        root_schema.field(field_name)
        resolved.append(field_name)
    return tuple(resolved)


def nested_schema_for(name: str, *, allow_derived: bool = False) -> pa.Schema:
    """Return the schema for an intrinsic nested dataset.

    Returns
    -------
    pyarrow.Schema
        Derived nested schema for the dataset.

    Raises
    ------
    KeyError
        Raised when the dataset is not intrinsic unless allow_derived is True.
    """
    if not allow_derived and not is_intrinsic_nested_dataset(name):
        msg = f"Nested schema {name!r} is derived; set allow_derived=True to resolve."
        raise KeyError(msg)
    root, path = nested_path_for(name)
    root_schema = SCHEMA_REGISTRY[root]
    row_struct = struct_for_path(root_schema, path)
    identity_fields = identity_fields_for(name, root_schema, row_struct)
    context_fields = _context_fields_for(name, root_schema)
    fields: list[pa.Field] = []
    seen: set[str] = set()
    for field_name in identity_fields:
        field = root_schema.field(field_name)
        fields.append(pa.field(field.name, field.type, field.nullable))
        seen.add(field.name)
    for field in context_fields:
        if field.name in seen:
            continue
        fields.append(field)
        seen.add(field.name)
    for field in row_struct:
        if field.name in seen:
            continue
        fields.append(pa.field(field.name, field.type, field.nullable))
        seen.add(field.name)
    return pa.schema(fields)


def _append_selection(
    selections: list[str],
    selected_names: set[str],
    *,
    name: str,
    expr: str,
) -> None:
    if name in selected_names:
        return
    selections.append(f"{expr} AS {name}")
    selected_names.add(name)


def _context_expr(
    *,
    root_alias: str,
    prefix_exprs: Mapping[str, str],
    ctx_path: str,
    dataset_name: str,
) -> str:
    prefix, sep, field_name = ctx_path.rpartition(".")
    if not sep:
        return f"{root_alias}.{field_name}"
    prefix_expr = prefix_exprs.get(prefix)
    if prefix_expr is None:
        msg = f"Nested context path {ctx_path!r} not resolved for {dataset_name!r}."
        raise KeyError(msg)
    return f"{prefix_expr}['{field_name}']"


def _resolve_nested_path(
    *,
    root_schema: pa.Schema,
    path: str,
    root_alias: str,
    table: str,
) -> tuple[str, str, pa.StructType, dict[str, str]]:
    from_clause = f"FROM {table} AS {root_alias}"
    current_expr = root_alias
    current_is_root = True
    current_struct: pa.Schema | pa.StructType = root_schema
    prefix_exprs: dict[str, str] = {}
    parts = path.split(".")
    for idx, step in enumerate(parts):
        field = _field_from_container(current_struct, step)
        dtype = field.type
        prefix = ".".join(parts[: idx + 1])
        if _is_list_type(dtype):
            list_expr = f"{current_expr}.{step}" if current_is_root else f"{current_expr}['{step}']"
            alias = f"n{idx}"
            from_clause += f"\nCROSS JOIN unnest({list_expr}) AS {alias}"
            current_expr = alias
            current_is_root = False
            value_type = dtype.value_type
            if not pa.types.is_struct(value_type):
                msg = f"Nested path {path!r} does not resolve to a struct at {step!r}."
                raise TypeError(msg)
            current_struct = value_type
            prefix_exprs[prefix] = alias
            continue
        if not pa.types.is_struct(dtype):
            msg = f"Nested path {path!r} does not resolve to a struct at {step!r}."
            raise TypeError(msg)
        current_expr = f"{current_expr}.{step}" if current_is_root else f"{current_expr}['{step}']"
        current_is_root = False
        current_struct = dtype
        prefix_exprs[prefix] = current_expr
    if not isinstance(current_struct, pa.StructType):
        msg = f"Nested path {path!r} did not resolve to a struct."
        raise TypeError(msg)
    return from_clause, current_expr, current_struct, prefix_exprs


def nested_base_sql(name: str, *, table: str | None = None) -> str:
    """Return base SQL for a nested dataset path.

    Returns
    -------
    str
        Base SQL query string.
    """
    root, path = nested_path_for(name)
    root_schema = SCHEMA_REGISTRY[root]
    root_alias = "root"
    resolved_table = table or root
    from_clause, row_expr, row_struct, prefix_exprs = _resolve_nested_path(
        root_schema=root_schema,
        path=path,
        root_alias=root_alias,
        table=resolved_table,
    )
    identity_fields = identity_fields_for(name, root_schema, row_struct)
    context_fields = nested_context_for(name)
    selections: list[str] = []
    selected_names: set[str] = set()
    for field_name in identity_fields:
        _append_selection(
            selections,
            selected_names,
            name=field_name,
            expr=f"{root_alias}.{field_name}",
        )
    for alias, ctx_path in context_fields.items():
        expr = _context_expr(
            root_alias=root_alias,
            prefix_exprs=prefix_exprs,
            ctx_path=ctx_path,
            dataset_name=name,
        )
        _append_selection(selections, selected_names, name=alias, expr=expr)
    for field in row_struct:
        _append_selection(
            selections,
            selected_names,
            name=field.name,
            expr=f"{row_expr}['{field.name}']",
        )
    select_sql = ",\n      ".join(selections)
    return f"SELECT\n      {select_sql}\n{from_clause}"


def nested_view_spec(name: str, *, table: str | None = None) -> ViewSpec:
    """Return a ViewSpec for a nested dataset.

    Returns
    -------
    ViewSpec
        View specification with schema and base SQL.
    """
    schema = nested_schema_for(name, allow_derived=True)
    sql = nested_base_sql(name, table=table)
    return ViewSpec(name=name, sql=sql, schema=schema)


def nested_view_specs(*, table: str | None = None) -> tuple[ViewSpec, ...]:
    """Return ViewSpecs for all nested datasets.

    Returns
    -------
    tuple[ViewSpec, ...]
        View specifications for nested datasets.
    """
    return tuple(nested_view_spec(name, table=table) for name in nested_dataset_names())


def validate_schema_metadata(schema: pa.Schema) -> None:
    """Validate required schema metadata tags.

    Raises
    ------
    ValueError
        Raised when required schema metadata is missing.
    """
    meta = schema.metadata or {}
    if SCHEMA_META_NAME not in meta:
        msg = "Schema metadata missing schema_name."
        raise ValueError(msg)
    if SCHEMA_META_VERSION not in meta:
        msg = "Schema metadata missing schema_version."
        raise ValueError(msg)


def validate_nested_types(ctx: SessionContext, name: str) -> None:
    """Validate nested dataset types using DataFusion arrow_typeof."""
    ctx.sql(f"SELECT arrow_typeof(*) AS row_type FROM {name} LIMIT 1").collect()


def validate_symtable_views(ctx: SessionContext) -> None:
    """Validate symtable view schemas using DataFusion introspection."""
    errors: dict[str, str] = {}
    for name in SYMTABLE_VIEW_NAMES:
        try:
            ctx.sql(f"DESCRIBE SELECT * FROM {name}").collect()
        except (RuntimeError, TypeError, ValueError) as exc:
            errors[name] = str(exc)
    try:
        ctx.sql(
            "SELECT arrow_typeof(blocks) AS blocks_type FROM symtable_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_typeof(blocks.symbols) AS symbols_type FROM symtable_files_v1 LIMIT 1"
        ).collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["symtable_files_v1"] = str(exc)
    if errors:
        msg = f"Symtable view validation failed: {errors}."
        raise ValueError(msg)


def registered_table_names(ctx: SessionContext) -> set[str]:
    """Return registered table names from information_schema.

    Returns
    -------
    set[str]
        Set of table names present in the session catalog.
    """
    table = ctx.sql("SELECT table_name FROM information_schema.tables").to_arrow_table()
    if "table_name" not in table.column_names:
        return set()
    return {str(name) for name in table["table_name"].to_pylist() if name is not None}


def missing_schema_names(
    ctx: SessionContext,
    *,
    expected: Sequence[str] | None = None,
) -> tuple[str, ...]:
    """Return missing schema names from information_schema.

    Returns
    -------
    tuple[str, ...]
        Sorted tuple of missing schema names.
    """
    expected_names = set(expected or schema_names())
    registered = registered_table_names(ctx)
    missing = sorted(expected_names - registered)
    return tuple(missing)


def schema_registry() -> Mapping[str, pa.Schema]:
    """Return the registered DataFusion schemas.

    Returns
    -------
    Mapping[str, pyarrow.Schema]
        Mapping of schema names to Arrow schemas.
    """
    return dict(SCHEMA_REGISTRY)


def schema_names() -> tuple[str, ...]:
    """Return registered schema names in sorted order.

    Returns
    -------
    tuple[str, ...]
        Sorted schema name tuple.
    """
    return tuple(sorted({*SCHEMA_REGISTRY, *nested_schema_names()}))


def has_schema(name: str) -> bool:
    """Return whether a schema is registered.

    Returns
    -------
    bool
        ``True`` when the schema name is registered.
    """
    return name in SCHEMA_REGISTRY or is_intrinsic_nested_dataset(name)


def schema_for(name: str) -> pa.Schema:
    """Return the schema for a registered dataset name.

    Returns
    -------
    pyarrow.Schema
        Registered schema instance.

    Raises
    ------
    KeyError
        Raised when the schema name is not registered.
    """
    schema = SCHEMA_REGISTRY.get(name)
    if schema is not None:
        return schema
    if is_intrinsic_nested_dataset(name):
        return nested_schema_for(name)
    msg = f"Unknown DataFusion schema: {name!r}."
    raise KeyError(msg)


def register_schema(ctx: SessionContext, name: str, schema: pa.Schema) -> None:
    """Register a schema in a DataFusion SessionContext using an empty batch."""
    arrays = [pa.array([], type=field.type) for field in schema]
    batch = pa.record_batch(arrays, schema=schema)
    ctx.register_record_batches(name, [[batch]])


def register_all_schemas(ctx: SessionContext) -> None:
    """Register all canonical schemas in a DataFusion SessionContext."""
    for name, schema in SCHEMA_REGISTRY.items():
        validate_schema_metadata(schema)
        register_schema(ctx, name, schema)
    for name in nested_schema_names():
        register_schema(ctx, name, nested_schema_for(name))


__all__ = [
    "AST_FILES_SCHEMA",
    "BYTECODE_FILES_SCHEMA",
    "LIBCST_FILES_SCHEMA",
    "NESTED_DATASET_INDEX",
    "SCHEMA_REGISTRY",
    "SCIP_INDEX_SCHEMA",
    "SYMTABLE_FILES_SCHEMA",
    "SYMTABLE_VIEW_NAMES",
    "TREE_SITTER_FILES_SCHEMA",
    "datasets_for_path",
    "has_schema",
    "identity_fields_for",
    "is_intrinsic_nested_dataset",
    "is_nested_dataset",
    "missing_schema_names",
    "nested_base_sql",
    "nested_context_for",
    "nested_dataset_names",
    "nested_path_for",
    "nested_role_for",
    "nested_schema_for",
    "nested_schema_names",
    "nested_view_spec",
    "nested_view_specs",
    "register_all_schemas",
    "register_schema",
    "registered_table_names",
    "schema_for",
    "schema_names",
    "schema_registry",
    "struct_for_path",
    "validate_nested_types",
    "validate_schema_metadata",
    "validate_symtable_views",
]
