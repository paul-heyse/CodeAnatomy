"""DataFusion-native nested schemas for canonical datasets."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Literal, TypedDict

import pyarrow as pa

from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.core.schema_constants import (
    DEFAULT_VALUE_META,
    KEY_FIELDS_META,
    REQUIRED_NON_NULL_META,
    SCHEMA_META_NAME,
    SCHEMA_META_VERSION,
)
from arrowdsl.schema.metadata import ordering_metadata_spec
from datafusion_engine.schema_introspection import SchemaIntrospector
from registry_common.metadata import metadata_list_bytes
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
_DEFAULT_ATTRS_META: dict[bytes, bytes] = {DEFAULT_VALUE_META: b"{}"}


def _attrs_field(name: str = "attrs") -> pa.Field:
    return pa.field(name, ATTRS_T, metadata=_DEFAULT_ATTRS_META)


SCIP_METADATA_SCHEMA = pa.schema(
    [
        ("index_id", pa.string()),
        ("protocol_version", pa.int32()),
        ("tool_name", pa.string()),
        ("tool_version", pa.string()),
        ("tool_arguments", pa.list_(pa.string())),
        ("project_root", pa.string()),
        ("text_document_encoding", pa.int32()),
        ("project_name", pa.string()),
        ("project_version", pa.string()),
        ("project_namespace", pa.string()),
    ]
)

SCIP_INDEX_STATS_SCHEMA = pa.schema(
    [
        ("index_id", pa.string()),
        ("document_count", pa.int64()),
        ("occurrence_count", pa.int64()),
        ("diagnostic_count", pa.int64()),
        ("symbol_count", pa.int64()),
        ("external_symbol_count", pa.int64()),
        ("missing_position_encoding_count", pa.int64()),
        ("document_text_count", pa.int64()),
        ("document_text_bytes", pa.int64()),
    ]
)

SCIP_DOCUMENTS_SCHEMA = pa.schema(
    [
        ("index_id", pa.string()),
        ("document_id", pa.string()),
        ("path", pa.string()),
        ("language", pa.string()),
        ("position_encoding", pa.int32()),
    ]
)

SCIP_DOCUMENT_TEXTS_SCHEMA = pa.schema(
    [
        ("document_id", pa.string()),
        ("path", pa.string()),
        ("text", pa.string()),
    ]
)

SCIP_SYMBOL_INFO_FIELDS: tuple[pa.Field, ...] = (
    pa.field("symbol", pa.string()),
    pa.field("display_name", pa.string()),
    pa.field("kind", pa.int32()),
    pa.field("kind_name", pa.string()),
    pa.field("enclosing_symbol", pa.string()),
    pa.field("documentation", pa.list_(pa.string())),
    pa.field("signature_text", pa.string()),
    pa.field("signature_language", pa.string()),
)

SCIP_SYMBOL_INFORMATION_SCHEMA = pa.schema(SCIP_SYMBOL_INFO_FIELDS)

SCIP_DOCUMENT_SYMBOLS_SCHEMA = pa.schema(
    [
        ("document_id", pa.string()),
        ("path", pa.string()),
        *SCIP_SYMBOL_INFO_FIELDS,
    ]
)

SCIP_EXTERNAL_SYMBOL_INFORMATION_SCHEMA = pa.schema(SCIP_SYMBOL_INFO_FIELDS)

SCIP_SYMBOL_RELATIONSHIPS_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("related_symbol", pa.string()),
        ("is_reference", pa.bool_()),
        ("is_implementation", pa.bool_()),
        ("is_type_definition", pa.bool_()),
        ("is_definition", pa.bool_()),
    ]
)

SCIP_SIGNATURE_OCCURRENCES_SCHEMA = pa.schema(
    [
        ("parent_symbol", pa.string()),
        ("symbol", pa.string()),
        ("symbol_roles", pa.int32()),
        ("syntax_kind", pa.int32()),
        ("syntax_kind_name", pa.string()),
        ("range_raw", pa.list_(pa.int32())),
        ("start_line", pa.int32()),
        ("start_char", pa.int32()),
        ("end_line", pa.int32()),
        ("end_char", pa.int32()),
        ("range_len", pa.int32()),
        ("line_base", pa.int32()),
        ("col_unit", pa.string()),
        ("end_exclusive", pa.bool_()),
        ("is_definition", pa.bool_()),
        ("is_import", pa.bool_()),
        ("is_write", pa.bool_()),
        ("is_read", pa.bool_()),
        ("is_generated", pa.bool_()),
        ("is_test", pa.bool_()),
        ("is_forward_definition", pa.bool_()),
    ]
)

SCIP_OCCURRENCES_SCHEMA = pa.schema(
    [
        ("document_id", pa.string()),
        ("path", pa.string()),
        ("symbol", pa.string()),
        ("symbol_roles", pa.int32()),
        ("syntax_kind", pa.int32()),
        ("syntax_kind_name", pa.string()),
        ("override_documentation", pa.list_(pa.string())),
        ("range_raw", pa.list_(pa.int32())),
        ("enclosing_range_raw", pa.list_(pa.int32())),
        ("start_line", pa.int32()),
        ("start_char", pa.int32()),
        ("end_line", pa.int32()),
        ("end_char", pa.int32()),
        ("range_len", pa.int32()),
        ("enc_start_line", pa.int32()),
        ("enc_start_char", pa.int32()),
        ("enc_end_line", pa.int32()),
        ("enc_end_char", pa.int32()),
        ("enc_range_len", pa.int32()),
        ("line_base", pa.int32()),
        ("col_unit", pa.string()),
        ("end_exclusive", pa.bool_()),
        ("is_definition", pa.bool_()),
        ("is_import", pa.bool_()),
        ("is_write", pa.bool_()),
        ("is_read", pa.bool_()),
        ("is_generated", pa.bool_()),
        ("is_test", pa.bool_()),
        ("is_forward_definition", pa.bool_()),
    ]
)

SCIP_DIAGNOSTICS_SCHEMA = pa.schema(
    [
        ("document_id", pa.string()),
        ("path", pa.string()),
        ("severity", pa.int32()),
        ("code", pa.string()),
        ("message", pa.string()),
        ("source", pa.string()),
        ("tags", pa.list_(pa.int32())),
        ("start_line", pa.int32()),
        ("start_char", pa.int32()),
        ("end_line", pa.int32()),
        ("end_char", pa.int32()),
        ("line_base", pa.int32()),
        ("col_unit", pa.string()),
        ("end_exclusive", pa.bool_()),
    ]
)

CST_NODE_T = pa.struct(
    [
        pa.field("cst_id", pa.int64(), nullable=False),
        ("kind", pa.string()),
        ("span", SPAN_T),
        ("span_ws", SPAN_T),
        _attrs_field(),
    ]
)

CST_EDGE_T = pa.struct(
    [
        pa.field("src", pa.int64(), nullable=False),
        pa.field("dst", pa.int64(), nullable=False),
        ("kind", pa.string()),
        ("slot", pa.string()),
        ("idx", pa.int32()),
        _attrs_field(),
    ]
)

QNAME_T = pa.struct([("name", pa.string()), ("source", pa.string())])
FQN_LIST = pa.list_(pa.string())

CST_PARSE_MANIFEST_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
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
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
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
        _attrs_field("meta"),
    ]
)

CST_REF_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        pa.field("ref_id", pa.string(), nullable=False),
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
        _attrs_field(),
    ]
)

CST_IMPORT_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
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
        _attrs_field(),
    ]
)

CST_CALLSITE_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        pa.field("call_id", pa.string(), nullable=False),
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
        _attrs_field(),
    ]
)

CST_DEF_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        pa.field("def_id", pa.string(), nullable=False),
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
        _attrs_field(),
    ]
)

CST_TYPE_EXPR_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
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
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        pa.field("owner_def_id", pa.string(), nullable=False),
        ("owner_kind", pa.string()),
        ("docstring", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
    ]
)

CST_DECORATOR_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        pa.field("owner_def_id", pa.string(), nullable=False),
        ("owner_kind", pa.string()),
        ("decorator_text", pa.string()),
        ("decorator_index", pa.int32()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
    ]
)

CST_CALL_ARG_T = pa.struct(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        ("file_sha256", pa.string()),
        pa.field("call_id", pa.string(), nullable=False),
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
        pa.field("path", pa.string(), nullable=False),
        pa.field("file_id", pa.string(), nullable=False),
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
        _attrs_field(),
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

AST_DOCSTRING_T = pa.struct(
    [
        ("owner_ast_id", pa.int32()),
        ("owner_kind", pa.string()),
        ("owner_name", pa.string()),
        ("docstring", pa.string()),
        ("span", SPAN_T),
        ("source", pa.string()),
        ("attrs", ATTRS_T),
    ]
)

AST_IMPORT_T = pa.struct(
    [
        ("ast_id", pa.int32()),
        ("parent_ast_id", pa.int32()),
        ("kind", pa.string()),
        ("module", pa.string()),
        ("name", pa.string()),
        ("asname", pa.string()),
        ("alias_index", pa.int32()),
        ("level", pa.int32()),
        ("span", SPAN_T),
        ("attrs", ATTRS_T),
    ]
)

AST_DEF_T = pa.struct(
    [
        ("ast_id", pa.int32()),
        ("parent_ast_id", pa.int32()),
        ("kind", pa.string()),
        ("name", pa.string()),
        ("span", SPAN_T),
        ("attrs", ATTRS_T),
    ]
)

AST_CALL_T = pa.struct(
    [
        ("ast_id", pa.int32()),
        ("parent_ast_id", pa.int32()),
        ("func_kind", pa.string()),
        ("func_name", pa.string()),
        ("span", SPAN_T),
        ("attrs", ATTRS_T),
    ]
)

AST_TYPE_IGNORE_T = pa.struct(
    [
        ("ast_id", pa.int32()),
        ("tag", pa.string()),
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
        ("docstrings", pa.list_(AST_DOCSTRING_T)),
        ("imports", pa.list_(AST_IMPORT_T)),
        ("defs", pa.list_(AST_DEF_T)),
        ("calls", pa.list_(AST_CALL_T)),
        ("type_ignores", pa.list_(AST_TYPE_IGNORE_T)),
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
        ("sym_symbol_id", pa.string()),
        ("flags", SYM_FLAGS_T),
        ("namespace_count", pa.int32()),
        ("namespace_block_ids", pa.list_(pa.int64())),
        _attrs_field(),
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

SYMTABLE_SPAN_META: dict[bytes, bytes] = {
    b"line_base": b"0",
    b"col_unit": b"utf32",
    b"end_exclusive": b"true",
}

SYM_BLOCK_T = pa.struct(
    [
        ("block_id", pa.int64()),
        ("parent_block_id", pa.int64()),
        ("block_type", pa.string()),
        ("is_meta_scope", pa.bool_()),
        ("name", pa.string()),
        ("lineno1", pa.int32()),
        pa.field("span_hint", SPAN_T, metadata=SYMTABLE_SPAN_META),
        ("scope_id", pa.string()),
        ("scope_local_id", pa.int64()),
        ("scope_type_value", pa.int32()),
        ("qualpath", pa.string()),
        ("function_partitions", SYM_FUNCTION_PARTITIONS_T),
        ("class_methods", pa.list_(pa.string())),
        ("symbols", pa.list_(SYM_SYMBOL_T)),
        _attrs_field(),
    ]
)

SYMTABLE_FILES_SCHEMA = pa.schema(
    [
        ("repo", pa.string()),
        ("path", pa.string()),
        ("file_id", pa.string()),
        ("blocks", pa.list_(SYM_BLOCK_T)),
        _attrs_field(),
    ]
)

BYTECODE_SPAN_META: dict[bytes, bytes] = {
    b"line_base": b"0",
    b"col_unit": b"utf32",
    b"end_exclusive": b"true",
}

BYTECODE_LINE_META: dict[bytes, bytes] = {b"line_base": b"1"}

_BYTECODE_IDENTITY_FIELDS: tuple[str, ...] = ("file_id", "path")
_BYTECODE_ORDERING_META = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("path", "ascending"), ("file_id", "ascending")),
)
_BYTECODE_CONSTRAINT_META = {
    REQUIRED_NON_NULL_META: metadata_list_bytes(_BYTECODE_IDENTITY_FIELDS),
    KEY_FIELDS_META: metadata_list_bytes(_BYTECODE_IDENTITY_FIELDS),
}

BYTECODE_SCHEMA_META: dict[bytes, bytes] = dict(_BYTECODE_ORDERING_META.schema_metadata)
BYTECODE_SCHEMA_META.update(_BYTECODE_CONSTRAINT_META)
BYTECODE_SCHEMA_META.update(
    {
        b"bytecode_abi_version": b"1",
        b"bytecode_span_line_base": BYTECODE_SPAN_META[b"line_base"],
        b"bytecode_span_col_unit": BYTECODE_SPAN_META[b"col_unit"],
        b"bytecode_span_end_exclusive": BYTECODE_SPAN_META[b"end_exclusive"],
    }
)

BYTECODE_CACHE_ENTRY_T = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("size", pa.int32()),
        pa.field("data_hex", pa.string()),
    ]
)

BYTECODE_CONST_T = pa.struct(
    [
        pa.field("const_index", pa.int32(), nullable=False),
        pa.field("const_repr", pa.string(), nullable=False),
    ]
)

BYTECODE_INSTR_T = pa.struct(
    [
        pa.field("instr_index", pa.int32(), nullable=False),
        pa.field("offset", pa.int32(), nullable=False),
        pa.field("start_offset", pa.int32()),
        pa.field("end_offset", pa.int32()),
        pa.field("opname", pa.string(), nullable=False),
        pa.field("baseopname", pa.string()),
        pa.field("opcode", pa.int32(), nullable=False),
        pa.field("baseopcode", pa.int32()),
        pa.field("arg", pa.int32()),
        pa.field("oparg", pa.int32()),
        pa.field("argval_kind", pa.string()),
        pa.field("argval_int", pa.int64()),
        pa.field("argval_str", pa.string()),
        pa.field("argrepr", pa.string()),
        pa.field("line_number", pa.int32()),
        pa.field("starts_line", pa.int32()),
        pa.field("label", pa.int32()),
        pa.field("is_jump_target", pa.bool_()),
        pa.field("jump_target", pa.int32()),
        pa.field("cache_info", pa.list_(BYTECODE_CACHE_ENTRY_T)),
        pa.field("span", SPAN_T, metadata=BYTECODE_SPAN_META),
        pa.field("attrs", ATTRS_T),
    ]
)

BYTECODE_EXCEPTION_T = pa.struct(
    [
        pa.field("exc_index", pa.int32(), nullable=False),
        pa.field("start_offset", pa.int32()),
        pa.field("end_offset", pa.int32()),
        pa.field("target_offset", pa.int32()),
        pa.field("depth", pa.int32()),
        pa.field("lasti", pa.bool_()),
        pa.field("attrs", ATTRS_T),
    ]
)

BYTECODE_BLOCK_T = pa.struct(
    [
        pa.field("start_offset", pa.int32(), nullable=False),
        pa.field("end_offset", pa.int32(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("attrs", ATTRS_T),
    ]
)

BYTECODE_CFG_EDGE_T = pa.struct(
    [
        pa.field("src_block_start", pa.int32(), nullable=False),
        pa.field("src_block_end", pa.int32(), nullable=False),
        pa.field("dst_block_start", pa.int32(), nullable=False),
        pa.field("dst_block_end", pa.int32(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("edge_key", pa.string(), nullable=False),
        pa.field("cond_instr_index", pa.int32()),
        pa.field("cond_instr_offset", pa.int32()),
        pa.field("exc_index", pa.int32()),
        pa.field("attrs", ATTRS_T),
    ]
)

BYTECODE_ERROR_T = pa.struct(
    [
        pa.field("error_type", pa.string(), nullable=False),
        pa.field("message", pa.string(), nullable=False),
        pa.field("attrs", ATTRS_T),
    ]
)

BYTECODE_LINE_T = pa.struct(
    [
        pa.field("offset", pa.int32(), nullable=False),
        pa.field("line1", pa.int32(), metadata=BYTECODE_LINE_META),
        pa.field("line0", pa.int32()),
        pa.field("attrs", ATTRS_T),
    ]
)

BYTECODE_DFG_EDGE_T = pa.struct(
    [
        pa.field("src_instr_index", pa.int32(), nullable=False),
        pa.field("dst_instr_index", pa.int32(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("attrs", ATTRS_T),
    ]
)

BYTECODE_FLAGS_T = pa.struct(
    [
        pa.field("is_optimized", pa.bool_()),
        pa.field("is_newlocals", pa.bool_()),
        pa.field("has_varargs", pa.bool_()),
        pa.field("has_varkeywords", pa.bool_()),
        pa.field("is_nested", pa.bool_()),
        pa.field("is_generator", pa.bool_()),
        pa.field("is_nofree", pa.bool_()),
        pa.field("is_coroutine", pa.bool_()),
        pa.field("is_iterable_coroutine", pa.bool_()),
        pa.field("is_async_generator", pa.bool_()),
    ]
)

BYTECODE_CODE_OBJ_T = pa.struct(
    [
        pa.field("code_id", pa.string(), nullable=False),
        pa.field("qualname", pa.string(), nullable=False),
        pa.field("co_qualname", pa.string()),
        pa.field("co_filename", pa.string()),
        pa.field("name", pa.string(), nullable=False),
        pa.field("firstlineno1", pa.int32(), nullable=False),
        pa.field("argcount", pa.int32()),
        pa.field("posonlyargcount", pa.int32()),
        pa.field("kwonlyargcount", pa.int32()),
        pa.field("nlocals", pa.int32()),
        pa.field("flags", pa.int32()),
        pa.field("flags_detail", BYTECODE_FLAGS_T),
        pa.field("stacksize", pa.int32()),
        pa.field("code_len", pa.int32()),
        pa.field("varnames", pa.list_(pa.string())),
        pa.field("freevars", pa.list_(pa.string())),
        pa.field("cellvars", pa.list_(pa.string())),
        pa.field("names", pa.list_(pa.string())),
        pa.field("consts", pa.list_(BYTECODE_CONST_T)),
        pa.field("consts_json", pa.string()),
        pa.field("line_table", pa.list_(BYTECODE_LINE_T), metadata=BYTECODE_LINE_META),
        pa.field("instructions", pa.list_(BYTECODE_INSTR_T), metadata=BYTECODE_SPAN_META),
        pa.field("exception_table", pa.list_(BYTECODE_EXCEPTION_T)),
        pa.field("blocks", pa.list_(BYTECODE_BLOCK_T)),
        pa.field("cfg_edges", pa.list_(BYTECODE_CFG_EDGE_T)),
        pa.field("dfg_edges", pa.list_(BYTECODE_DFG_EDGE_T)),
        pa.field("attrs", ATTRS_T),
    ]
)

BYTECODE_FILES_SCHEMA = pa.schema(
    [
        pa.field("repo", pa.string()),
        pa.field("path", pa.string(), nullable=False),
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("code_objects", pa.list_(BYTECODE_CODE_OBJ_T)),
        pa.field("errors", pa.list_(BYTECODE_ERROR_T)),
        pa.field("attrs", ATTRS_T),
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


def _schema_with_metadata(
    name: str,
    schema: pa.Schema,
    *,
    extra_metadata: Mapping[bytes, bytes] | None = None,
) -> pa.Schema:
    metadata = dict(schema.metadata or {})
    metadata[SCHEMA_META_NAME] = name.encode("utf-8")
    version = _schema_version_from_name(name)
    if version is not None:
        metadata[SCHEMA_META_VERSION] = str(version).encode("utf-8")
    if extra_metadata:
        metadata.update(extra_metadata)
    return schema.with_metadata(metadata)


AST_FILES_SCHEMA = _schema_with_metadata("ast_files_v1", AST_FILES_SCHEMA)
BYTECODE_FILES_SCHEMA = _schema_with_metadata(
    "bytecode_files_v1",
    BYTECODE_FILES_SCHEMA,
    extra_metadata=BYTECODE_SCHEMA_META,
)
_LIBCST_ORDERING_META = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("path", "ascending"), ("file_id", "ascending")),
)
_LIBCST_IDENTITY_FIELDS = ("file_id", "path")
_LIBCST_CONSTRAINT_META = {
    REQUIRED_NON_NULL_META: metadata_list_bytes(_LIBCST_IDENTITY_FIELDS),
    KEY_FIELDS_META: metadata_list_bytes(_LIBCST_IDENTITY_FIELDS),
}
_LIBCST_SCHEMA_META = dict(_LIBCST_ORDERING_META.schema_metadata)
_LIBCST_SCHEMA_META.update(_LIBCST_CONSTRAINT_META)
LIBCST_FILES_SCHEMA = _schema_with_metadata(
    "libcst_files_v1",
    LIBCST_FILES_SCHEMA,
    extra_metadata=_LIBCST_SCHEMA_META,
)
SCIP_METADATA_SCHEMA = _schema_with_metadata("scip_metadata_v1", SCIP_METADATA_SCHEMA)
SCIP_INDEX_STATS_SCHEMA = _schema_with_metadata("scip_index_stats_v1", SCIP_INDEX_STATS_SCHEMA)
SCIP_DOCUMENTS_SCHEMA = _schema_with_metadata("scip_documents_v1", SCIP_DOCUMENTS_SCHEMA)
SCIP_DOCUMENT_TEXTS_SCHEMA = _schema_with_metadata("scip_document_texts_v1", SCIP_DOCUMENT_TEXTS_SCHEMA)
SCIP_OCCURRENCES_SCHEMA = _schema_with_metadata("scip_occurrences_v1", SCIP_OCCURRENCES_SCHEMA)
SCIP_SYMBOL_INFORMATION_SCHEMA = _schema_with_metadata(
    "scip_symbol_information_v1",
    SCIP_SYMBOL_INFORMATION_SCHEMA,
)
SCIP_DOCUMENT_SYMBOLS_SCHEMA = _schema_with_metadata(
    "scip_document_symbols_v1",
    SCIP_DOCUMENT_SYMBOLS_SCHEMA,
)
SCIP_EXTERNAL_SYMBOL_INFORMATION_SCHEMA = _schema_with_metadata(
    "scip_external_symbol_information_v1",
    SCIP_EXTERNAL_SYMBOL_INFORMATION_SCHEMA,
)
SCIP_SYMBOL_RELATIONSHIPS_SCHEMA = _schema_with_metadata(
    "scip_symbol_relationships_v1",
    SCIP_SYMBOL_RELATIONSHIPS_SCHEMA,
)
SCIP_SIGNATURE_OCCURRENCES_SCHEMA = _schema_with_metadata(
    "scip_signature_occurrences_v1",
    SCIP_SIGNATURE_OCCURRENCES_SCHEMA,
)
SCIP_DIAGNOSTICS_SCHEMA = _schema_with_metadata("scip_diagnostics_v1", SCIP_DIAGNOSTICS_SCHEMA)
_SYMTABLE_IDENTITY_FIELDS = ("file_id", "path")
_SYMTABLE_ORDERING_META = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("path", "ascending"), ("file_id", "ascending")),
)
_SYMTABLE_CONSTRAINT_META = {
    REQUIRED_NON_NULL_META: metadata_list_bytes(_SYMTABLE_IDENTITY_FIELDS),
    KEY_FIELDS_META: metadata_list_bytes(_SYMTABLE_IDENTITY_FIELDS),
}
_SYMTABLE_SCHEMA_META = dict(_SYMTABLE_ORDERING_META.schema_metadata)
_SYMTABLE_SCHEMA_META.update(_SYMTABLE_CONSTRAINT_META)
SYMTABLE_FILES_SCHEMA = _schema_with_metadata(
    "symtable_files_v1",
    SYMTABLE_FILES_SCHEMA,
    extra_metadata=_SYMTABLE_SCHEMA_META,
)
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

DIM_QUALIFIED_NAMES_SCHEMA = _schema_with_metadata(
    "dim_qualified_names_v1",
    pa.schema(
        [
            pa.field("qname_id", pa.string()),
            pa.field("qname", pa.string()),
        ]
    ),
)

CALLSITE_QNAME_CANDIDATES_SCHEMA = _schema_with_metadata(
    "callsite_qname_candidates_v1",
    pa.schema(
        [
            pa.field("call_id", pa.string()),
            pa.field("qname", pa.string()),
            pa.field("path", pa.string()),
            pa.field("call_bstart", pa.int64()),
            pa.field("call_bend", pa.int64()),
            pa.field("arg_count", pa.int32()),
            pa.field("keyword_count", pa.int32()),
            pa.field("star_arg_count", pa.int32()),
            pa.field("star_kwarg_count", pa.int32()),
            pa.field("positional_count", pa.int32()),
            pa.field("qname_source", pa.string()),
        ]
    ),
)

REL_NAME_SYMBOL_SCHEMA = _schema_with_metadata(
    "rel_name_symbol_v1",
    pa.schema(
        [
            pa.field("ref_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("symbol_roles", pa.int32()),
            pa.field("path", pa.string()),
            pa.field("edge_owner_file_id", pa.string()),
            pa.field("bstart", pa.int64()),
            pa.field("bend", pa.int64()),
            pa.field("resolution_method", pa.string()),
            pa.field("confidence", pa.float32()),
            pa.field("score", pa.float32()),
            pa.field("rule_name", pa.string()),
            pa.field("rule_priority", pa.int32()),
        ]
    ),
)

REL_IMPORT_SYMBOL_SCHEMA = _schema_with_metadata(
    "rel_import_symbol_v1",
    pa.schema(
        [
            pa.field("import_alias_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("symbol_roles", pa.int32()),
            pa.field("path", pa.string()),
            pa.field("edge_owner_file_id", pa.string()),
            pa.field("bstart", pa.int64()),
            pa.field("bend", pa.int64()),
            pa.field("resolution_method", pa.string()),
            pa.field("confidence", pa.float32()),
            pa.field("score", pa.float32()),
            pa.field("rule_name", pa.string()),
            pa.field("rule_priority", pa.int32()),
        ]
    ),
)

REL_DEF_SYMBOL_SCHEMA = _schema_with_metadata(
    "rel_def_symbol_v1",
    pa.schema(
        [
            pa.field("def_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("symbol_roles", pa.int32()),
            pa.field("path", pa.string()),
            pa.field("edge_owner_file_id", pa.string()),
            pa.field("bstart", pa.int64()),
            pa.field("bend", pa.int64()),
            pa.field("resolution_method", pa.string()),
            pa.field("confidence", pa.float32()),
            pa.field("score", pa.float32()),
            pa.field("rule_name", pa.string()),
            pa.field("rule_priority", pa.int32()),
        ]
    ),
)

REL_CALLSITE_SYMBOL_SCHEMA = _schema_with_metadata(
    "rel_callsite_symbol_v1",
    pa.schema(
        [
            pa.field("call_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("symbol_roles", pa.int32()),
            pa.field("path", pa.string()),
            pa.field("edge_owner_file_id", pa.string()),
            pa.field("call_bstart", pa.int64()),
            pa.field("call_bend", pa.int64()),
            pa.field("resolution_method", pa.string()),
            pa.field("confidence", pa.float32()),
            pa.field("score", pa.float32()),
            pa.field("rule_name", pa.string()),
            pa.field("rule_priority", pa.int32()),
        ]
    ),
)

REL_CALLSITE_QNAME_SCHEMA = _schema_with_metadata(
    "rel_callsite_qname_v1",
    pa.schema(
        [
            pa.field("call_id", pa.string()),
            pa.field("qname_id", pa.string()),
            pa.field("qname_source", pa.string()),
            pa.field("path", pa.string()),
            pa.field("edge_owner_file_id", pa.string()),
            pa.field("call_bstart", pa.int64()),
            pa.field("call_bend", pa.int64()),
            pa.field("confidence", pa.float32()),
            pa.field("score", pa.float32()),
            pa.field("ambiguity_group_id", pa.string()),
            pa.field("rule_name", pa.string()),
            pa.field("rule_priority", pa.int32()),
        ]
    ),
)

RELATION_OUTPUT_SCHEMA = _schema_with_metadata(
    "relation_output_v1",
    pa.schema(
        [
            pa.field("src", pa.string(), nullable=True),
            pa.field("dst", pa.string(), nullable=True),
            pa.field("path", pa.string(), nullable=True),
            pa.field("edge_owner_file_id", pa.string(), nullable=True),
            pa.field("bstart", pa.int64(), nullable=True),
            pa.field("bend", pa.int64(), nullable=True),
            pa.field("origin", pa.string(), nullable=True),
            pa.field("resolution_method", pa.string(), nullable=True),
            pa.field("binding_kind", pa.string(), nullable=True),
            pa.field("def_site_kind", pa.string(), nullable=True),
            pa.field("use_kind", pa.string(), nullable=True),
            pa.field("kind", pa.string(), nullable=True),
            pa.field("reason", pa.string(), nullable=True),
            pa.field("confidence", pa.float32(), nullable=True),
            pa.field("score", pa.float32(), nullable=True),
            pa.field("symbol_roles", pa.int32(), nullable=True),
            pa.field("qname_source", pa.string(), nullable=True),
            pa.field("ambiguity_group_id", pa.string(), nullable=True),
            pa.field("diag_source", pa.string(), nullable=True),
            pa.field("severity", pa.string(), nullable=True),
            pa.field("rule_name", pa.string(), nullable=True),
            pa.field("rule_priority", pa.int32(), nullable=True),
        ]
    ),
)

SCALAR_PARAM_SIGNATURE_SCHEMA = _schema_with_metadata(
    "scalar_param_signature_v1",
    pa.schema(
        [
            pa.field("version", pa.int32(), nullable=False),
            pa.field(
                "entries",
                pa.list_(
                    pa.struct(
                        [
                            ("key", pa.string()),
                            ("value", pa.string()),
                        ]
                    )
                ),
                nullable=False,
            ),
        ]
    ),
)

DATASET_FINGERPRINT_SCHEMA = _schema_with_metadata(
    "dataset_fingerprint_v1",
    pa.schema(
        [
            pa.field("version", pa.int32(), nullable=False),
            pa.field("plan_hash", pa.string(), nullable=False),
            pa.field("schema_fingerprint", pa.string(), nullable=False),
            pa.field("profile_hash", pa.string(), nullable=False),
            pa.field("writer_strategy", pa.string(), nullable=False),
            pa.field("input_fingerprints", pa.list_(pa.string()), nullable=False),
        ]
    ),
)

PARAM_FILE_IDS_SCHEMA = _schema_with_metadata(
    "param_file_ids_v1",
    pa.schema(
        [
            pa.field("file_id", pa.string(), nullable=False),
        ]
    ),
)

SCHEMA_REGISTRY: dict[str, pa.Schema] = {
    "ast_files_v1": AST_FILES_SCHEMA,
    "bytecode_files_v1": BYTECODE_FILES_SCHEMA,
    "callsite_qname_candidates_v1": CALLSITE_QNAME_CANDIDATES_SCHEMA,
    "dataset_fingerprint_v1": DATASET_FINGERPRINT_SCHEMA,
    "datafusion_explains_v1": DATAFUSION_EXPLAINS_SCHEMA,
    "datafusion_fallbacks_v1": DATAFUSION_FALLBACKS_SCHEMA,
    "datafusion_schema_registry_validation_v1": DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SCHEMA,
    "dim_qualified_names_v1": DIM_QUALIFIED_NAMES_SCHEMA,
    "feature_state_v1": FEATURE_STATE_SCHEMA,
    "libcst_files_v1": LIBCST_FILES_SCHEMA,
    "param_file_ids_v1": PARAM_FILE_IDS_SCHEMA,
    "rel_callsite_qname_v1": REL_CALLSITE_QNAME_SCHEMA,
    "rel_callsite_symbol_v1": REL_CALLSITE_SYMBOL_SCHEMA,
    "rel_def_symbol_v1": REL_DEF_SYMBOL_SCHEMA,
    "rel_import_symbol_v1": REL_IMPORT_SYMBOL_SCHEMA,
    "rel_name_symbol_v1": REL_NAME_SYMBOL_SCHEMA,
    "relation_output_v1": RELATION_OUTPUT_SCHEMA,
    "repo_snapshot_v1": REPO_SNAPSHOT_SCHEMA,
    "scalar_param_signature_v1": SCALAR_PARAM_SIGNATURE_SCHEMA,
    "scip_metadata_v1": SCIP_METADATA_SCHEMA,
    "scip_index_stats_v1": SCIP_INDEX_STATS_SCHEMA,
    "scip_documents_v1": SCIP_DOCUMENTS_SCHEMA,
    "scip_document_texts_v1": SCIP_DOCUMENT_TEXTS_SCHEMA,
    "scip_occurrences_v1": SCIP_OCCURRENCES_SCHEMA,
    "scip_symbol_information_v1": SCIP_SYMBOL_INFORMATION_SCHEMA,
    "scip_document_symbols_v1": SCIP_DOCUMENT_SYMBOLS_SCHEMA,
    "scip_external_symbol_information_v1": SCIP_EXTERNAL_SYMBOL_INFORMATION_SCHEMA,
    "scip_symbol_relationships_v1": SCIP_SYMBOL_RELATIONSHIPS_SCHEMA,
    "scip_signature_occurrences_v1": SCIP_SIGNATURE_OCCURRENCES_SCHEMA,
    "scip_diagnostics_v1": SCIP_DIAGNOSTICS_SCHEMA,
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
    "ast_docstrings": {
        "root": "ast_files_v1",
        "path": "docstrings",
        "role": "derived",
        "context": {},
    },
    "ast_imports": {
        "root": "ast_files_v1",
        "path": "imports",
        "role": "derived",
        "context": {},
    },
    "ast_defs": {
        "root": "ast_files_v1",
        "path": "defs",
        "role": "derived",
        "context": {},
    },
    "ast_calls": {
        "root": "ast_files_v1",
        "path": "calls",
        "role": "derived",
        "context": {},
    },
    "ast_type_ignores": {
        "root": "ast_files_v1",
        "path": "type_ignores",
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
    "py_bc_code_units": {
        "root": "bytecode_files_v1",
        "path": "code_objects",
        "role": "derived",
        "context": {},
    },
    "py_bc_line_table": {
        "root": "bytecode_files_v1",
        "path": "code_objects.line_table",
        "role": "derived",
        "context": {"code_id": "code_objects.code_id"},
    },
    "py_bc_instructions": {
        "root": "bytecode_files_v1",
        "path": "code_objects.instructions",
        "role": "derived",
        "context": {"code_id": "code_objects.code_id"},
    },
    "py_bc_cache_entries": {
        "root": "bytecode_files_v1",
        "path": "code_objects.instructions.cache_info",
        "role": "derived",
        "context": {
            "code_id": "code_objects.code_id",
            "instr_index": "code_objects.instructions.instr_index",
            "offset": "code_objects.instructions.offset",
        },
    },
    "py_bc_consts": {
        "root": "bytecode_files_v1",
        "path": "code_objects.consts",
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
    "py_bc_dfg_edges": {
        "root": "bytecode_files_v1",
        "path": "code_objects.dfg_edges",
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
    "bytecode_files_v1": _BYTECODE_IDENTITY_FIELDS,
    "libcst_files_v1": _LIBCST_IDENTITY_FIELDS,
    "symtable_files_v1": _SYMTABLE_IDENTITY_FIELDS,
    "tree_sitter_files_v1": ("file_id", "path"),
}

AST_VIEW_NAMES: tuple[str, ...] = (
    "ast_nodes",
    "ast_edges",
    "ast_errors",
    "ast_docstrings",
    "ast_imports",
    "ast_defs",
    "ast_calls",
    "ast_type_ignores",
)

SCIP_VIEW_SCHEMA_MAP: dict[str, str] = {
    "scip_metadata": "scip_metadata_v1",
    "scip_index_stats": "scip_index_stats_v1",
    "scip_documents": "scip_documents_v1",
    "scip_document_texts": "scip_document_texts_v1",
    "scip_occurrences": "scip_occurrences_v1",
    "scip_symbol_information": "scip_symbol_information_v1",
    "scip_document_symbols": "scip_document_symbols_v1",
    "scip_external_symbol_information": "scip_external_symbol_information_v1",
    "scip_symbol_relationships": "scip_symbol_relationships_v1",
    "scip_signature_occurrences": "scip_signature_occurrences_v1",
    "scip_diagnostics": "scip_diagnostics_v1",
}

SCIP_VIEW_NAMES: tuple[str, ...] = tuple(SCIP_VIEW_SCHEMA_MAP)

SYMTABLE_VIEW_NAMES: tuple[str, ...] = (
    "symtable_scopes",
    "symtable_symbols",
    "symtable_scope_edges",
    "symtable_namespace_edges",
    "symtable_function_partitions",
    "symtable_class_methods",
    "symtable_symbol_attrs",
)

BYTECODE_VIEW_NAMES: tuple[str, ...] = (
    "py_bc_code_units",
    "py_bc_consts",
    "py_bc_line_table",
    "py_bc_instructions",
    "py_bc_instruction_attrs",
    "py_bc_instruction_attr_keys",
    "py_bc_instruction_attr_values",
    "py_bc_instruction_spans",
    "py_bc_instruction_span_fields",
    "py_bc_cache_entries",
    "py_bc_metadata",
    "bytecode_exception_table",
    "py_bc_cfg_edge_attrs",
    "py_bc_blocks",
    "py_bc_dfg_edges",
    "py_bc_flags_detail",
    "py_bc_cfg_edges",
    "py_bc_error_attrs",
    "bytecode_errors",
)

BYTECODE_REQUIRED_FUNCTIONS: tuple[str, ...] = (
    "arrow_cast",
    "arrow_metadata",
    "arrow_typeof",
    "map_entries",
    "map_extract",
    "map_keys",
    "map_values",
    "list_extract",
    "named_struct",
    "unnest",
)

BYTECODE_REQUIRED_FUNCTION_SIGNATURES: dict[str, int] = {
    "arrow_cast": 2,
    "arrow_metadata": 1,
    "arrow_typeof": 1,
    "list_extract": 2,
    "map_entries": 1,
    "map_extract": 2,
    "map_keys": 1,
    "map_values": 1,
    "named_struct": 2,
    "unnest": 1,
}

CST_REQUIRED_FUNCTIONS: tuple[str, ...] = (
    "arrow_cast",
    "arrow_metadata",
    "arrow_typeof",
    "map_entries",
    "named_struct",
    "unnest",
)

CST_REQUIRED_FUNCTION_SIGNATURES: dict[str, int] = {
    "arrow_cast": 2,
    "arrow_metadata": 1,
    "arrow_typeof": 1,
    "map_entries": 1,
    "named_struct": 2,
    "unnest": 1,
}

SCIP_REQUIRED_FUNCTIONS: tuple[str, ...] = ()

CST_VIEW_NAMES: tuple[str, ...] = (
    "cst_parse_manifest",
    "cst_parse_errors",
    "cst_refs",
    "cst_imports",
    "cst_callsites",
    "cst_defs",
    "cst_type_exprs",
    "cst_docstrings",
    "cst_decorators",
    "cst_call_args",
    "cst_nodes",
    "cst_edges",
    "cst_refs_attrs",
    "cst_defs_attrs",
    "cst_callsites_attrs",
    "cst_imports_attrs",
    "cst_nodes_attrs",
    "cst_edges_attrs",
    "cst_refs_attr_origin",
    "cst_defs_attr_origin",
    "cst_callsites_attr_origin",
    "cst_imports_attr_origin",
    "cst_nodes_attr_origin",
    "cst_edges_attr_origin",
    "cst_ref_spans",
    "cst_callsite_spans",
    "cst_def_spans",
    "cst_ref_span_unnest",
    "cst_callsite_span_unnest",
    "cst_def_span_unnest",
    "cst_schema_diagnostics",
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
            item_alias = f"{alias}_item"
            from_clause += f"\nCROSS JOIN unnest({list_expr}) AS {alias}({item_alias})"
            current_expr = f"{alias}.{item_alias}"
            current_is_root = False
            value_type = dtype.value_type
            if not pa.types.is_struct(value_type):
                msg = f"Nested path {path!r} does not resolve to a struct at {step!r}."
                raise TypeError(msg)
            current_struct = value_type
            prefix_exprs[prefix] = current_expr
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


def validate_ast_views(ctx: SessionContext) -> None:
    """Validate AST view schemas using DataFusion introspection.

    Raises
    ------
    ValueError
        Raised when view schemas fail validation.
    """
    errors: dict[str, str] = {}
    for name in AST_VIEW_NAMES:
        try:
            ctx.sql(f"DESCRIBE SELECT * FROM {name}").collect()
        except (RuntimeError, TypeError, ValueError) as exc:
            errors[name] = str(exc)
    try:
        ctx.sql("SELECT arrow_typeof(nodes) AS nodes_type FROM ast_files_v1 LIMIT 1").collect()
        ctx.sql("SELECT arrow_typeof(edges) AS edges_type FROM ast_files_v1 LIMIT 1").collect()
        ctx.sql("SELECT arrow_typeof(errors) AS errors_type FROM ast_files_v1 LIMIT 1").collect()
        ctx.sql(
            "SELECT arrow_typeof(docstrings) AS docstrings_type FROM ast_files_v1 LIMIT 1"
        ).collect()
        ctx.sql("SELECT arrow_typeof(imports) AS imports_type FROM ast_files_v1 LIMIT 1").collect()
        ctx.sql("SELECT arrow_typeof(defs) AS defs_type FROM ast_files_v1 LIMIT 1").collect()
        ctx.sql("SELECT arrow_typeof(calls) AS calls_type FROM ast_files_v1 LIMIT 1").collect()
        ctx.sql(
            "SELECT arrow_typeof(type_ignores) AS type_ignores_type FROM ast_files_v1 LIMIT 1"
        ).collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["ast_files_v1"] = str(exc)
    if errors:
        msg = f"AST view validation failed: {errors}."
        raise ValueError(msg)


def validate_symtable_views(ctx: SessionContext) -> None:
    """Validate symtable view schemas using DataFusion introspection.

    Raises
    ------
    ValueError
        Raised when view schemas fail validation.
    """
    errors: dict[str, str] = {}
    for name in SYMTABLE_VIEW_NAMES:
        try:
            ctx.sql(f"DESCRIBE SELECT * FROM {name}").collect()
        except (RuntimeError, TypeError, ValueError) as exc:
            errors[name] = str(exc)
    for name in SYMTABLE_VIEW_NAMES:
        try:
            expected = set(schema_for(name).names)
            rows = ctx.sql(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{name}'"
            ).to_arrow_table()
            actual = {
                str(row.get("column_name")) for row in rows.to_pylist() if row.get("column_name")
            }
            missing = sorted(expected - actual)
            if missing:
                errors[f"{name}_information_schema"] = f"Missing columns: {missing}."
        except (RuntimeError, TypeError, ValueError, KeyError) as exc:
            errors[f"{name}_information_schema"] = str(exc)
    try:
        ctx.sql(
            "SELECT arrow_typeof(blocks) AS blocks_type FROM symtable_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_typeof(blocks.symbols) AS symbols_type FROM symtable_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_metadata(blocks.span_hint, 'line_base') AS span_line_base "
            "FROM symtable_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_metadata(blocks.span_hint, 'col_unit') AS span_col_unit "
            "FROM symtable_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_metadata(blocks.span_hint, 'end_exclusive') AS span_end_exclusive "
            "FROM symtable_files_v1 LIMIT 1"
        ).collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["symtable_files_v1"] = str(exc)
    if errors:
        msg = f"Symtable view validation failed: {errors}."
        raise ValueError(msg)


def validate_scip_views(ctx: SessionContext) -> None:
    """Validate SCIP view schemas using DataFusion introspection.

    Raises
    ------
    ValueError
        Raised when view schemas fail validation.
    """
    errors: dict[str, str] = {}
    _validate_required_functions(ctx, required=SCIP_REQUIRED_FUNCTIONS, errors=errors)
    introspector = SchemaIntrospector(ctx)
    for view_name in SCIP_VIEW_NAMES:
        try:
            rows = introspector.describe_query(f"SELECT * FROM {view_name}")
        except (RuntimeError, TypeError, ValueError) as exc:
            errors[view_name] = str(exc)
            continue
        columns = _describe_column_names(rows)
        invalid = _invalid_output_names(columns)
        if invalid:
            errors[f"{view_name}_invalid_columns"] = f"Invalid column names: {invalid}."
    for view_name, schema_name in SCIP_VIEW_SCHEMA_MAP.items():
        try:
            expected = set(schema_for(schema_name).names)
            rows = ctx.sql(
                "SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{view_name}'"
            ).to_arrow_table()
            actual = {
                str(row.get("column_name"))
                for row in rows.to_pylist()
                if row.get("column_name")
            }
            missing = sorted(expected - actual)
            if missing:
                errors[f"{view_name}_information_schema"] = f"Missing columns: {missing}."
        except (RuntimeError, TypeError, ValueError, KeyError) as exc:
            errors[f"{view_name}_information_schema"] = str(exc)
    if errors:
        msg = f"SCIP view validation failed: {errors}."
        raise ValueError(msg)


def _function_names(ctx: SessionContext) -> set[str]:
    try:
        table = ctx.sql("SHOW FUNCTIONS").to_arrow_table()
    except (RuntimeError, TypeError, ValueError):
        return set()
    names: set[str] = set()
    for row in table.to_pylist():
        for key in ("function_name", "name"):
            value = row.get(key)
            if isinstance(value, str):
                names.add(value)
    return names


def _validate_required_functions(
    ctx: SessionContext,
    *,
    required: Sequence[str],
    errors: dict[str, str],
) -> None:
    available = {name.lower() for name in _function_names(ctx)}
    if not available:
        errors["datafusion_functions"] = "SHOW FUNCTIONS returned no entries."
        return
    missing = sorted(name for name in required if name.lower() not in available)
    if missing:
        errors["datafusion_functions"] = f"Missing required functions: {missing}."


def _signature_name(row: Mapping[str, object]) -> str | None:
    for key in ("specific_name", "routine_name", "function_name", "name"):
        value = row.get(key)
        if isinstance(value, str):
            return value
    return None


def _validate_function_signatures(
    ctx: SessionContext,
    *,
    required: Mapping[str, int],
    errors: dict[str, str],
) -> None:
    introspector = SchemaIntrospector(ctx)
    try:
        rows = introspector.parameters_snapshot()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["datafusion_function_signatures"] = str(exc)
        return
    if not rows:
        errors["datafusion_function_signatures"] = "information_schema.parameters returned no rows."
        return
    counts = _parameter_counts(rows)
    signature_errors = _signature_errors(required, counts)
    if signature_errors:
        errors["datafusion_function_signatures"] = str(signature_errors)


def _parameter_counts(rows: Sequence[Mapping[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        name = _signature_name(row)
        if name is None:
            continue
        normalized = name.lower()
        counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def _signature_errors(
    required: Mapping[str, int],
    counts: Mapping[str, int],
) -> dict[str, list[str]]:
    missing = [name for name in required if name.lower() not in counts]
    mismatched = _mismatched_signatures(required, counts)
    details: dict[str, list[str]] = {}
    if missing:
        details["missing"] = missing
    if mismatched:
        details["mismatched"] = mismatched
    return details


def _mismatched_signatures(
    required: Mapping[str, int],
    counts: Mapping[str, int],
) -> list[str]:
    mismatched: list[str] = []
    for name, min_args in required.items():
        count = counts.get(name.lower())
        if count is not None and count < min_args:
            mismatched.append(f"{name} ({count} < {min_args})")
    return mismatched


def _describe_column_names(rows: Sequence[Mapping[str, object]]) -> tuple[str, ...]:
    names: list[str] = []
    for row in rows:
        for key in ("column_name", "name", "column"):
            value = row.get(key)
            if value is not None:
                names.append(str(value))
                break
    return tuple(names)


def _invalid_output_names(names: Sequence[str]) -> tuple[str, ...]:
    invalid = []
    for name in names:
        if not name:
            invalid.append(name)
            continue
        if any(token in name for token in (".", "(", ")", "{", "}", " ")):
            invalid.append(name)
    return tuple(invalid)


def validate_bytecode_views(ctx: SessionContext) -> None:
    """Validate bytecode view schemas using DataFusion introspection.

    Raises
    ------
    ValueError
        Raised when view schemas fail validation.
    """
    errors: dict[str, str] = {}
    _validate_required_functions(ctx, required=BYTECODE_REQUIRED_FUNCTIONS, errors=errors)
    _validate_function_signatures(
        ctx,
        required=BYTECODE_REQUIRED_FUNCTION_SIGNATURES,
        errors=errors,
    )
    for name in BYTECODE_VIEW_NAMES:
        try:
            ctx.sql(f"DESCRIBE SELECT * FROM {name}").collect()
        except (RuntimeError, TypeError, ValueError) as exc:
            errors[name] = str(exc)
    try:
        ctx.sql(
            "SELECT arrow_typeof(code_objects) AS code_objects_type FROM bytecode_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_typeof(code_objects.instructions) AS instr_type "
            "FROM bytecode_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_metadata(code_objects.instructions, 'line_base') "
            "AS instr_line_base_meta FROM bytecode_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_metadata(code_objects.instructions, 'col_unit') "
            "AS instr_col_unit_meta FROM bytecode_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_metadata(code_objects.instructions, 'end_exclusive') "
            "AS instr_end_exclusive_meta FROM bytecode_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_typeof(code_objects.instructions.cache_info) AS cache_info_type "
            "FROM bytecode_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_typeof(code_objects.line_table) AS line_table_type "
            "FROM bytecode_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_metadata(code_objects.line_table, 'line_base') "
            "AS line_table_meta FROM bytecode_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_typeof(code_objects.flags_detail) AS flags_detail_type "
            "FROM bytecode_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_typeof(code_objects.consts) AS consts_type FROM bytecode_files_v1 LIMIT 1"
        ).collect()
        ctx.sql(
            "SELECT arrow_typeof(code_objects.dfg_edges) AS dfg_edges_type "
            "FROM bytecode_files_v1 LIMIT 1"
        ).collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["bytecode_files_v1"] = str(exc)
    if errors:
        msg = f"Bytecode view validation failed: {errors}."
        raise ValueError(msg)


def validate_cst_views(ctx: SessionContext) -> None:
    """Validate CST view schemas using DataFusion introspection.

    Raises
    ------
    ValueError
        Raised when view schemas fail validation.
    """
    errors: dict[str, str] = {}
    _validate_required_functions(ctx, required=CST_REQUIRED_FUNCTIONS, errors=errors)
    _validate_function_signatures(ctx, required=CST_REQUIRED_FUNCTION_SIGNATURES, errors=errors)
    introspector = SchemaIntrospector(ctx)
    for name in CST_VIEW_NAMES:
        try:
            rows = introspector.describe_query(f"SELECT * FROM {name}")
        except (RuntimeError, TypeError, ValueError) as exc:
            errors[name] = str(exc)
            continue
        columns = _describe_column_names(rows)
        invalid = _invalid_output_names(columns)
        if invalid:
            errors[f"{name}_output_names"] = f"Invalid output column names: {invalid}"
    try:
        ctx.sql("SELECT * FROM cst_schema_diagnostics").collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["cst_schema_diagnostics"] = str(exc)
    if errors:
        msg = f"CST view validation failed: {errors}."
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
    try:
        existing = registered_table_names(ctx)
    except (RuntimeError, TypeError, ValueError):
        existing = set()
    for name, schema in SCHEMA_REGISTRY.items():
        validate_schema_metadata(schema)
        if name in existing:
            continue
        register_schema(ctx, name, schema)
    for name in nested_schema_names():
        register_schema(ctx, name, nested_schema_for(name))


__all__ = [
    "AST_FILES_SCHEMA",
    "AST_VIEW_NAMES",
    "BYTECODE_FILES_SCHEMA",
    "BYTECODE_VIEW_NAMES",
    "CST_VIEW_NAMES",
    "LIBCST_FILES_SCHEMA",
    "NESTED_DATASET_INDEX",
    "SCHEMA_REGISTRY",
    "SCIP_DIAGNOSTICS_SCHEMA",
    "SCIP_DOCUMENTS_SCHEMA",
    "SCIP_DOCUMENT_SYMBOLS_SCHEMA",
    "SCIP_DOCUMENT_TEXTS_SCHEMA",
    "SCIP_EXTERNAL_SYMBOL_INFORMATION_SCHEMA",
    "SCIP_INDEX_STATS_SCHEMA",
    "SCIP_METADATA_SCHEMA",
    "SCIP_OCCURRENCES_SCHEMA",
    "SCIP_SIGNATURE_OCCURRENCES_SCHEMA",
    "SCIP_SYMBOL_INFORMATION_SCHEMA",
    "SCIP_SYMBOL_RELATIONSHIPS_SCHEMA",
    "SCIP_VIEW_NAMES",
    "SCIP_VIEW_SCHEMA_MAP",
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
    "validate_ast_views",
    "validate_bytecode_views",
    "validate_cst_views",
    "validate_nested_types",
    "validate_schema_metadata",
    "validate_scip_views",
    "validate_symtable_views",
]
