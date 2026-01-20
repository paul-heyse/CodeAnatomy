"""DataFusion-native nested schemas for canonical datasets."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import pyarrow as pa

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
        ("line_base", pa.int32()),
        ("col_unit", pa.string()),
        ("end_exclusive", pa.bool_()),
        ("meta", ATTRS_T),
    ]
)

CST_NAME_REF_T = pa.struct(
    [
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("name", pa.string()),
        ("expr_ctx", pa.string()),
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
        ("call_bstart", pa.int64()),
        ("call_bend", pa.int64()),
        ("callee_bstart", pa.int64()),
        ("callee_bend", pa.int64()),
        ("callee_shape", pa.string()),
        ("callee_text", pa.string()),
        ("arg_count", pa.int32()),
        ("callee_dotted", pa.string()),
        ("callee_qnames", pa.list_(QNAME_T)),
    ]
)

CST_DEF_T = pa.struct(
    [
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
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

LIBCST_FILES_SCHEMA = pa.schema(
    [
        ("repo", pa.string()),
        ("path", pa.string()),
        ("file_id", pa.string()),
        ("nodes", pa.list_(CST_NODE_T)),
        ("edges", pa.list_(CST_EDGE_T)),
        ("parse_manifest", pa.list_(CST_PARSE_MANIFEST_T)),
        ("parse_errors", pa.list_(CST_PARSE_ERROR_T)),
        ("name_refs", pa.list_(CST_NAME_REF_T)),
        ("imports", pa.list_(CST_IMPORT_T)),
        ("callsites", pa.list_(CST_CALLSITE_T)),
        ("defs", pa.list_(CST_DEF_T)),
        ("type_exprs", pa.list_(CST_TYPE_EXPR_T)),
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
        ("attrs", ATTRS_T),
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

SCHEMA_REGISTRY: dict[str, pa.Schema] = {
    "ast_files_v1": AST_FILES_SCHEMA,
    "bytecode_files_v1": BYTECODE_FILES_SCHEMA,
    "libcst_files_v1": LIBCST_FILES_SCHEMA,
    "scip_index_v1": SCIP_INDEX_SCHEMA,
    "symtable_files_v1": SYMTABLE_FILES_SCHEMA,
    "tree_sitter_files_v1": TREE_SITTER_FILES_SCHEMA,
}


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
    return tuple(sorted(SCHEMA_REGISTRY))


def has_schema(name: str) -> bool:
    """Return whether a schema is registered.

    Returns
    -------
    bool
        ``True`` when the schema name is registered.
    """
    return name in SCHEMA_REGISTRY


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
    if schema is None:
        msg = f"Unknown DataFusion schema: {name!r}."
        raise KeyError(msg)
    return schema


def register_schema(ctx: SessionContext, name: str, schema: pa.Schema) -> None:
    """Register a schema in a DataFusion SessionContext using an empty batch."""
    arrays = [pa.array([], type=field.type) for field in schema]
    batch = pa.record_batch(arrays, schema=schema)
    ctx.register_record_batches(name, [[batch]])


def register_all_schemas(ctx: SessionContext) -> None:
    """Register all canonical schemas in a DataFusion SessionContext."""
    for name, schema in SCHEMA_REGISTRY.items():
        register_schema(ctx, name, schema)


__all__ = [
    "AST_FILES_SCHEMA",
    "BYTECODE_FILES_SCHEMA",
    "LIBCST_FILES_SCHEMA",
    "SCHEMA_REGISTRY",
    "SCIP_INDEX_SCHEMA",
    "SYMTABLE_FILES_SCHEMA",
    "TREE_SITTER_FILES_SCHEMA",
    "has_schema",
    "register_all_schemas",
    "register_schema",
    "schema_for",
    "schema_names",
    "schema_registry",
]
