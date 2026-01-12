"""HashSpec registry for extract identifiers."""

from __future__ import annotations

from arrowdsl.core.ids import HashSpec


CST_NAME_REF_ID_SPEC = HashSpec(
    prefix="cst_name_ref",
    cols=("file_id", "bstart", "bend"),
    as_string=True,
    out_col="name_ref_id",
)

CST_IMPORT_ID_SPEC = HashSpec(
    prefix="cst_import",
    cols=("file_id", "kind", "alias_bstart", "alias_bend"),
    as_string=True,
    out_col="import_id",
)

CST_CALL_ID_SPEC = HashSpec(
    prefix="cst_call",
    cols=("file_id", "call_bstart", "call_bend"),
    as_string=True,
    out_col="call_id",
)

CST_DEF_ID_SPEC = HashSpec(
    prefix="cst_def",
    cols=("file_id", "kind", "def_bstart", "def_bend"),
    as_string=True,
    out_col="def_id",
)

CST_CONTAINER_DEF_ID_SPEC = HashSpec(
    prefix="cst_def",
    cols=("file_id", "container_def_kind", "container_def_bstart", "container_def_bend"),
    as_string=True,
    out_col="container_def_id",
)

CST_TYPE_EXPR_ID_SPEC = HashSpec(
    prefix="cst_type_expr",
    cols=("path", "bstart", "bend"),
    as_string=True,
    out_col="type_expr_id",
)

CST_OWNER_DEF_ID_SPEC = HashSpec(
    prefix="cst_def",
    cols=("file_id", "owner_def_kind", "owner_def_bstart", "owner_def_bend"),
    as_string=True,
    out_col="owner_def_id",
)

TS_NODE_ID_SPEC = HashSpec(
    prefix="ts_node",
    cols=("path", "start_byte", "end_byte", "ts_type"),
    as_string=True,
    out_col="ts_node_id",
)

TS_PARENT_NODE_ID_SPEC = HashSpec(
    prefix="ts_node",
    cols=("path", "parent_start_byte", "parent_end_byte", "parent_ts_type"),
    as_string=True,
    out_col="parent_ts_id",
)

TS_ERROR_ID_SPEC = HashSpec(
    prefix="ts_error",
    cols=("path", "start_byte", "end_byte"),
    as_string=True,
    out_col="ts_error_id",
)

TS_MISSING_ID_SPEC = HashSpec(
    prefix="ts_missing",
    cols=("path", "start_byte", "end_byte"),
    as_string=True,
    out_col="ts_missing_id",
)

BC_CODE_UNIT_ID_SPEC = HashSpec(
    prefix="bc_code",
    cols=("file_id", "qualpath", "firstlineno", "co_name"),
    as_string=True,
    out_col="code_unit_id",
)

BC_PARENT_CODE_UNIT_ID_SPEC = HashSpec(
    prefix="bc_code",
    cols=("file_id", "parent_qualpath", "parent_firstlineno", "parent_co_name"),
    as_string=True,
    out_col="parent_code_unit_id",
)

BC_INSTR_ID_SPEC = HashSpec(
    prefix="bc_instr",
    cols=("code_unit_id", "instr_index", "offset"),
    as_string=True,
    out_col="instr_id",
)

BC_EXC_ENTRY_ID_SPEC = HashSpec(
    prefix="bc_exc",
    cols=("code_unit_id", "exc_index", "start_offset", "end_offset", "target_offset"),
    as_string=True,
    out_col="exc_entry_id",
)

BC_BLOCK_ID_SPEC = HashSpec(
    prefix="bc_block",
    cols=("code_unit_id", "start_offset", "end_offset"),
    as_string=True,
    out_col="block_id",
)

BC_SRC_BLOCK_ID_SPEC = HashSpec(
    prefix="bc_block",
    cols=("code_unit_id", "src_block_start", "src_block_end"),
    as_string=True,
    out_col="src_block_id",
)

BC_DST_BLOCK_ID_SPEC = HashSpec(
    prefix="bc_block",
    cols=("code_unit_id", "dst_block_start", "dst_block_end"),
    as_string=True,
    out_col="dst_block_id",
)

BC_COND_INSTR_ID_SPEC = HashSpec(
    prefix="bc_instr",
    cols=("code_unit_id", "cond_instr_index", "cond_instr_offset"),
    as_string=True,
    out_col="cond_instr_id",
)

BC_EDGE_ID_SPEC = HashSpec(
    prefix="bc_edge",
    cols=("code_unit_id", "src_block_id", "dst_block_id", "kind", "edge_key", "exc_index"),
    as_string=True,
    out_col="edge_id",
)

SYM_SCOPE_ID_SPEC = HashSpec(
    prefix="sym_scope",
    cols=("file_id", "table_id", "scope_type", "scope_name", "lineno"),
    as_string=True,
    out_col="scope_id",
)

SYM_SYMBOL_ROW_ID_SPEC = HashSpec(
    prefix="sym_symbol",
    cols=("scope_id", "name"),
    as_string=True,
    out_col="symbol_row_id",
)

SYM_SCOPE_EDGE_ID_SPEC = HashSpec(
    prefix="sym_scope_edge",
    cols=("parent_scope_id", "child_scope_id"),
    as_string=True,
    out_col="edge_id",
)

SYM_NS_SYMBOL_ROW_ID_SPEC = HashSpec(
    prefix="sym_symbol",
    cols=("scope_id", "symbol_name"),
    as_string=True,
    out_col="symbol_row_id",
)

SYM_NS_EDGE_ID_SPEC = HashSpec(
    prefix="sym_ns_edge",
    cols=("symbol_row_id", "child_scope_id"),
    as_string=True,
    out_col="edge_id",
)

RT_OBJECT_ID_SPEC = HashSpec(
    prefix="rt_obj",
    cols=("module", "qualname"),
    as_string=True,
    out_col="rt_id",
)

RT_SIGNATURE_ID_SPEC = HashSpec(
    prefix="rt_sig",
    cols=("rt_id", "signature"),
    as_string=True,
    out_col="sig_id",
)

RT_PARAM_ID_SPEC = HashSpec(
    prefix="rt_param",
    cols=("sig_id", "name"),
    as_string=True,
    out_col="param_id",
)

RT_MEMBER_ID_SPEC = HashSpec(
    prefix="rt_member",
    cols=("rt_id", "name"),
    as_string=True,
    out_col="member_id",
)


def repo_file_id_spec(repo_id: str | None) -> HashSpec:
    """Return the repo file id HashSpec with optional repo-id literal.

    Returns
    -------
    HashSpec
        HashSpec for repo file identifiers.
    """
    extra = (repo_id,) if repo_id else ()
    return HashSpec(
        prefix="file",
        cols=("path",),
        extra_literals=extra,
        as_string=True,
        out_col="file_id",
    )


__all__ = [
    "BC_BLOCK_ID_SPEC",
    "BC_CODE_UNIT_ID_SPEC",
    "BC_COND_INSTR_ID_SPEC",
    "BC_DST_BLOCK_ID_SPEC",
    "BC_EDGE_ID_SPEC",
    "BC_EXC_ENTRY_ID_SPEC",
    "BC_INSTR_ID_SPEC",
    "BC_PARENT_CODE_UNIT_ID_SPEC",
    "BC_SRC_BLOCK_ID_SPEC",
    "CST_CALL_ID_SPEC",
    "CST_CONTAINER_DEF_ID_SPEC",
    "CST_DEF_ID_SPEC",
    "CST_IMPORT_ID_SPEC",
    "CST_NAME_REF_ID_SPEC",
    "CST_OWNER_DEF_ID_SPEC",
    "CST_TYPE_EXPR_ID_SPEC",
    "RT_MEMBER_ID_SPEC",
    "RT_OBJECT_ID_SPEC",
    "RT_PARAM_ID_SPEC",
    "RT_SIGNATURE_ID_SPEC",
    "SYM_NS_EDGE_ID_SPEC",
    "SYM_NS_SYMBOL_ROW_ID_SPEC",
    "SYM_SCOPE_EDGE_ID_SPEC",
    "SYM_SCOPE_ID_SPEC",
    "SYM_SYMBOL_ROW_ID_SPEC",
    "TS_ERROR_ID_SPEC",
    "TS_MISSING_ID_SPEC",
    "TS_NODE_ID_SPEC",
    "TS_PARENT_NODE_ID_SPEC",
    "repo_file_id_spec",
]
