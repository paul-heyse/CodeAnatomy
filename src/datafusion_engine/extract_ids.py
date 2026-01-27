"""Hash expression spec registry and extractor ID helpers."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion_engine.hashing import HashExprSpec, HashExprSpecOptions
from datafusion_engine.hashing import hash_expr_spec_factory as hash_spec_factory

_HASH_SPECS: Mapping[str, HashExprSpec] = {
    "span_id": hash_spec_factory(
        prefix="span",
        cols=("file_id", "bstart", "bend"),
        out_col="span_id",
    ),
    "cst_import_span_id": hash_spec_factory(
        prefix="span",
        cols=("file_id", "alias_bstart", "alias_bend"),
        out_col="span_id",
    ),
    "cst_call_span_id": hash_spec_factory(
        prefix="span",
        cols=("file_id", "call_bstart", "call_bend"),
        out_col="span_id",
    ),
    "cst_def_span_id": hash_spec_factory(
        prefix="span",
        cols=("file_id", "def_bstart", "def_bend"),
        out_col="span_id",
    ),
    "ts_span_id": hash_spec_factory(
        prefix="span",
        cols=("file_id", "start_byte", "end_byte"),
        out_col="span_id",
    ),
    "cst_ref_id": hash_spec_factory(
        prefix="cst_ref",
        cols=("file_id", "bstart", "bend"),
        out_col="ref_id",
    ),
    "cst_import_id": hash_spec_factory(
        prefix="cst_import",
        cols=("file_id", "kind", "alias_bstart", "alias_bend"),
        out_col="import_id",
    ),
    "cst_call_id": hash_spec_factory(
        prefix="cst_call",
        cols=("file_id", "call_bstart", "call_bend"),
        out_col="call_id",
    ),
    "cst_def_id": hash_spec_factory(
        prefix="cst_def",
        cols=("file_id", "kind", "def_bstart", "def_bend"),
        out_col="def_id",
    ),
    "cst_container_def_id": hash_spec_factory(
        prefix="cst_def",
        cols=("file_id", "container_def_kind", "container_def_bstart", "container_def_bend"),
        out_col="container_def_id",
    ),
    "cst_type_expr_id": hash_spec_factory(
        prefix="cst_type_expr",
        cols=("path", "bstart", "bend"),
        out_col="type_expr_id",
    ),
    "cst_owner_def_id": hash_spec_factory(
        prefix="cst_def",
        cols=("file_id", "owner_def_kind", "owner_def_bstart", "owner_def_bend"),
        out_col="owner_def_id",
    ),
    "ts_node_id": hash_spec_factory(
        prefix="ts_node",
        cols=("path", "start_byte", "end_byte", "ts_type"),
        out_col="ts_node_id",
    ),
    "ts_parent_node_id": hash_spec_factory(
        prefix="ts_node",
        cols=("path", "parent_start_byte", "parent_end_byte", "parent_ts_type"),
        out_col="parent_ts_id",
    ),
    "ts_error_id": hash_spec_factory(
        prefix="ts_error",
        cols=("path", "start_byte", "end_byte"),
        out_col="ts_error_id",
    ),
    "ts_missing_id": hash_spec_factory(
        prefix="ts_missing",
        cols=("path", "start_byte", "end_byte"),
        out_col="ts_missing_id",
    ),
    "bc_code_unit_id": hash_spec_factory(
        prefix="bc_code",
        cols=("file_id", "qualpath", "firstlineno", "co_name"),
        out_col="code_unit_id",
    ),
    "bc_parent_code_unit_id": hash_spec_factory(
        prefix="bc_code",
        cols=("file_id", "parent_qualpath", "parent_firstlineno", "parent_co_name"),
        out_col="parent_code_unit_id",
    ),
    "bc_instr_id": hash_spec_factory(
        prefix="bc_instr",
        cols=("code_unit_id", "instr_index", "offset"),
        out_col="instr_id",
    ),
    "bc_exc_entry_id": hash_spec_factory(
        prefix="bc_exc",
        cols=("code_unit_id", "exc_index", "start_offset", "end_offset", "target_offset"),
        out_col="exc_entry_id",
    ),
    "bc_block_id": hash_spec_factory(
        prefix="bc_block",
        cols=("code_unit_id", "start_offset", "end_offset"),
        out_col="block_id",
    ),
    "bc_src_block_id": hash_spec_factory(
        prefix="bc_block",
        cols=("code_unit_id", "src_block_start", "src_block_end"),
        out_col="src_block_id",
    ),
    "bc_dst_block_id": hash_spec_factory(
        prefix="bc_block",
        cols=("code_unit_id", "dst_block_start", "dst_block_end"),
        out_col="dst_block_id",
    ),
    "bc_cond_instr_id": hash_spec_factory(
        prefix="bc_instr",
        cols=("code_unit_id", "cond_instr_index", "cond_instr_offset"),
        out_col="cond_instr_id",
    ),
    "bc_edge_id": hash_spec_factory(
        prefix="bc_edge",
        cols=("code_unit_id", "src_block_id", "dst_block_id", "kind", "edge_key", "exc_index"),
        out_col="edge_id",
    ),
    "sym_scope_id": hash_spec_factory(
        prefix="sym_scope",
        cols=("file_id", "table_id", "scope_type", "scope_name", "lineno"),
        out_col="scope_id",
    ),
    "sym_symbol_row_id": hash_spec_factory(
        prefix="sym_symbol",
        cols=("scope_id", "name"),
        out_col="symbol_row_id",
    ),
    "sym_scope_edge_id": hash_spec_factory(
        prefix="sym_scope_edge",
        cols=("parent_scope_id", "child_scope_id"),
        out_col="edge_id",
    ),
    "sym_ns_symbol_row_id": hash_spec_factory(
        prefix="sym_symbol",
        cols=("scope_id", "symbol_name"),
        out_col="symbol_row_id",
    ),
    "sym_ns_edge_id": hash_spec_factory(
        prefix="sym_ns_edge",
        cols=("symbol_row_id", "child_scope_id"),
        out_col="edge_id",
    ),
    "rt_object_id": hash_spec_factory(
        prefix="rt_obj",
        cols=("module", "qualname"),
        out_col="rt_id",
    ),
    "rt_signature_id": hash_spec_factory(
        prefix="rt_sig",
        cols=("rt_id", "signature"),
        out_col="sig_id",
    ),
    "rt_param_id": hash_spec_factory(
        prefix="rt_param",
        cols=("sig_id", "name"),
        out_col="param_id",
    ),
    "rt_member_id": hash_spec_factory(
        prefix="rt_member",
        cols=("rt_id", "name"),
        out_col="member_id",
    ),
}


def repo_file_id_spec(repo_id: str | None) -> HashExprSpec:
    """Return the repo file id hash spec with optional repo-id literal.

    Returns
    -------
    HashExprSpec
        Hash spec for repository file ids.
    """
    extra = (repo_id,) if repo_id else ()
    return hash_spec_factory(
        prefix="file",
        cols=("path",),
        out_col="file_id",
        options=HashExprSpecOptions(extra_literals=extra),
    )


def hash_spec(name: str, *, repo_id: str | None = None) -> HashExprSpec:
    """Return the hash spec for a known identifier.

    Returns
    -------
    HashExprSpec
        Hash spec for the identifier name.
    """
    if name == "repo_file_id":
        return repo_file_id_spec(repo_id)
    return _HASH_SPECS[name]


__all__ = ["hash_spec", "repo_file_id_spec"]
