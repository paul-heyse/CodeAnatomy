"""SQL fragment helpers for nested DataFusion schemas."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SqlFragment:
    """Named SQL fragment for on-demand nested table projections."""

    name: str
    sql: str


def _map_value(map_name: str, key: str) -> str:
    return f"{map_name}['{key}']"


def _map_cast(map_name: str, key: str, dtype: str) -> str:
    return f"CAST({_map_value(map_name, key)} AS {dtype})"


def _map_bool(map_name: str, key: str) -> str:
    value = _map_value(map_name, key)
    return (
        "CASE "
        f"WHEN LOWER({value}) = 'true' THEN TRUE "
        f"WHEN LOWER({value}) = 'false' THEN FALSE "
        "ELSE NULL END"
    )


def _hash_expr(prefix: str, *values: str) -> str:
    joined = ", ".join(values)
    return f"prefixed_hash64('{prefix}', concat_ws(':', {joined}))"


def libcst_parse_manifest_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST parse manifest rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    row = "manifest"
    return f"""
    SELECT
      {row}['file_id'] AS file_id,
      {row}['path'] AS path,
      {row}['file_sha256'] AS file_sha256,
      {row}['encoding'] AS encoding,
      {row}['default_indent'] AS default_indent,
      {row}['default_newline'] AS default_newline,
      {row}['has_trailing_newline'] AS has_trailing_newline,
      {row}['future_imports'] AS future_imports,
      {row}['module_name'] AS module_name,
      {row}['package_name'] AS package_name
    FROM {table} AS files
    CROSS JOIN unnest(files.parse_manifest) AS {row}
    """


def libcst_parse_errors_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST parse error rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    row = "err"
    return f"""
    SELECT
      {row}['file_id'] AS file_id,
      {row}['path'] AS path,
      {row}['file_sha256'] AS file_sha256,
      {row}['error_type'] AS error_type,
      {row}['message'] AS message,
      {row}['raw_line'] AS raw_line,
      {row}['raw_column'] AS raw_column,
      {row}['line_base'] AS line_base,
      {row}['col_unit'] AS col_unit,
      {row}['end_exclusive'] AS end_exclusive
    FROM {table} AS files
    CROSS JOIN unnest(files.parse_errors) AS {row}
    """


def libcst_name_refs_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST name reference rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    row = "ref"
    file_id = f"{row}['file_id']"
    bstart = f"{row}['bstart']"
    bend = f"{row}['bend']"
    return f"""
    SELECT
      {file_id} AS file_id,
      {row}['path'] AS path,
      {row}['file_sha256'] AS file_sha256,
      {row}['name'] AS name,
      {row}['expr_ctx'] AS expr_ctx,
      {bstart} AS bstart,
      {bend} AS bend,
      {_hash_expr("cst_name_ref", file_id, bstart, bend)} AS name_ref_id
    FROM {table} AS files
    CROSS JOIN unnest(files.name_refs) AS {row}
    """


def libcst_imports_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST import rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    row = "imp"
    file_id = f"{row}['file_id']"
    alias_bstart = f"{row}['alias_bstart']"
    alias_bend = f"{row}['alias_bend']"
    return f"""
    SELECT
      {file_id} AS file_id,
      {row}['path'] AS path,
      {row}['file_sha256'] AS file_sha256,
      {row}['kind'] AS kind,
      {row}['module'] AS module,
      {row}['relative_level'] AS relative_level,
      {row}['name'] AS name,
      {row}['asname'] AS asname,
      {row}['is_star'] AS is_star,
      {row}['stmt_bstart'] AS stmt_bstart,
      {row}['stmt_bend'] AS stmt_bend,
      {alias_bstart} AS alias_bstart,
      {alias_bend} AS alias_bend,
      {_hash_expr("cst_import", file_id, f"{row}['kind']", alias_bstart, alias_bend)}
        AS import_id,
      {_hash_expr("span", file_id, alias_bstart, alias_bend)} AS span_id
    FROM {table} AS files
    CROSS JOIN unnest(files.imports) AS {row}
    """


def libcst_callsites_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST callsite rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    row = "call"
    file_id = f"{row}['file_id']"
    call_bstart = f"{row}['call_bstart']"
    call_bend = f"{row}['call_bend']"
    return f"""
    SELECT
      {file_id} AS file_id,
      {row}['path'] AS path,
      {row}['file_sha256'] AS file_sha256,
      {call_bstart} AS call_bstart,
      {call_bend} AS call_bend,
      {row}['callee_bstart'] AS callee_bstart,
      {row}['callee_bend'] AS callee_bend,
      {row}['callee_shape'] AS callee_shape,
      {row}['callee_text'] AS callee_text,
      {row}['arg_count'] AS arg_count,
      {row}['callee_dotted'] AS callee_dotted,
      {row}['callee_qnames'] AS callee_qnames,
      {_hash_expr("cst_call", file_id, call_bstart, call_bend)} AS call_id,
      {_hash_expr("span", file_id, call_bstart, call_bend)} AS span_id
    FROM {table} AS files
    CROSS JOIN unnest(files.callsites) AS {row}
    """


def libcst_defs_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST definition rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    row = "defs"
    file_id = f"{row}['file_id']"
    def_bstart = f"{row}['def_bstart']"
    def_bend = f"{row}['def_bend']"
    container_bstart = f"{row}['container_def_bstart']"
    container_bend = f"{row}['container_def_bend']"
    return f"""
    SELECT
      {file_id} AS file_id,
      {row}['path'] AS path,
      {row}['file_sha256'] AS file_sha256,
      {row}['container_def_kind'] AS container_def_kind,
      {container_bstart} AS container_def_bstart,
      {container_bend} AS container_def_bend,
      {row}['kind'] AS kind,
      {row}['name'] AS name,
      {def_bstart} AS def_bstart,
      {def_bend} AS def_bend,
      {row}['name_bstart'] AS name_bstart,
      {row}['name_bend'] AS name_bend,
      {row}['qnames'] AS qnames,
      {_hash_expr("cst_def", file_id, f"{row}['kind']", def_bstart, def_bend)} AS def_id,
      {_hash_expr(
        "cst_def",
        file_id,
        f"{row}['container_def_kind']",
        container_bstart,
        container_bend,
      )} AS container_def_id,
      {_hash_expr("span", file_id, def_bstart, def_bend)} AS span_id
    FROM {table} AS files
    CROSS JOIN unnest(files.defs) AS {row}
    """


def libcst_type_exprs_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST type expression rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    row = "expr"
    file_id = f"{row}['file_id']"
    bstart = f"{row}['bstart']"
    bend = f"{row}['bend']"
    owner_bstart = f"{row}['owner_def_bstart']"
    owner_bend = f"{row}['owner_def_bend']"
    return f"""
    SELECT
      {file_id} AS file_id,
      {row}['path'] AS path,
      {row}['file_sha256'] AS file_sha256,
      {row}['owner_def_kind'] AS owner_def_kind,
      {owner_bstart} AS owner_def_bstart,
      {owner_bend} AS owner_def_bend,
      {row}['param_name'] AS param_name,
      {row}['expr_kind'] AS expr_kind,
      {row}['expr_role'] AS expr_role,
      {bstart} AS bstart,
      {bend} AS bend,
      {row}['expr_text'] AS expr_text,
      {_hash_expr("cst_type_expr", f"{row}['path']", bstart, bend)} AS type_expr_id,
      {_hash_expr("cst_def", file_id, f"{row}['owner_def_kind']", owner_bstart, owner_bend)}
        AS owner_def_id
    FROM {table} AS files
    CROSS JOIN unnest(files.type_exprs) AS {row}
    """


def ast_nodes_sql(table: str = "ast_files_v1") -> str:
    """Return SQL for AST node rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      node['ast_id'] AS ast_id,
      node['parent_ast_id'] AS parent_ast_id,
      node['kind'] AS kind,
      node['name'] AS name,
      node['value'] AS value_repr,
      (node['span']['start']['line0'] + 1) AS lineno,
      node['span']['start']['col'] AS col_offset,
      (node['span']['end']['line0'] + 1) AS end_lineno,
      node['span']['end']['col'] AS end_col_offset,
      CAST(1 AS INT) AS line_base,
      node['span']['col_unit'] AS col_unit,
      node['span']['end_exclusive'] AS end_exclusive,
      node['span']['byte_span']['byte_start'] AS bstart,
      (node['span']['byte_span']['byte_start'] + node['span']['byte_span']['byte_len']) AS bend
    FROM {table} AS files
    CROSS JOIN unnest(files.nodes) AS node
    """


def ast_edges_sql(table: str = "ast_files_v1") -> str:
    """Return SQL for AST edge rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      edge['src'] AS src,
      edge['dst'] AS dst,
      edge['kind'] AS kind,
      edge['slot'] AS slot,
      edge['idx'] AS idx
    FROM {table} AS files
    CROSS JOIN unnest(files.edges) AS edge
    """


def ast_errors_sql(table: str = "ast_files_v1") -> str:
    """Return SQL for AST parse error rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      err['error_type'] AS error_type,
      err['message'] AS message,
      (err['span']['start']['line0'] + 1) AS lineno,
      err['span']['start']['col'] AS col_offset,
      (err['span']['end']['line0'] + 1) AS end_lineno,
      err['span']['end']['col'] AS end_col_offset,
      CAST(1 AS INT) AS line_base,
      err['span']['col_unit'] AS col_unit,
      err['span']['end_exclusive'] AS end_exclusive
    FROM {table} AS files
    CROSS JOIN unnest(files.errors) AS err
    """


def tree_sitter_nodes_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter node rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      node['node_id'] AS ts_node_id,
      node['parent_id'] AS parent_ts_id,
      node['kind'] AS ts_type,
      node['span']['byte_span']['byte_start'] AS start_byte,
      (node['span']['byte_span']['byte_start'] + node['span']['byte_span']['byte_len']) AS end_byte,
      node['flags']['is_named'] AS is_named,
      node['flags']['has_error'] AS has_error,
      node['flags']['is_error'] AS is_error,
      node['flags']['is_missing'] AS is_missing
    FROM {table} AS files
    CROSS JOIN unnest(files.nodes) AS node
    """


def tree_sitter_errors_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter parse error rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      err['error_id'] AS ts_error_id,
      err['node_id'] AS ts_node_id,
      err['span']['byte_span']['byte_start'] AS start_byte,
      (err['span']['byte_span']['byte_start'] + err['span']['byte_span']['byte_len']) AS end_byte,
      CAST(TRUE AS BOOLEAN) AS is_error
    FROM {table} AS files
    CROSS JOIN unnest(files.errors) AS err
    """


def tree_sitter_missing_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter missing node rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      miss['missing_id'] AS ts_missing_id,
      miss['node_id'] AS ts_node_id,
      miss['span']['byte_span']['byte_start'] AS start_byte,
      (miss['span']['byte_span']['byte_start'] + miss['span']['byte_span']['byte_len']) AS end_byte,
      CAST(TRUE AS BOOLEAN) AS is_missing
    FROM {table} AS files
    CROSS JOIN unnest(files.missing) AS miss
    """


def symtable_scopes_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable scope rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      blk['block_id'] AS table_id,
      blk['block_type'] AS scope_type,
      blk['name'] AS scope_name,
      blk['lineno1'] AS lineno,
      blk['parent_block_id'] AS parent_table_id
    FROM {table} AS files
    CROSS JOIN unnest(files.blocks) AS blk
    """


def symtable_symbols_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable symbol rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      blk['block_id'] AS table_id,
      sym['name'] AS symbol_name,
      sym['flags']['is_referenced'] AS is_referenced,
      sym['flags']['is_imported'] AS is_imported,
      sym['flags']['is_parameter'] AS is_parameter,
      sym['flags']['is_type_parameter'] AS is_type_parameter,
      sym['flags']['is_global'] AS is_global,
      sym['flags']['is_nonlocal'] AS is_nonlocal,
      sym['flags']['is_declared_global'] AS is_declared_global,
      sym['flags']['is_local'] AS is_local,
      sym['flags']['is_annotated'] AS is_annotated,
      sym['flags']['is_free'] AS is_free,
      sym['flags']['is_assigned'] AS is_assigned,
      sym['flags']['is_namespace'] AS is_namespace
    FROM {table} AS files
    CROSS JOIN unnest(files.blocks) AS blk
    CROSS JOIN unnest(blk['symbols']) AS sym
    """


def symtable_scope_edges_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable scope edge rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      blk['parent_block_id'] AS parent_table_id,
      blk['block_id'] AS child_table_id
    FROM {table} AS files
    CROSS JOIN unnest(files.blocks) AS blk
    """


def scip_metadata_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP metadata rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      idx.index_id AS index_id,
      idx.metadata['protocol_version'] AS protocol_version,
      idx.metadata['tool_info']['name'] AS tool_name,
      idx.metadata['tool_info']['version'] AS tool_version,
      idx.metadata['project_root'] AS project_root,
      idx.metadata['text_document_encoding'] AS text_document_encoding
    FROM {table} AS idx
    """


def scip_documents_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP document rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      idx.index_id AS index_id,
      prefixed_hash64('scip_doc', doc['relative_path']) AS document_id,
      doc['relative_path'] AS path,
      doc['language'] AS language,
      doc['position_encoding'] AS position_encoding
    FROM {table} AS idx
    CROSS JOIN unnest(idx.documents) AS doc
    """


def scip_occurrences_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP occurrence rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      prefixed_hash64('scip_doc', doc['relative_path']) AS document_id,
      doc['relative_path'] AS path,
      occ['symbol'] AS symbol,
      occ['symbol_roles'] AS symbol_roles,
      occ['syntax_kind'] AS syntax_kind,
      occ['range']['start']['line0'] AS start_line,
      occ['range']['start']['col'] AS start_char,
      occ['range']['end']['line0'] AS end_line,
      occ['range']['end']['col'] AS end_char,
      cardinality(occ['range_raw']) AS range_len,
      occ['enclosing_range']['start']['line0'] AS enc_start_line,
      occ['enclosing_range']['start']['col'] AS enc_start_char,
      occ['enclosing_range']['end']['line0'] AS enc_end_line,
      occ['enclosing_range']['end']['col'] AS enc_end_char,
      cardinality(occ['enclosing_range_raw']) AS enc_range_len,
      CAST(0 AS INT) AS line_base,
      occ['range']['col_unit'] AS col_unit,
      occ['range']['end_exclusive'] AS end_exclusive
    FROM {table} AS idx
    CROSS JOIN unnest(idx.documents) AS doc
    CROSS JOIN unnest(doc['occurrences']) AS occ
    """


def scip_symbol_information_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP symbol info rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      sym['symbol'] AS symbol,
      sym['display_name'] AS display_name,
      sym['kind'] AS kind,
      sym['enclosing_symbol'] AS enclosing_symbol,
      sym['documentation'] AS documentation,
      sym['signature_documentation'] AS signature_documentation
    FROM {table} AS idx
    CROSS JOIN unnest(idx.symbols) AS sym
    """


def scip_external_symbol_information_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP external symbol info rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      sym['symbol'] AS symbol,
      sym['display_name'] AS display_name,
      sym['kind'] AS kind,
      sym['enclosing_symbol'] AS enclosing_symbol,
      sym['documentation'] AS documentation,
      sym['signature_documentation'] AS signature_documentation
    FROM {table} AS idx
    CROSS JOIN unnest(idx.external_symbols) AS sym
    """


def scip_symbol_relationships_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP symbol relationship rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      sym['symbol'] AS symbol,
      rel['symbol'] AS related_symbol,
      rel['is_reference'] AS is_reference,
      rel['is_implementation'] AS is_implementation,
      rel['is_type_definition'] AS is_type_definition,
      rel['is_definition'] AS is_definition
    FROM {table} AS idx
    CROSS JOIN unnest(idx.symbols) AS sym
    CROSS JOIN unnest(sym['relationships']) AS rel
    """


def scip_diagnostics_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP diagnostics rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      prefixed_hash64('scip_doc', doc['relative_path']) AS document_id,
      doc['relative_path'] AS path,
      diag['severity'] AS severity,
      diag['code'] AS code,
      diag['message'] AS message,
      diag['source'] AS source,
      diag['tags'] AS tags,
      occ['range']['start']['line0'] AS start_line,
      occ['range']['start']['col'] AS start_char,
      occ['range']['end']['line0'] AS end_line,
      occ['range']['end']['col'] AS end_char,
      CAST(0 AS INT) AS line_base,
      occ['range']['col_unit'] AS col_unit,
      occ['range']['end_exclusive'] AS end_exclusive
    FROM {table} AS idx
    CROSS JOIN unnest(idx.documents) AS doc
    CROSS JOIN unnest(doc['occurrences']) AS occ
    CROSS JOIN unnest(occ['diagnostics']) AS diag
    """


def bytecode_code_units_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode code-unit rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      code['code_id'] AS code_unit_id,
      code['qualname'] AS qualpath,
      code['name'] AS co_name,
      code['firstlineno1'] AS firstlineno,
      code['argcount'] AS argcount,
      code['posonlyargcount'] AS posonlyargcount,
      code['kwonlyargcount'] AS kwonlyargcount,
      code['nlocals'] AS nlocals,
      code['flags'] AS flags,
      code['stacksize'] AS stacksize,
      code['code_len'] AS code_len,
      code['varnames'] AS varnames,
      code['freevars'] AS freevars,
      code['cellvars'] AS cellvars,
      code['names'] AS names
    FROM {table} AS files
    CROSS JOIN unnest(files.code_objects) AS code
    """


def bytecode_instructions_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode instruction rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    instr = "instr"
    code_id = "code['code_id']"
    instr_index = _map_cast(f"{instr}['attrs']", "instr_index", "INT")
    offset = f"{instr}['offset']"
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      {code_id} AS code_unit_id,
      {instr_index} AS instr_index,
      {offset} AS offset,
      {instr}['opname'] AS opname,
      {instr}['opcode'] AS opcode,
      {instr}['arg'] AS arg,
      {instr}['argrepr'] AS argrepr,
      {instr}['is_jump_target'] AS is_jump_target,
      {instr}['jump_target'] AS jump_target_offset,
      {instr}['span']['start']['line0'] AS pos_start_line,
      {instr}['span']['end']['line0'] AS pos_end_line,
      {instr}['span']['start']['col'] AS pos_start_col,
      {instr}['span']['end']['col'] AS pos_end_col,
      {instr}['span']['col_unit'] AS col_unit,
      {instr}['span']['end_exclusive'] AS end_exclusive,
      {_map_value(f"{instr}['attrs']", "argval_str")} AS argval_str,
      {_map_cast(f"{instr}['attrs']", "starts_line", "INT")} AS starts_line,
      {_hash_expr("bc_instr", code_id, instr_index, offset)} AS instr_id
    FROM {table} AS files
    CROSS JOIN unnest(files.code_objects) AS code
    CROSS JOIN unnest(code['instructions']) AS {instr}
    """


def bytecode_exception_table_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode exception table rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    exc = "exc"
    code_id = "code['code_id']"
    exc_index = f"{exc}['exc_index']"
    start_offset = f"{exc}['start_offset']"
    end_offset = f"{exc}['end_offset']"
    target_offset = f"{exc}['target_offset']"
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      {code_id} AS code_unit_id,
      {exc_index} AS exc_index,
      {start_offset} AS start_offset,
      {end_offset} AS end_offset,
      {target_offset} AS target_offset,
      {exc}['depth'] AS depth,
      {exc}['lasti'] AS lasti,
      {_hash_expr("bc_exc", code_id, exc_index, start_offset, end_offset, target_offset)}
        AS exc_entry_id
    FROM {table} AS files
    CROSS JOIN unnest(files.code_objects) AS code
    CROSS JOIN unnest(code['exception_table']) AS {exc}
    """


def bytecode_blocks_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode basic block rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    blk = "blk"
    code_id = "code['code_id']"
    start_offset = f"{blk}['start_offset']"
    end_offset = f"{blk}['end_offset']"
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      {code_id} AS code_unit_id,
      {start_offset} AS start_offset,
      {end_offset} AS end_offset,
      {blk}['kind'] AS kind,
      {_hash_expr("bc_block", code_id, start_offset, end_offset)} AS block_id
    FROM {table} AS files
    CROSS JOIN unnest(files.code_objects) AS code
    CROSS JOIN unnest(code['blocks']) AS {blk}
    """


def bytecode_cfg_edges_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode CFG edge rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    edge = "edge"
    code_id = "code['code_id']"
    src_start = f"{edge}['src_block_start']"
    src_end = f"{edge}['src_block_end']"
    dst_start = f"{edge}['dst_block_start']"
    dst_end = f"{edge}['dst_block_end']"
    cond_index = f"{edge}['cond_instr_index']"
    cond_offset = f"{edge}['cond_instr_offset']"
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      {code_id} AS code_unit_id,
      {src_start} AS src_block_start,
      {src_end} AS src_block_end,
      {dst_start} AS dst_block_start,
      {dst_end} AS dst_block_end,
      {edge}['kind'] AS kind,
      {edge}['edge_key'] AS edge_key,
      {cond_index} AS cond_instr_index,
      {cond_offset} AS cond_instr_offset,
      {edge}['exc_index'] AS exc_index,
      {_hash_expr("bc_block", code_id, src_start, src_end)} AS src_block_id,
      {_hash_expr("bc_block", code_id, dst_start, dst_end)} AS dst_block_id,
      {_hash_expr("bc_instr", code_id, cond_index, cond_offset)} AS cond_instr_id,
      {_hash_expr(
        "bc_edge",
        code_id,
        _hash_expr("bc_block", code_id, src_start, src_end),
        _hash_expr("bc_block", code_id, dst_start, dst_end),
        f"{edge}['kind']",
        f"{edge}['edge_key']",
        f"{edge}['exc_index']",
      )} AS edge_id
    FROM {table} AS files
    CROSS JOIN unnest(files.code_objects) AS code
    CROSS JOIN unnest(code['cfg_edges']) AS {edge}
    """


def bytecode_errors_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode parse error rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    err = "err"
    return f"""
    SELECT
      files.file_id AS file_id,
      files.path AS path,
      {err}['error_type'] AS error_type,
      {err}['message'] AS message
    FROM {table} AS files
    CROSS JOIN unnest(files.errors) AS {err}
    """


__all__ = [
    "SqlFragment",
    "ast_edges_sql",
    "ast_errors_sql",
    "ast_nodes_sql",
    "bytecode_blocks_sql",
    "bytecode_cfg_edges_sql",
    "bytecode_code_units_sql",
    "bytecode_errors_sql",
    "bytecode_exception_table_sql",
    "bytecode_instructions_sql",
    "libcst_callsites_sql",
    "libcst_defs_sql",
    "libcst_imports_sql",
    "libcst_name_refs_sql",
    "libcst_parse_errors_sql",
    "libcst_parse_manifest_sql",
    "libcst_type_exprs_sql",
    "scip_diagnostics_sql",
    "scip_documents_sql",
    "scip_external_symbol_information_sql",
    "scip_metadata_sql",
    "scip_occurrences_sql",
    "scip_symbol_information_sql",
    "scip_symbol_relationships_sql",
    "symtable_scope_edges_sql",
    "symtable_scopes_sql",
    "symtable_symbols_sql",
    "tree_sitter_errors_sql",
    "tree_sitter_missing_sql",
    "tree_sitter_nodes_sql",
]
