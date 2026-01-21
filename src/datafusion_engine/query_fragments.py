"""SQL fragment helpers for nested DataFusion schemas."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from datafusion import SessionContext

from datafusion_engine.schema_registry import nested_base_sql
from schema_spec.view_specs import ViewSpec, view_spec_from_sql


@dataclass(frozen=True)
class SqlFragment:
    """Named SQL fragment for on-demand nested table projections."""

    name: str
    sql: str


def _map_value(map_name: str, key: str) -> str:
    return f"{map_name}['{key}']"


def _named_struct(fields: Sequence[tuple[str, str]]) -> str:
    parts = ", ".join(f"'{name}', {expr}" for name, expr in fields)
    return f"named_struct({parts})"


_ARROW_CAST_TYPES: dict[str, str] = {
    "INT": "Int32",
}


def _arrow_cast(value: str, dtype: str) -> str:
    return f"arrow_cast({value}, '{dtype}')"


def _map_cast(map_name: str, key: str, dtype: str) -> str:
    value = _map_value(map_name, key)
    arrow_type = _ARROW_CAST_TYPES.get(dtype.upper())
    if arrow_type is None:
        return f"CAST({value} AS {dtype})"
    return _arrow_cast(value, arrow_type)


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
    return nested_base_sql("cst_parse_manifest", table=table)


def libcst_parse_errors_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST parse error rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("cst_parse_errors", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      base.error_type AS error_type,
      base.message AS message,
      base.raw_line AS raw_line,
      base.raw_column AS raw_column,
      base.editor_line AS editor_line,
      base.editor_column AS editor_column,
      base.context AS context,
      base.line_base AS line_base,
      base.col_unit AS col_unit,
      base.end_exclusive AS end_exclusive
    FROM base
    """


def libcst_refs_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST reference rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("cst_refs", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      base.ref_id AS ref_id,
      base.ref_kind AS ref_kind,
      base.ref_text AS ref_text,
      base.expr_ctx AS expr_ctx,
      base.scope_type AS scope_type,
      base.scope_name AS scope_name,
      base.scope_role AS scope_role,
      base.parent_kind AS parent_kind,
      base.inferred_type AS inferred_type,
      base.bstart AS bstart,
      base.bend AS bend
    FROM base
    """


def libcst_imports_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST import rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("cst_imports", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      base.kind AS kind,
      base.module AS module,
      base.relative_level AS relative_level,
      base.name AS name,
      base.asname AS asname,
      base.is_star AS is_star,
      base.stmt_bstart AS stmt_bstart,
      base.stmt_bend AS stmt_bend,
      base.alias_bstart AS alias_bstart,
      base.alias_bend AS alias_bend,
      {
        _hash_expr(
            "cst_import",
            "base.file_id",
            "base.kind",
            "base.alias_bstart",
            "base.alias_bend",
        )
    } AS import_id,
      {_hash_expr("span", "base.file_id", "base.alias_bstart", "base.alias_bend")} AS span_id
    FROM base
    """


def libcst_callsites_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST callsite rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("cst_callsites", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      base.call_id AS call_id,
      base.call_bstart AS call_bstart,
      base.call_bend AS call_bend,
      base.callee_bstart AS callee_bstart,
      base.callee_bend AS callee_bend,
      base.callee_shape AS callee_shape,
      base.callee_text AS callee_text,
      base.arg_count AS arg_count,
      base.callee_dotted AS callee_dotted,
      base.callee_qnames AS callee_qnames,
      base.callee_fqns AS callee_fqns,
      base.inferred_type AS inferred_type,
      {_hash_expr("span", "base.file_id", "base.call_bstart", "base.call_bend")} AS span_id
    FROM base
    """


def libcst_defs_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST definition rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("cst_defs", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      base.def_id AS def_id,
      base.container_def_kind AS container_def_kind,
      base.container_def_bstart AS container_def_bstart,
      base.container_def_bend AS container_def_bend,
      base.kind AS kind,
      base.name AS name,
      base.def_bstart AS def_bstart,
      base.def_bend AS def_bend,
      base.name_bstart AS name_bstart,
      base.name_bend AS name_bend,
      base.qnames AS qnames,
      base.def_fqns AS def_fqns,
      base.docstring AS docstring,
      base.decorator_count AS decorator_count,
      {
        _hash_expr(
            "cst_def",
            "base.file_id",
            "base.kind",
            "base.def_bstart",
            "base.def_bend",
        )
    } AS def_id,
      {
        _hash_expr(
            "cst_def",
            "base.file_id",
            "base.container_def_kind",
            "base.container_def_bstart",
            "base.container_def_bend",
        )
    } AS container_def_id,
      {_hash_expr("span", "base.file_id", "base.def_bstart", "base.def_bend")} AS span_id
    FROM base
    """


def libcst_docstrings_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST docstring rows."""
    base = nested_base_sql("cst_docstrings", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      base.owner_def_id AS owner_def_id,
      base.owner_kind AS owner_kind,
      base.docstring AS docstring,
      base.bstart AS bstart,
      base.bend AS bend
    FROM base
    """


def libcst_decorators_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST decorator rows."""
    base = nested_base_sql("cst_decorators", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      base.owner_def_id AS owner_def_id,
      base.owner_kind AS owner_kind,
      base.decorator_text AS decorator_text,
      base.decorator_index AS decorator_index,
      base.bstart AS bstart,
      base.bend AS bend
    FROM base
    """


def libcst_call_args_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST call argument rows."""
    base = nested_base_sql("cst_call_args", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      base.call_id AS call_id,
      base.arg_index AS arg_index,
      base.keyword AS keyword,
      base.star AS star,
      base.arg_text AS arg_text,
      base.bstart AS bstart,
      base.bend AS bend
    FROM base
    """


def libcst_type_exprs_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST type expression rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("cst_type_exprs", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      base.owner_def_kind AS owner_def_kind,
      base.owner_def_bstart AS owner_def_bstart,
      base.owner_def_bend AS owner_def_bend,
      base.param_name AS param_name,
      base.expr_kind AS expr_kind,
      base.expr_role AS expr_role,
      base.bstart AS bstart,
      base.bend AS bend,
      base.expr_text AS expr_text,
      {_hash_expr("cst_type_expr", "base.path", "base.bstart", "base.bend")} AS type_expr_id,
      {
        _hash_expr(
            "cst_def",
            "base.file_id",
            "base.owner_def_kind",
            "base.owner_def_bstart",
            "base.owner_def_bend",
        )
    } AS owner_def_id
    FROM base
    """


def ast_nodes_sql(table: str = "ast_files_v1") -> str:
    """Return SQL for AST node rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ast_nodes", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.ast_id AS ast_id,
      base.parent_ast_id AS parent_ast_id,
      base.kind AS kind,
      base.name AS name,
      base.value AS value_repr,
      (base.span['start']['line0'] + 1) AS lineno,
      base.span['start']['col'] AS col_offset,
      (base.span['end']['line0'] + 1) AS end_lineno,
      base.span['end']['col'] AS end_col_offset,
      CAST(1 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive,
      base.span['byte_span']['byte_start'] AS bstart,
      (base.span['byte_span']['byte_start'] + base.span['byte_span']['byte_len']) AS bend
    FROM base
    """


def ast_edges_sql(table: str = "ast_files_v1") -> str:
    """Return SQL for AST edge rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ast_edges", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.src AS src,
      base.dst AS dst,
      base.kind AS kind,
      base.slot AS slot,
      base.idx AS idx
    FROM base
    """


def ast_errors_sql(table: str = "ast_files_v1") -> str:
    """Return SQL for AST parse error rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ast_errors", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.error_type AS error_type,
      base.message AS message,
      (base.span['start']['line0'] + 1) AS lineno,
      base.span['start']['col'] AS col_offset,
      (base.span['end']['line0'] + 1) AS end_lineno,
      base.span['end']['col'] AS end_col_offset,
      CAST(1 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive
    FROM base
    """


def tree_sitter_nodes_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter node rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ts_nodes", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.node_id AS ts_node_id,
      base.parent_id AS parent_ts_id,
      base.kind AS ts_type,
      base.span['byte_span']['byte_start'] AS start_byte,
      (base.span['byte_span']['byte_start'] + base.span['byte_span']['byte_len']) AS end_byte,
      base.flags['is_named'] AS is_named,
      base.flags['has_error'] AS has_error,
      base.flags['is_error'] AS is_error,
      base.flags['is_missing'] AS is_missing
    FROM base
    """


def tree_sitter_errors_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter parse error rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ts_errors", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.error_id AS ts_error_id,
      base.node_id AS ts_node_id,
      base.span['byte_span']['byte_start'] AS start_byte,
      (base.span['byte_span']['byte_start'] + base.span['byte_span']['byte_len']) AS end_byte,
      CAST(TRUE AS BOOLEAN) AS is_error
    FROM base
    """


def tree_sitter_missing_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter missing node rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ts_missing", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.missing_id AS ts_missing_id,
      base.node_id AS ts_node_id,
      base.span['byte_span']['byte_start'] AS start_byte,
      (base.span['byte_span']['byte_start'] + base.span['byte_span']['byte_len']) AS end_byte,
      CAST(TRUE AS BOOLEAN) AS is_missing
    FROM base
    """


def symtable_scopes_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable scope rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("symtable_scopes", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.block_id AS table_id,
      base.scope_id AS scope_id,
      base.scope_local_id AS scope_local_id,
      base.block_type AS scope_type,
      base.scope_type_value AS scope_type_value,
      base.name AS scope_name,
      base.qualpath AS qualpath,
      base.lineno1 AS lineno,
      base.parent_block_id AS parent_table_id
    FROM base
    """


def symtable_symbols_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable symbol rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("symtable_symbols", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.block_id AS table_id,
      base.scope_id AS scope_id,
      base.name AS symbol_name,
      base.flags['is_referenced'] AS is_referenced,
      base.flags['is_imported'] AS is_imported,
      base.flags['is_parameter'] AS is_parameter,
      base.flags['is_type_parameter'] AS is_type_parameter,
      base.flags['is_global'] AS is_global,
      base.flags['is_nonlocal'] AS is_nonlocal,
      base.flags['is_declared_global'] AS is_declared_global,
      base.flags['is_local'] AS is_local,
      base.flags['is_annotated'] AS is_annotated,
      base.flags['is_free'] AS is_free,
      base.flags['is_assigned'] AS is_assigned,
      base.flags['is_namespace'] AS is_namespace,
      base.namespace_count AS namespace_count,
      base.namespace_block_ids AS namespace_block_ids
    FROM base
    """


def symtable_scope_edges_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable scope edge rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("symtable_scope_edges", table=table)
    scopes = symtable_scopes_sql(table=table)
    return f"""
    WITH base AS ({base}),
    scopes AS ({scopes})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.parent_block_id AS parent_table_id,
      base.block_id AS child_table_id,
      parent.scope_id AS parent_scope_id,
      child.scope_id AS child_scope_id
    FROM base
    LEFT JOIN scopes parent
      ON parent.table_id = base.parent_block_id
     AND parent.path = base.path
    LEFT JOIN scopes child
      ON child.table_id = base.block_id
     AND child.path = base.path
    """


def symtable_namespace_edges_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable namespace edge rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("symtable_symbols", table=table)
    scopes = symtable_scopes_sql(table=table)
    return f"""
    WITH base AS ({base}),
    scopes AS ({scopes})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.scope_id AS scope_id,
      base.name AS symbol_name,
      child_block_id AS child_table_id,
      child.scope_id AS child_scope_id,
      base.namespace_count AS namespace_count
    FROM base
    CROSS JOIN unnest(base.namespace_block_ids) AS child_block_id
    LEFT JOIN scopes child
      ON child.table_id = child_block_id
     AND child.path = base.path
    """


def symtable_function_partitions_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable function partition rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("symtable_scopes", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.scope_id AS scope_id,
      base.scope_name AS scope_name,
      base.function_partitions['parameters'] AS parameters,
      base.function_partitions['locals'] AS locals,
      base.function_partitions['globals'] AS globals,
      base.function_partitions['nonlocals'] AS nonlocals,
      base.function_partitions['frees'] AS frees
    FROM base
    WHERE base.function_partitions IS NOT NULL
    """


def symtable_class_methods_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable class method rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("symtable_scopes", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.scope_id AS scope_id,
      base.scope_name AS scope_name,
      method_name AS method_name
    FROM base
    CROSS JOIN unnest(base.class_methods) AS method_name
    """


def symtable_symbol_attrs_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable symbol attribute rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("symtable_symbols", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.scope_id AS scope_id,
      base.name AS symbol_name,
      kv['key'] AS attr_key,
      kv['value'] AS attr_value
    FROM base
    CROSS JOIN unnest(map_entries(base.attrs)) AS kv
    """


def scip_metadata_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP metadata rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("scip_metadata", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.index_id AS index_id,
      base.protocol_version AS protocol_version,
      base.tool_info['name'] AS tool_name,
      base.tool_info['version'] AS tool_version,
      base.project_root AS project_root,
      base.text_document_encoding AS text_document_encoding
    FROM base
    """


def scip_documents_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP document rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("scip_documents", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.index_id AS index_id,
      prefixed_hash64('scip_doc', base.relative_path) AS document_id,
      base.relative_path AS path,
      base.language AS language,
      base.position_encoding AS position_encoding
    FROM base
    """


def scip_occurrences_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP occurrence rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("scip_occurrences", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      prefixed_hash64('scip_doc', base.relative_path) AS document_id,
      base.relative_path AS path,
      base.symbol AS symbol,
      base.symbol_roles AS symbol_roles,
      base.syntax_kind AS syntax_kind,
      base.range['start']['line0'] AS start_line,
      base.range['start']['col'] AS start_char,
      base.range['end']['line0'] AS end_line,
      base.range['end']['col'] AS end_char,
      cardinality(base.range_raw) AS range_len,
      base.enclosing_range['start']['line0'] AS enc_start_line,
      base.enclosing_range['start']['col'] AS enc_start_char,
      base.enclosing_range['end']['line0'] AS enc_end_line,
      base.enclosing_range['end']['col'] AS enc_end_char,
      cardinality(base.enclosing_range_raw) AS enc_range_len,
      CAST(0 AS INT) AS line_base,
      base.range['col_unit'] AS col_unit,
      base.range['end_exclusive'] AS end_exclusive
    FROM base
    """


def scip_symbol_information_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP symbol info rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("scip_symbol_information", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.symbol AS symbol,
      base.display_name AS display_name,
      base.kind AS kind,
      base.enclosing_symbol AS enclosing_symbol,
      base.documentation AS documentation,
      base.signature_documentation AS signature_documentation
    FROM base
    """


def scip_external_symbol_information_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP external symbol info rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("scip_external_symbol_information", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.symbol AS symbol,
      base.display_name AS display_name,
      base.kind AS kind,
      base.enclosing_symbol AS enclosing_symbol,
      base.documentation AS documentation,
      base.signature_documentation AS signature_documentation
    FROM base
    """


def scip_symbol_relationships_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP symbol relationship rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("scip_symbol_relationships", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.parent_symbol AS symbol,
      base.symbol AS related_symbol,
      base.is_reference AS is_reference,
      base.is_implementation AS is_implementation,
      base.is_type_definition AS is_type_definition,
      base.is_definition AS is_definition
    FROM base
    """


def scip_diagnostics_sql(table: str = "scip_index_v1") -> str:
    """Return SQL for SCIP diagnostics rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("scip_diagnostics", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      prefixed_hash64('scip_doc', base.relative_path) AS document_id,
      base.relative_path AS path,
      base.severity AS severity,
      base.code AS code,
      base.message AS message,
      base.source AS source,
      base.tags AS tags,
      base.occ_range['start']['line0'] AS start_line,
      base.occ_range['start']['col'] AS start_char,
      base.occ_range['end']['line0'] AS end_line,
      base.occ_range['end']['col'] AS end_char,
      CAST(0 AS INT) AS line_base,
      base.occ_range['col_unit'] AS col_unit,
      base.occ_range['end_exclusive'] AS end_exclusive
    FROM base
    """


def bytecode_code_units_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode code-unit rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_code_units", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_id AS code_unit_id,
      base.qualname AS qualpath,
      base.name AS co_name,
      base.firstlineno1 AS firstlineno,
      base.argcount AS argcount,
      base.posonlyargcount AS posonlyargcount,
      base.kwonlyargcount AS kwonlyargcount,
      base.nlocals AS nlocals,
      base.flags AS flags,
      base.stacksize AS stacksize,
      base.code_len AS code_len,
      base.varnames AS varnames,
      base.freevars AS freevars,
      base.cellvars AS cellvars,
      base.names AS names
    FROM base
    """


def bytecode_instructions_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode instruction rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_instructions", table=table)
    attrs_struct = _named_struct(
        (
            ("instr_index", _map_cast("base.attrs", "instr_index", "INT")),
            ("argval_str", _map_value("base.attrs", "argval_str")),
            ("starts_line", _map_cast("base.attrs", "starts_line", "INT")),
        )
    )
    instr_index = "decorated.attrs_struct['instr_index']"
    offset = "decorated.offset"
    return f"""
    WITH base AS ({base}),
    decorated AS (
      SELECT
        base.*,
        {attrs_struct} AS attrs_struct
      FROM base
    )
    SELECT
      decorated.file_id AS file_id,
      decorated.path AS path,
      decorated.code_id AS code_unit_id,
      {instr_index} AS instr_index,
      {offset} AS offset,
      decorated.opname AS opname,
      decorated.opcode AS opcode,
      decorated.arg AS arg,
      decorated.argrepr AS argrepr,
      decorated.is_jump_target AS is_jump_target,
      decorated.jump_target AS jump_target_offset,
      decorated.span['start']['line0'] AS pos_start_line,
      decorated.span['end']['line0'] AS pos_end_line,
      decorated.span['start']['col'] AS pos_start_col,
      decorated.span['end']['col'] AS pos_end_col,
      decorated.span['col_unit'] AS col_unit,
      decorated.span['end_exclusive'] AS end_exclusive,
      decorated.attrs_struct['argval_str'] AS argval_str,
      decorated.attrs_struct['starts_line'] AS starts_line,
      {_hash_expr("bc_instr", "decorated.code_id", instr_index, offset)} AS instr_id
    FROM decorated
    """


def bytecode_exception_table_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode exception table rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("bytecode_exception_table", table=table)
    exc_index = "base.exc_index"
    start_offset = "base.start_offset"
    end_offset = "base.end_offset"
    target_offset = "base.target_offset"
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_id AS code_unit_id,
      {exc_index} AS exc_index,
      {start_offset} AS start_offset,
      {end_offset} AS end_offset,
      {target_offset} AS target_offset,
      base.depth AS depth,
      base.lasti AS lasti,
      {
        _hash_expr(
            "bc_exc",
            "base.code_id",
            exc_index,
            start_offset,
            end_offset,
            target_offset,
        )
    } AS exc_entry_id
    FROM base
    """


def bytecode_blocks_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode basic block rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_blocks", table=table)
    start_offset = "base.start_offset"
    end_offset = "base.end_offset"
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_id AS code_unit_id,
      {start_offset} AS start_offset,
      {end_offset} AS end_offset,
      base.kind AS kind,
      {_hash_expr("bc_block", "base.code_id", start_offset, end_offset)} AS block_id
    FROM base
    """


def bytecode_cfg_edges_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode CFG edge rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_cfg_edges", table=table)
    src_start = "base.src_block_start"
    src_end = "base.src_block_end"
    dst_start = "base.dst_block_start"
    dst_end = "base.dst_block_end"
    cond_index = "base.cond_instr_index"
    cond_offset = "base.cond_instr_offset"
    exc_index = "base.exc_index"
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_id AS code_unit_id,
      {src_start} AS src_block_start,
      {src_end} AS src_block_end,
      {dst_start} AS dst_block_start,
      {dst_end} AS dst_block_end,
      base.kind AS kind,
      base.edge_key AS edge_key,
      {cond_index} AS cond_instr_index,
      {cond_offset} AS cond_instr_offset,
      {exc_index} AS exc_index,
      {_hash_expr("bc_block", "base.code_id", src_start, src_end)} AS src_block_id,
      {_hash_expr("bc_block", "base.code_id", dst_start, dst_end)} AS dst_block_id,
      {_hash_expr("bc_instr", "base.code_id", cond_index, cond_offset)} AS cond_instr_id,
      {
        _hash_expr(
            "bc_edge",
            "base.code_id",
            _hash_expr("bc_block", "base.code_id", src_start, src_end),
            _hash_expr("bc_block", "base.code_id", dst_start, dst_end),
            "base.kind",
            "base.edge_key",
            exc_index,
        )
    } AS edge_id
    FROM base
    """


def bytecode_errors_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode parse error rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("bytecode_errors", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.error_type AS error_type,
      base.message AS message
    FROM base
    """


type FragmentSqlBuilder = Callable[[], str]

_FRAGMENT_SQL_BUILDERS: dict[str, FragmentSqlBuilder] = {
    "cst_parse_manifest": libcst_parse_manifest_sql,
    "cst_parse_errors": libcst_parse_errors_sql,
    "cst_refs": libcst_refs_sql,
    "cst_imports": libcst_imports_sql,
    "cst_callsites": libcst_callsites_sql,
    "cst_defs": libcst_defs_sql,
    "cst_type_exprs": libcst_type_exprs_sql,
    "cst_docstrings": libcst_docstrings_sql,
    "cst_decorators": libcst_decorators_sql,
    "cst_call_args": libcst_call_args_sql,
    "ast_nodes": ast_nodes_sql,
    "ast_edges": ast_edges_sql,
    "ast_errors": ast_errors_sql,
    "ts_nodes": tree_sitter_nodes_sql,
    "ts_errors": tree_sitter_errors_sql,
    "ts_missing": tree_sitter_missing_sql,
    "symtable_scopes": symtable_scopes_sql,
    "symtable_symbols": symtable_symbols_sql,
    "symtable_scope_edges": symtable_scope_edges_sql,
    "symtable_namespace_edges": symtable_namespace_edges_sql,
    "symtable_function_partitions": symtable_function_partitions_sql,
    "symtable_class_methods": symtable_class_methods_sql,
    "symtable_symbol_attrs": symtable_symbol_attrs_sql,
    "scip_metadata": scip_metadata_sql,
    "scip_documents": scip_documents_sql,
    "scip_occurrences": scip_occurrences_sql,
    "scip_symbol_information": scip_symbol_information_sql,
    "scip_external_symbol_information": scip_external_symbol_information_sql,
    "scip_symbol_relationships": scip_symbol_relationships_sql,
    "scip_diagnostics": scip_diagnostics_sql,
    "py_bc_code_units": bytecode_code_units_sql,
    "py_bc_instructions": bytecode_instructions_sql,
    "bytecode_exception_table": bytecode_exception_table_sql,
    "py_bc_blocks": bytecode_blocks_sql,
    "py_bc_cfg_edges": bytecode_cfg_edges_sql,
    "bytecode_errors": bytecode_errors_sql,
}


def fragment_view_specs(ctx: SessionContext) -> tuple[ViewSpec, ...]:
    """Return ViewSpecs for SQL fragments using DataFusion schema inference.

    Parameters
    ----------
    ctx:
        DataFusion session context used to infer fragment schemas.

    Returns
    -------
    tuple[ViewSpec, ...]
        View specifications derived from fragment SQL.
    """
    return tuple(
        view_spec_from_sql(ctx, name=name, sql=_FRAGMENT_SQL_BUILDERS[name]())
        for name in sorted(_FRAGMENT_SQL_BUILDERS)
    )


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
    "fragment_view_specs",
    "libcst_call_args_sql",
    "libcst_callsites_sql",
    "libcst_decorators_sql",
    "libcst_defs_sql",
    "libcst_docstrings_sql",
    "libcst_imports_sql",
    "libcst_parse_errors_sql",
    "libcst_parse_manifest_sql",
    "libcst_refs_sql",
    "libcst_type_exprs_sql",
    "scip_diagnostics_sql",
    "scip_documents_sql",
    "scip_external_symbol_information_sql",
    "scip_metadata_sql",
    "scip_occurrences_sql",
    "scip_symbol_information_sql",
    "scip_symbol_relationships_sql",
    "symtable_class_methods_sql",
    "symtable_function_partitions_sql",
    "symtable_namespace_edges_sql",
    "symtable_scope_edges_sql",
    "symtable_scopes_sql",
    "symtable_symbol_attrs_sql",
    "symtable_symbols_sql",
    "tree_sitter_errors_sql",
    "tree_sitter_missing_sql",
    "tree_sitter_nodes_sql",
]
