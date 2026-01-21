"""SQL helpers for symtable-derived CPG tables."""

from __future__ import annotations

from datafusion_engine.query_fragments import (
    symtable_scope_edges_sql,
    symtable_scopes_sql,
    symtable_symbols_sql,
)


def _hash_expr(prefix: str, *values: str) -> str:
    """Return a prefixed hash expression for SQL fragments.

    Parameters
    ----------
    prefix
        Hash prefix to namespace the resulting identifier.
    *values
        SQL expressions to concatenate before hashing.

    Returns
    -------
    str
        SQL snippet that computes a prefixed hash.
    """
    joined = ", ".join(values)
    return f"prefixed_hash64('{prefix}', concat_ws(':', {joined}))"


def symtable_bindings_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable binding rows.

    Parameters
    ----------
    table
        Source symtable files table name.

    Returns
    -------
    str
        SQL fragment text.
    """
    scopes = symtable_scopes_sql(table=table)
    symbols = symtable_symbols_sql(table=table)
    return f"""
    WITH scopes AS ({scopes}),
    symbols AS ({symbols})
    SELECT
      symbols.file_id AS file_id,
      symbols.path AS path,
      symbols.scope_id AS scope_id,
      scopes.scope_type AS scope_type,
      scopes.scope_name AS scope_name,
      scopes.lineno AS scope_lineno,
      symbols.symbol_name AS name,
      concat_ws(
        ':',
        symbols.scope_id,
        'BIND',
        symbols.symbol_name
      ) AS binding_id,
      CASE
        WHEN symbols.is_nonlocal THEN 'nonlocal_ref'
        WHEN symbols.is_global OR symbols.is_declared_global THEN 'global_ref'
        WHEN symbols.is_free THEN 'free_ref'
        WHEN symbols.is_parameter THEN 'param'
        WHEN symbols.is_imported THEN 'import'
        WHEN symbols.is_namespace THEN 'namespace'
        WHEN symbols.is_annotated AND NOT symbols.is_assigned THEN 'annot_only'
        ELSE 'local'
      END AS binding_kind,
      (
        symbols.is_parameter
        OR symbols.is_assigned
        OR symbols.is_imported
        OR symbols.is_namespace
        OR symbols.is_annotated
      ) AS declared_here,
      symbols.is_referenced AS referenced_here,
      symbols.is_assigned AS assigned_here,
      symbols.is_annotated AS annotated_here,
      symbols.is_imported AS is_imported,
      symbols.is_parameter AS is_parameter,
      symbols.is_free AS is_free,
      symbols.is_nonlocal AS is_nonlocal,
      symbols.is_global AS is_global,
      symbols.is_declared_global AS is_declared_global
    FROM symbols
    LEFT JOIN scopes
      ON scopes.scope_id = symbols.scope_id
     AND scopes.path = symbols.path
    """


def symtable_def_sites_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable definition site rows.

    Parameters
    ----------
    table
        Source symtable files table name.

    Returns
    -------
    str
        SQL fragment text.
    """
    bindings = symtable_bindings_sql(table=table)
    return f"""
    WITH bindings AS ({bindings}),
    defs AS (SELECT * FROM cst_defs)
    SELECT
      bindings.file_id AS file_id,
      bindings.path AS path,
      bindings.scope_id AS scope_id,
      bindings.binding_id AS binding_id,
      bindings.name AS name,
      defs.def_id AS def_id,
      COALESCE(defs.name_bstart, defs.def_bstart) AS bstart,
      COALESCE(defs.name_bend, defs.def_bend) AS bend,
      CASE
        WHEN defs.kind = 'class' THEN 'class'
        WHEN defs.kind IN ('function', 'async_function') THEN 'function'
        ELSE 'other'
      END AS def_site_kind,
      CASE
        WHEN defs.name_bstart IS NOT NULL THEN CAST(0.95 AS FLOAT)
        ELSE CAST(0.85 AS FLOAT)
      END AS anchor_confidence,
      CASE
        WHEN defs.name_bstart IS NOT NULL THEN 'cst_name_span'
        ELSE 'cst_def_span'
      END AS anchor_reason,
      defs.def_id AS ambiguity_group_id,
      {
        _hash_expr(
            "py_def_site",
            "bindings.binding_id",
            "COALESCE(defs.name_bstart, defs.def_bstart)",
            "COALESCE(defs.name_bend, defs.def_bend)",
        )
    } AS def_site_id
    FROM bindings
    JOIN defs
      ON defs.path = bindings.path
     AND defs.name = bindings.name
    WHERE bindings.declared_here
    """


def symtable_use_sites_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable use site rows.

    Parameters
    ----------
    table
        Source symtable files table name.

    Returns
    -------
    str
        SQL fragment text.
    """
    bindings = symtable_bindings_sql(table=table)
    return f"""
    WITH bindings AS ({bindings}),
    refs AS (SELECT * FROM cst_refs)
    SELECT
      bindings.file_id AS file_id,
      bindings.path AS path,
      bindings.scope_id AS scope_id,
      bindings.binding_id AS binding_id,
      bindings.name AS name,
      refs.ref_id AS ref_id,
      refs.bstart AS bstart,
      refs.bend AS bend,
      CASE
        WHEN refs.expr_ctx = 'Store' THEN 'write'
        WHEN refs.expr_ctx = 'Del' THEN 'del'
        ELSE 'read'
      END AS use_kind,
      CAST(0.85 AS FLOAT) AS anchor_confidence,
      'cst_ref' AS anchor_reason,
      refs.ref_id AS ambiguity_group_id,
      {
        _hash_expr(
            "py_use_site",
            "bindings.binding_id",
            "refs.bstart",
            "refs.bend",
        )
    } AS use_site_id
    FROM bindings
    JOIN refs
      ON refs.path = bindings.path
     AND refs.ref_text = bindings.name
    """


def symtable_type_params_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable type parameter rows.

    Parameters
    ----------
    table
        Source symtable files table name.

    Returns
    -------
    str
        SQL fragment text.
    """
    scopes = symtable_scopes_sql(table=table)
    return f"""
    WITH scopes AS ({scopes})
    SELECT
      scopes.file_id AS file_id,
      scopes.path AS path,
      scopes.scope_id AS type_param_id,
      scopes.scope_id AS scope_id,
      scopes.scope_name AS name,
      CAST(NULL AS STRING) AS variance
    FROM scopes
    WHERE scopes.scope_type IN ('TYPE_PARAMETERS', 'TYPE_VARIABLE')
    """


def symtable_type_param_edges_sql(table: str = "symtable_files_v1") -> str:
    """Return SQL for symtable type-parameter edge rows.

    Parameters
    ----------
    table
        Source symtable files table name.

    Returns
    -------
    str
        SQL fragment text.
    """
    scopes = symtable_scopes_sql(table=table)
    edges = symtable_scope_edges_sql(table=table)
    return f"""
    WITH scopes AS ({scopes}),
    edges AS ({edges})
    SELECT
      edges.path AS path,
      edges.child_scope_id AS type_param_id,
      edges.parent_scope_id AS owner_scope_id,
      scopes.scope_type AS scope_type
    FROM edges
    JOIN scopes
      ON scopes.scope_id = edges.child_scope_id
     AND scopes.path = edges.path
    WHERE scopes.scope_type IN ('TYPE_PARAMETERS', 'TYPE_VARIABLE')
    """


__all__ = [
    "symtable_bindings_sql",
    "symtable_def_sites_sql",
    "symtable_type_param_edges_sql",
    "symtable_type_params_sql",
    "symtable_use_sites_sql",
]
