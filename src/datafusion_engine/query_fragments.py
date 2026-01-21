"""SQL fragment helpers for nested DataFusion schemas."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from datafusion import SessionContext

from datafusion_engine.schema_registry import nested_base_sql, schema_for
from schema_spec.view_specs import ViewSpec, view_spec_from_sql


@dataclass(frozen=True)
class SqlFragment:
    """Named SQL fragment for on-demand nested table projections."""

    name: str
    sql: str


def _map_value(map_name: str, key: str) -> str:
    return f"list_extract(map_extract({map_name}, '{key}'), 1)"


def _named_struct(fields: Sequence[tuple[str, str]]) -> str:
    parts = ", ".join(f"'{name}', {expr}" for name, expr in fields)
    return f"named_struct({parts})"


_ARROW_CAST_TYPES: dict[str, str] = {
    "INT": "Int32",
    "INT32": "Int32",
    "INT64": "Int64",
    "BOOL": "Boolean",
    "BOOLEAN": "Boolean",
    "STRING": "Utf8",
    "UTF8": "Utf8",
}

_LIBCST_DIAGNOSTIC_FIELDS: tuple[str, ...] = (
    "nodes",
    "edges",
    "parse_manifest",
    "parse_errors",
    "refs",
    "imports",
    "callsites",
    "defs",
    "type_exprs",
    "docstrings",
    "decorators",
    "call_args",
)


def _arrow_cast(value: str, dtype: str) -> str:
    return f"arrow_cast({value}, '{dtype}')"


def _map_cast(map_name: str, key: str, dtype: str) -> str:
    value = _map_value(map_name, key)
    arrow_type = _ARROW_CAST_TYPES.get(dtype.upper())
    if arrow_type is None:
        return f"CAST({value} AS {dtype})"
    return _arrow_cast(value, arrow_type)


def _metadata_value(expr: str, key: str) -> str:
    return f"arrow_metadata({expr}, '{key}')"


def _metadata_cast(expr: str, key: str, dtype: str) -> str:
    arrow_type = _ARROW_CAST_TYPES.get(dtype.upper())
    value = _metadata_value(expr, key)
    if arrow_type is None:
        return f"CAST({value} AS {dtype})"
    return _arrow_cast(value, arrow_type)


def _metadata_bool(expr: str, key: str) -> str:
    value = _metadata_value(expr, key)
    return (
        "CASE "
        f"WHEN LOWER({value}) = 'true' THEN TRUE "
        f"WHEN LOWER({value}) = 'false' THEN FALSE "
        "ELSE NULL END"
    )


def _hash_expr(prefix: str, *values: str) -> str:
    joined = ", ".join(values)
    return f"prefixed_hash64('{prefix}', concat_ws(':', {joined}))"


def _attrs_view_sql(base: str, *, cols: Sequence[str]) -> str:
    select_cols = ",\n      ".join(f"base.{col} AS {col}" for col in cols)
    return f"""
    WITH base AS (
      SELECT *
      FROM {base}
    )
    SELECT
      {select_cols},
      kv['key'] AS attr_key,
      kv['value'] AS attr_value
    FROM base
    CROSS JOIN unnest(map_entries(base.attrs)) AS kv
    """


def _attrs_extract_view_sql(
    base: str,
    *,
    cols: Sequence[str],
    key: str,
    alias: str,
) -> str:
    select_cols = ",\n      ".join(f"base.{col} AS {col}" for col in cols)
    return f"""
    WITH base AS (
      SELECT *
      FROM {base}
    )
    SELECT
      {select_cols},
      map_extract(base.attrs, '{key}') AS {alias}
    FROM base
    """


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
      base.bend AS bend,
      base.attrs AS attrs
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
      base.attrs AS attrs,
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
      base.attrs AS attrs,
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
      base.attrs AS attrs,
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
    """Return SQL for LibCST docstring rows.

    Returns
    -------
    str
        SQL fragment text.
    """
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
    """Return SQL for LibCST decorator rows.

    Returns
    -------
    str
        SQL fragment text.
    """
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
    """Return SQL for LibCST call argument rows.

    Returns
    -------
    str
        SQL fragment text.
    """
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


def libcst_schema_diagnostics_sql(table: str = "libcst_files_v1") -> str:
    """Return SQL for LibCST schema diagnostics rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    schema = schema_for(table)
    cast_fields = {name: str(schema.field(name).type) for name in _LIBCST_DIAGNOSTIC_FIELDS}
    diagnostics: list[str] = []
    for name, dtype in cast_fields.items():
        cast_expr = _arrow_cast(f"base.{name}", dtype)
        diagnostics.extend(
            [
                f"arrow_typeof(base.{name}) AS {name}_type",
                f"arrow_typeof({cast_expr}) AS {name}_cast_type",
                f"arrow_metadata(base.{name}) AS {name}_meta",
                f"arrow_metadata({cast_expr}) AS {name}_cast_meta",
            ]
        )
    diagnostics.extend(
        [
            "arrow_typeof(base.attrs) AS attrs_type",
            "arrow_metadata(base.attrs) AS attrs_meta",
        ]
    )
    select_clause = ",\n      ".join(diagnostics)
    return f"""
    WITH base AS (
      SELECT *
      FROM {table}
      LIMIT 1
    )
    SELECT
      {select_clause}
    FROM base
    """


def cst_refs_attrs_sql() -> str:
    """Return SQL for CST reference attrs.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_view_sql("cst_refs", cols=("file_id", "path", "ref_id"))


def cst_defs_attrs_sql() -> str:
    """Return SQL for CST definition attrs.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_view_sql("cst_defs", cols=("file_id", "path", "def_id"))


def cst_callsites_attrs_sql() -> str:
    """Return SQL for CST callsite attrs.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_view_sql("cst_callsites", cols=("file_id", "path", "call_id"))


def cst_imports_attrs_sql() -> str:
    """Return SQL for CST import attrs.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_view_sql("cst_imports", cols=("file_id", "path", "import_id"))


def cst_nodes_attrs_sql() -> str:
    """Return SQL for CST node attrs.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_view_sql("cst_nodes", cols=("file_id", "path", "cst_id"))


def cst_edges_attrs_sql() -> str:
    """Return SQL for CST edge attrs.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_view_sql(
        "cst_edges",
        cols=("file_id", "path", "src", "dst", "kind", "slot", "idx"),
    )


def cst_refs_attr_origin_sql() -> str:
    """Return SQL for CST reference origin attributes.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_extract_view_sql(
        "cst_refs",
        cols=("file_id", "path", "ref_id"),
        key="origin",
        alias="origin",
    )


def cst_defs_attr_origin_sql() -> str:
    """Return SQL for CST definition origin attributes.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_extract_view_sql(
        "cst_defs",
        cols=("file_id", "path", "def_id"),
        key="origin",
        alias="origin",
    )


def cst_callsites_attr_origin_sql() -> str:
    """Return SQL for CST callsite origin attributes.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_extract_view_sql(
        "cst_callsites",
        cols=("file_id", "path", "call_id"),
        key="origin",
        alias="origin",
    )


def cst_imports_attr_origin_sql() -> str:
    """Return SQL for CST import origin attributes.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_extract_view_sql(
        "cst_imports",
        cols=("file_id", "path", "import_id"),
        key="origin",
        alias="origin",
    )


def cst_nodes_attr_origin_sql() -> str:
    """Return SQL for CST node origin attributes.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_extract_view_sql(
        "cst_nodes",
        cols=("file_id", "path", "cst_id"),
        key="origin",
        alias="origin",
    )


def cst_edges_attr_origin_sql() -> str:
    """Return SQL for CST edge origin attributes.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_extract_view_sql(
        "cst_edges",
        cols=("file_id", "path", "src", "dst", "kind", "slot", "idx"),
        key="origin",
        alias="origin",
    )


def cst_ref_spans_sql() -> str:
    """Return SQL for CST reference spans.

    Returns
    -------
    str
        SQL fragment text.
    """
    span = _named_struct((("bstart", "base.bstart"), ("bend", "base.bend")))
    return f"""
    WITH base AS (
      SELECT * FROM cst_refs
    )
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.ref_id AS ref_id,
      base.bstart AS bstart,
      base.bend AS bend,
      {span} AS span
    FROM base
    """


def cst_callsite_spans_sql() -> str:
    """Return SQL for CST callsite spans.

    Returns
    -------
    str
        SQL fragment text.
    """
    span = _named_struct((("bstart", "base.call_bstart"), ("bend", "base.call_bend")))
    return f"""
    WITH base AS (
      SELECT * FROM cst_callsites
    )
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.call_id AS call_id,
      base.call_bstart AS bstart,
      base.call_bend AS bend,
      {span} AS span
    FROM base
    """


def cst_def_spans_sql() -> str:
    """Return SQL for CST definition spans.

    Returns
    -------
    str
        SQL fragment text.
    """
    span = _named_struct((("bstart", "base.def_bstart"), ("bend", "base.def_bend")))
    return f"""
    WITH base AS (
      SELECT * FROM cst_defs
    )
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.def_id AS def_id,
      base.def_bstart AS bstart,
      base.def_bend AS bend,
      {span} AS span
    FROM base
    """


def cst_ref_span_unnest_sql() -> str:
    """Return SQL for flattened CST reference spans.

    Returns
    -------
    str
        SQL fragment text.
    """
    span = _named_struct((("bstart", "base.bstart"), ("bend", "base.bend")))
    return f"""
    WITH base AS (
      SELECT
        base.*,
        {span} AS span
      FROM cst_refs base
    )
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.ref_id AS ref_id,
      base.span['bstart'] AS bstart,
      base.span['bend'] AS bend
    FROM base
    """


def cst_callsite_span_unnest_sql() -> str:
    """Return SQL for flattened CST callsite spans.

    Returns
    -------
    str
        SQL fragment text.
    """
    span = _named_struct((("bstart", "base.call_bstart"), ("bend", "base.call_bend")))
    return f"""
    WITH base AS (
      SELECT
        base.*,
        {span} AS span
      FROM cst_callsites base
    )
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.call_id AS call_id,
      base.span['bstart'] AS bstart,
      base.span['bend'] AS bend
    FROM base
    """


def cst_def_span_unnest_sql() -> str:
    """Return SQL for flattened CST definition spans.

    Returns
    -------
    str
        SQL fragment text.
    """
    span = _named_struct((("bstart", "base.def_bstart"), ("bend", "base.def_bend")))
    return f"""
    WITH base AS (
      SELECT
        base.*,
        {span} AS span
      FROM cst_defs base
    )
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.def_id AS def_id,
      base.span['bstart'] AS bstart,
      base.span['bend'] AS bend
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


def ast_docstrings_sql(table: str = "ast_files_v1") -> str:
    """Return SQL for AST docstring rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ast_docstrings", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.owner_ast_id AS owner_ast_id,
      base.owner_kind AS owner_kind,
      base.owner_name AS owner_name,
      base.docstring AS docstring,
      base.source AS source,
      (base.span['start']['line0'] + 1) AS lineno,
      base.span['start']['col'] AS col_offset,
      (base.span['end']['line0'] + 1) AS end_lineno,
      base.span['end']['col'] AS end_col_offset,
      CAST(1 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive
    FROM base
    """


def ast_imports_sql(table: str = "ast_files_v1") -> str:
    """Return SQL for AST import rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ast_imports", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.ast_id AS ast_id,
      base.parent_ast_id AS parent_ast_id,
      base.kind AS kind,
      base.module AS module,
      base.name AS name,
      base.asname AS asname,
      base.alias_index AS alias_index,
      base.level AS level,
      (base.span['start']['line0'] + 1) AS lineno,
      base.span['start']['col'] AS col_offset,
      (base.span['end']['line0'] + 1) AS end_lineno,
      base.span['end']['col'] AS end_col_offset,
      CAST(1 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive
    FROM base
    """


def ast_defs_sql(table: str = "ast_files_v1") -> str:
    """Return SQL for AST definition rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ast_defs", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.ast_id AS ast_id,
      base.parent_ast_id AS parent_ast_id,
      base.kind AS kind,
      base.name AS name,
      base.attrs AS attrs,
      (base.span['start']['line0'] + 1) AS lineno,
      base.span['start']['col'] AS col_offset,
      (base.span['end']['line0'] + 1) AS end_lineno,
      base.span['end']['col'] AS end_col_offset,
      CAST(1 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive
    FROM base
    """


def ast_calls_sql(table: str = "ast_files_v1") -> str:
    """Return SQL for AST call rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ast_calls", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.ast_id AS ast_id,
      base.parent_ast_id AS parent_ast_id,
      base.func_kind AS func_kind,
      base.func_name AS func_name,
      base.attrs AS attrs,
      (base.span['start']['line0'] + 1) AS lineno,
      base.span['start']['col'] AS col_offset,
      (base.span['end']['line0'] + 1) AS end_lineno,
      base.span['end']['col'] AS end_col_offset,
      CAST(1 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive
    FROM base
    """


def ast_type_ignores_sql(table: str = "ast_files_v1") -> str:
    """Return SQL for AST type ignore rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ast_type_ignores", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.ast_id AS ast_id,
      base.tag AS tag,
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
      base.node_uid AS ts_node_uid,
      base.parent_id AS parent_ts_id,
      base.kind AS ts_type,
      base.kind_id AS ts_kind_id,
      base.grammar_id AS ts_grammar_id,
      base.grammar_name AS ts_grammar_name,
      base.span['start']['line0'] AS start_line,
      base.span['start']['col'] AS start_col,
      base.span['end']['line0'] AS end_line,
      base.span['end']['col'] AS end_col,
      CAST(0 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive,
      base.span['byte_span']['byte_start'] AS start_byte,
      (base.span['byte_span']['byte_start'] + base.span['byte_span']['byte_len']) AS end_byte,
      base.flags['is_named'] AS is_named,
      base.flags['has_error'] AS has_error,
      base.flags['is_error'] AS is_error,
      base.flags['is_missing'] AS is_missing,
      base.flags['is_extra'] AS is_extra,
      base.flags['has_changes'] AS has_changes,
      base.attrs AS attrs
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
      base.span['start']['line0'] AS start_line,
      base.span['start']['col'] AS start_col,
      base.span['end']['line0'] AS end_line,
      base.span['end']['col'] AS end_col,
      CAST(0 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive,
      base.span['byte_span']['byte_start'] AS start_byte,
      (base.span['byte_span']['byte_start'] + base.span['byte_span']['byte_len']) AS end_byte,
      CAST(TRUE AS BOOLEAN) AS is_error,
      base.attrs AS attrs
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
      base.span['start']['line0'] AS start_line,
      base.span['start']['col'] AS start_col,
      base.span['end']['line0'] AS end_line,
      base.span['end']['col'] AS end_col,
      CAST(0 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive,
      base.span['byte_span']['byte_start'] AS start_byte,
      (base.span['byte_span']['byte_start'] + base.span['byte_span']['byte_len']) AS end_byte,
      CAST(TRUE AS BOOLEAN) AS is_missing,
      base.attrs AS attrs
    FROM base
    """


def tree_sitter_edges_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter edge rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ts_edges", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.parent_id AS parent_ts_id,
      base.child_id AS child_ts_id,
      base.field_name AS field_name,
      base.child_index AS child_index,
      base.attrs AS attrs
    FROM base
    """


def tree_sitter_captures_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter query capture rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ts_captures", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.capture_id AS ts_capture_id,
      base.query_name AS query_name,
      base.capture_name AS capture_name,
      base.pattern_index AS pattern_index,
      base.node_id AS ts_node_id,
      base.node_kind AS node_kind,
      base.span['start']['line0'] AS start_line,
      base.span['start']['col'] AS start_col,
      base.span['end']['line0'] AS end_line,
      base.span['end']['col'] AS end_col,
      CAST(0 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive,
      base.span['byte_span']['byte_start'] AS start_byte,
      (base.span['byte_span']['byte_start'] + base.span['byte_span']['byte_len']) AS end_byte,
      base.attrs AS attrs
    FROM base
    """


def tree_sitter_defs_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter definition rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ts_defs", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.node_id AS ts_node_id,
      base.parent_id AS parent_ts_id,
      base.kind AS kind,
      base.name AS name,
      base.span['start']['line0'] AS start_line,
      base.span['start']['col'] AS start_col,
      base.span['end']['line0'] AS end_line,
      base.span['end']['col'] AS end_col,
      CAST(0 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive,
      base.span['byte_span']['byte_start'] AS start_byte,
      (base.span['byte_span']['byte_start'] + base.span['byte_span']['byte_len']) AS end_byte,
      base.attrs AS attrs
    FROM base
    """


def tree_sitter_calls_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter call rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ts_calls", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.node_id AS ts_node_id,
      base.parent_id AS parent_ts_id,
      base.callee_kind AS callee_kind,
      base.callee_text AS callee_text,
      base.callee_node_id AS callee_node_id,
      base.span['start']['line0'] AS start_line,
      base.span['start']['col'] AS start_col,
      base.span['end']['line0'] AS end_line,
      base.span['end']['col'] AS end_col,
      CAST(0 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive,
      base.span['byte_span']['byte_start'] AS start_byte,
      (base.span['byte_span']['byte_start'] + base.span['byte_span']['byte_len']) AS end_byte,
      base.attrs AS attrs
    FROM base
    """


def tree_sitter_imports_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter import rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ts_imports", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.node_id AS ts_node_id,
      base.parent_id AS parent_ts_id,
      base.kind AS kind,
      base.module AS module,
      base.name AS name,
      base.asname AS asname,
      base.alias_index AS alias_index,
      base.level AS level,
      base.span['start']['line0'] AS start_line,
      base.span['start']['col'] AS start_col,
      base.span['end']['line0'] AS end_line,
      base.span['end']['col'] AS end_col,
      CAST(0 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive,
      base.span['byte_span']['byte_start'] AS start_byte,
      (base.span['byte_span']['byte_start'] + base.span['byte_span']['byte_len']) AS end_byte,
      base.attrs AS attrs
    FROM base
    """


def tree_sitter_docstrings_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter docstring rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ts_docstrings", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.owner_node_id AS owner_node_id,
      base.owner_kind AS owner_kind,
      base.owner_name AS owner_name,
      base.doc_node_id AS doc_node_id,
      base.docstring AS docstring,
      base.source AS source,
      base.span['start']['line0'] AS start_line,
      base.span['start']['col'] AS start_col,
      base.span['end']['line0'] AS end_line,
      base.span['end']['col'] AS end_col,
      CAST(0 AS INT) AS line_base,
      base.span['col_unit'] AS col_unit,
      base.span['end_exclusive'] AS end_exclusive,
      base.span['byte_span']['byte_start'] AS start_byte,
      (base.span['byte_span']['byte_start'] + base.span['byte_span']['byte_len']) AS end_byte,
      base.attrs AS attrs
    FROM base
    """


def tree_sitter_stats_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter stats rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ts_stats", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.node_count AS node_count,
      base.named_count AS named_count,
      base.error_count AS error_count,
      base.missing_count AS missing_count,
      base.parse_ms AS parse_ms,
      base.parse_timed_out AS parse_timed_out,
      base.incremental_used AS incremental_used,
      base.query_match_count AS query_match_count,
      base.query_capture_count AS query_capture_count,
      base.match_limit_exceeded AS match_limit_exceeded
    FROM base
    """


def ts_ast_defs_check_sql() -> str:
    """Return SQL for tree-sitter vs AST def count checks."""
    return """
    WITH ts AS (
      SELECT file_id, path, COUNT(*) AS ts_defs
      FROM ts_defs
      GROUP BY file_id, path
    ),
    ast AS (
      SELECT file_id, path, COUNT(*) AS ast_defs
      FROM ast_defs
      GROUP BY file_id, path
    )
    SELECT
      COALESCE(ts.file_id, ast.file_id) AS file_id,
      COALESCE(ts.path, ast.path) AS path,
      ts.ts_defs AS ts_defs,
      ast.ast_defs AS ast_defs,
      (COALESCE(ts.ts_defs, 0) - COALESCE(ast.ast_defs, 0)) AS diff,
      (COALESCE(ts.ts_defs, 0) != COALESCE(ast.ast_defs, 0)) AS mismatch
    FROM ts
    FULL OUTER JOIN ast
      ON ts.file_id = ast.file_id AND ts.path = ast.path
    """


def ts_ast_calls_check_sql() -> str:
    """Return SQL for tree-sitter vs AST call count checks."""
    return """
    WITH ts AS (
      SELECT file_id, path, COUNT(*) AS ts_calls
      FROM ts_calls
      GROUP BY file_id, path
    ),
    ast AS (
      SELECT file_id, path, COUNT(*) AS ast_calls
      FROM ast_calls
      GROUP BY file_id, path
    )
    SELECT
      COALESCE(ts.file_id, ast.file_id) AS file_id,
      COALESCE(ts.path, ast.path) AS path,
      ts.ts_calls AS ts_calls,
      ast.ast_calls AS ast_calls,
      (COALESCE(ts.ts_calls, 0) - COALESCE(ast.ast_calls, 0)) AS diff,
      (COALESCE(ts.ts_calls, 0) != COALESCE(ast.ast_calls, 0)) AS mismatch
    FROM ts
    FULL OUTER JOIN ast
      ON ts.file_id = ast.file_id AND ts.path = ast.path
    """


def ts_ast_imports_check_sql() -> str:
    """Return SQL for tree-sitter vs AST import count checks."""
    return """
    WITH ts AS (
      SELECT file_id, path, COUNT(*) AS ts_imports
      FROM ts_imports
      GROUP BY file_id, path
    ),
    ast AS (
      SELECT file_id, path, COUNT(*) AS ast_imports
      FROM ast_imports
      GROUP BY file_id, path
    )
    SELECT
      COALESCE(ts.file_id, ast.file_id) AS file_id,
      COALESCE(ts.path, ast.path) AS path,
      ts.ts_imports AS ts_imports,
      ast.ast_imports AS ast_imports,
      (COALESCE(ts.ts_imports, 0) - COALESCE(ast.ast_imports, 0)) AS diff,
      (COALESCE(ts.ts_imports, 0) != COALESCE(ast.ast_imports, 0)) AS mismatch
    FROM ts
    FULL OUTER JOIN ast
      ON ts.file_id = ast.file_id AND ts.path = ast.path
    """


def ts_cst_docstrings_check_sql() -> str:
    """Return SQL for tree-sitter vs CST docstring count checks."""
    return """
    WITH ts AS (
      SELECT file_id, path, COUNT(*) AS ts_docstrings
      FROM ts_docstrings
      GROUP BY file_id, path
    ),
    cst AS (
      SELECT file_id, path, COUNT(*) AS cst_docstrings
      FROM cst_docstrings
      GROUP BY file_id, path
    )
    SELECT
      COALESCE(ts.file_id, cst.file_id) AS file_id,
      COALESCE(ts.path, cst.path) AS path,
      ts.ts_docstrings AS ts_docstrings,
      cst.cst_docstrings AS cst_docstrings,
      (COALESCE(ts.ts_docstrings, 0) - COALESCE(cst.cst_docstrings, 0)) AS diff,
      (COALESCE(ts.ts_docstrings, 0) != COALESCE(cst.cst_docstrings, 0)) AS mismatch
    FROM ts
    FULL OUTER JOIN cst
      ON ts.file_id = cst.file_id AND ts.path = cst.path
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
      base.function_partitions AS function_partitions,
      base.class_methods AS class_methods,
      base.lineno1 AS lineno,
      base.is_meta_scope AS is_meta_scope,
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
      base.sym_symbol_id AS sym_symbol_id,
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


def scip_metadata_sql(table: str = "scip_metadata_v1") -> str:
    """Return SQL for SCIP metadata rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"SELECT * FROM {table}"


def scip_index_stats_sql(table: str = "scip_index_stats_v1") -> str:
    """Return SQL for SCIP index stats rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"SELECT * FROM {table}"


def scip_documents_sql(table: str = "scip_documents_v1") -> str:
    """Return SQL for SCIP document rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"SELECT * FROM {table}"


def scip_document_texts_sql(table: str = "scip_document_texts_v1") -> str:
    """Return SQL for SCIP document text rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"SELECT * FROM {table}"


def scip_occurrences_sql(table: str = "scip_occurrences_v1") -> str:
    """Return SQL for SCIP occurrence rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"SELECT * FROM {table}"


def scip_symbol_information_sql(table: str = "scip_symbol_information_v1") -> str:
    """Return SQL for SCIP symbol info rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"SELECT * FROM {table}"


def scip_document_symbols_sql(table: str = "scip_document_symbols_v1") -> str:
    """Return SQL for SCIP document symbol rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"SELECT * FROM {table}"


def scip_external_symbol_information_sql(table: str = "scip_external_symbol_information_v1") -> str:
    """Return SQL for SCIP external symbol info rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"SELECT * FROM {table}"


def scip_symbol_relationships_sql(table: str = "scip_symbol_relationships_v1") -> str:
    """Return SQL for SCIP symbol relationship rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"SELECT * FROM {table}"


def scip_signature_occurrences_sql(table: str = "scip_signature_occurrences_v1") -> str:
    """Return SQL for SCIP signature occurrence rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"SELECT * FROM {table}"


def scip_diagnostics_sql(table: str = "scip_diagnostics_v1") -> str:
    """Return SQL for SCIP diagnostics rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"SELECT * FROM {table}"


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
      base.co_qualname AS co_qualname,
      base.co_filename AS co_filename,
      base.name AS co_name,
      base.firstlineno1 AS firstlineno,
      base.argcount AS argcount,
      base.posonlyargcount AS posonlyargcount,
      base.kwonlyargcount AS kwonlyargcount,
      base.nlocals AS nlocals,
      base.flags AS flags,
      base.flags_detail AS flags_detail,
      base.stacksize AS stacksize,
      base.code_len AS code_len,
      base.varnames AS varnames,
      base.freevars AS freevars,
      base.cellvars AS cellvars,
      base.names AS names,
      base.consts AS consts,
      base.consts_json AS consts_json
    FROM base
    """


def bytecode_consts_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode constant rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_consts", table=table)
    const_index = "base.const_index"
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_id AS code_unit_id,
      {const_index} AS const_index,
      base.const_repr AS const_repr,
      {_hash_expr("bc_const", "base.code_id", const_index, "base.const_repr")} AS const_id
    FROM base
    """


def bytecode_instruction_attrs_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode instruction attrs.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_instructions", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_id AS code_unit_id,
      base.instr_index AS instr_index,
      base.offset AS offset,
      kv['key'] AS attr_key,
      kv['value'] AS attr_value
    FROM base
    CROSS JOIN unnest(map_entries(base.attrs)) AS kv
    """


def bytecode_instruction_attr_keys_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode instruction attr keys.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_instructions", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_id AS code_unit_id,
      base.instr_index AS instr_index,
      base.offset AS offset,
      unnest(map_keys(base.attrs)) AS attr_key
    FROM base
    """


def bytecode_instruction_attr_values_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode instruction attr values.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_instructions", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_id AS code_unit_id,
      base.instr_index AS instr_index,
      base.offset AS offset,
      unnest(map_values(base.attrs)) AS attr_value
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
    stack_depth_before = _map_cast("base.attrs", "stack_depth_before", "INT")
    stack_depth_after = _map_cast("base.attrs", "stack_depth_after", "INT")
    line_base = _metadata_cast("base.span", "line_base", "INT")
    col_unit = _metadata_value("base.span", "col_unit")
    end_exclusive = _metadata_bool("base.span", "end_exclusive")
    instr_index = "base.instr_index"
    offset = "base.offset"
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_id AS code_unit_id,
      {instr_index} AS instr_index,
      {offset} AS offset,
      base.start_offset AS start_offset,
      base.end_offset AS end_offset,
      base.opname AS opname,
      base.baseopname AS baseopname,
      base.opcode AS opcode,
      base.baseopcode AS baseopcode,
      base.arg AS arg,
      base.oparg AS oparg,
      base.argval_kind AS argval_kind,
      base.argval_int AS argval_int,
      base.argval_str AS argval_str,
      base.argrepr AS argrepr,
      base.line_number AS line_number,
      base.starts_line AS starts_line,
      base.label AS label,
      base.is_jump_target AS is_jump_target,
      base.jump_target AS jump_target_offset,
      base.cache_info AS cache_info,
      base.span['start']['line0'] AS pos_start_line,
      base.span['end']['line0'] AS pos_end_line,
      base.span['start']['col'] AS pos_start_col,
      base.span['end']['col'] AS pos_end_col,
      {line_base} AS line_base,
      {col_unit} AS col_unit,
      {end_exclusive} AS end_exclusive,
      {stack_depth_before} AS stack_depth_before,
      {stack_depth_after} AS stack_depth_after,
      {_hash_expr("bc_instr", "base.code_id", instr_index, offset)} AS instr_id
    FROM base
    """


def bytecode_metadata_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode schema metadata maps.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      arrow_metadata(code_objects.instructions) AS instructions_metadata,
      arrow_metadata(code_objects.line_table) AS line_table_metadata
    FROM {table}
    LIMIT 1
    """


def bytecode_instruction_spans_sql(table: str = "py_bc_instructions") -> str:
    """Return SQL for bytecode instruction spans.

    Returns
    -------
    str
        SQL fragment text.
    """
    span_start = _named_struct(
        (
            ("line0", "base.pos_start_line"),
            ("col", "base.pos_start_col"),
        )
    )
    span_end = _named_struct(
        (
            ("line0", "base.pos_end_line"),
            ("col", "base.pos_end_col"),
        )
    )
    span = _named_struct(
        (
            ("start", span_start),
            ("end", span_end),
            ("col_unit", "base.col_unit"),
            ("end_exclusive", "base.end_exclusive"),
        )
    )
    return f"""
    WITH base AS (
      SELECT * FROM {table}
    )
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_unit_id AS code_unit_id,
      base.instr_id AS instr_id,
      base.instr_index AS instr_index,
      base.offset AS offset,
      {span} AS span
    FROM base
    """


def bytecode_instruction_span_fields_sql(table: str = "py_bc_instruction_spans") -> str:
    """Return SQL for bytecode instruction span field expansion.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    WITH base AS (
      SELECT * FROM {table}
    ),
    expanded AS (
      SELECT
        base.file_id AS file_id,
        base.path AS path,
        base.code_unit_id AS code_unit_id,
        base.instr_id AS instr_id,
        base.instr_index AS instr_index,
        base.offset AS offset,
        unnest(base.span)
      FROM base
    )
    SELECT
      expanded.file_id AS file_id,
      expanded.path AS path,
      expanded.code_unit_id AS code_unit_id,
      expanded.instr_id AS instr_id,
      expanded.instr_index AS instr_index,
      expanded.offset AS offset,
      expanded."__unnest_placeholder(base.span).start"['line0'] AS pos_start_line,
      expanded."__unnest_placeholder(base.span).start"['col'] AS pos_start_col,
      expanded."__unnest_placeholder(base.span).end"['line0'] AS pos_end_line,
      expanded."__unnest_placeholder(base.span).end"['col'] AS pos_end_col,
      expanded."__unnest_placeholder(base.span).col_unit" AS col_unit,
      expanded."__unnest_placeholder(base.span).end_exclusive" AS end_exclusive
    FROM expanded
    """


def bytecode_cache_entries_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode cache entry rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_cache_entries", table=table)
    instr_index = "base.instr_index"
    offset = "base.offset"
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_id AS code_unit_id,
      {instr_index} AS instr_index,
      {offset} AS offset,
      base.name AS name,
      base.size AS size,
      base.data_hex AS data_hex,
      {_hash_expr("bc_instr", "base.code_id", instr_index, offset)} AS instr_id,
      {
        _hash_expr(
            "bc_cache",
            "base.code_id",
            instr_index,
            offset,
            "base.name",
            "base.size",
            "base.data_hex",
        )
    } AS cache_entry_id
    FROM base
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


def bytecode_line_table_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode line table rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_line_table", table=table)
    line_base = _metadata_cast("base.line1", "line_base", "INT")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_id AS code_unit_id,
      base.offset AS offset,
      base.line1 AS line1,
      base.line0 AS line0,
      {line_base} AS line_base
    FROM base
    """


def bytecode_cfg_edge_attrs_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode CFG edge attrs.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_cfg_edges", table=table)
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_id AS code_unit_id,
      base.edge_key AS edge_key,
      base.kind AS kind,
      kv['key'] AS attr_key,
      kv['value'] AS attr_value
    FROM base
    CROSS JOIN unnest(map_entries(base.attrs)) AS kv
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


def bytecode_dfg_edges_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode DFG edge rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_dfg_edges", table=table)
    src_index = "base.src_instr_index"
    dst_index = "base.dst_instr_index"
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.code_id AS code_unit_id,
      {src_index} AS src_instr_index,
      {dst_index} AS dst_instr_index,
      base.kind AS kind,
      {_hash_expr("bc_dfg", "base.code_id", src_index, dst_index, "base.kind")} AS edge_id
    FROM base
    """


def bytecode_flags_detail_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode flags detail rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_code_units", table=table)
    return f"""
    WITH base AS ({base}),
    expanded AS (
      SELECT
        base.file_id AS file_id,
        base.path AS path,
        base.code_id AS code_unit_id,
        unnest(base.flags_detail) AS flags_detail
      FROM base
    )
    SELECT
      file_id,
      path,
      code_unit_id,
      "__unnest_placeholder(flags_detail).is_optimized" AS is_optimized,
      "__unnest_placeholder(flags_detail).is_newlocals" AS is_newlocals,
      "__unnest_placeholder(flags_detail).has_varargs" AS has_varargs,
      "__unnest_placeholder(flags_detail).has_varkeywords" AS has_varkeywords,
      "__unnest_placeholder(flags_detail).is_nested" AS is_nested,
      "__unnest_placeholder(flags_detail).is_generator" AS is_generator,
      "__unnest_placeholder(flags_detail).is_nofree" AS is_nofree,
      "__unnest_placeholder(flags_detail).is_coroutine" AS is_coroutine,
      "__unnest_placeholder(flags_detail).is_iterable_coroutine" AS is_iterable_coroutine,
      "__unnest_placeholder(flags_detail).is_async_generator" AS is_async_generator
    FROM expanded
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
    jump_kind = _map_value("base.attrs", "jump_kind")
    jump_label = _map_cast("base.attrs", "jump_label", "INT")
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
      {jump_kind} AS jump_kind,
      {jump_label} AS jump_label,
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


def bytecode_error_attrs_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode error attrs.

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
      base.message AS message,
      kv['key'] AS attr_key,
      kv['value'] AS attr_value
    FROM base
    CROSS JOIN unnest(map_entries(base.attrs)) AS kv
    """


def bytecode_errors_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode parse error rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("bytecode_errors", table=table)
    error_stage = _map_value("base.attrs", "error_stage")
    code_id = _map_value("base.attrs", "code_id")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.error_type AS error_type,
      base.message AS message,
      {error_stage} AS error_stage,
      {code_id} AS code_id
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
    "cst_schema_diagnostics": libcst_schema_diagnostics_sql,
    "cst_refs_attrs": cst_refs_attrs_sql,
    "cst_defs_attrs": cst_defs_attrs_sql,
    "cst_callsites_attrs": cst_callsites_attrs_sql,
    "cst_imports_attrs": cst_imports_attrs_sql,
    "cst_nodes_attrs": cst_nodes_attrs_sql,
    "cst_edges_attrs": cst_edges_attrs_sql,
    "cst_refs_attr_origin": cst_refs_attr_origin_sql,
    "cst_defs_attr_origin": cst_defs_attr_origin_sql,
    "cst_callsites_attr_origin": cst_callsites_attr_origin_sql,
    "cst_imports_attr_origin": cst_imports_attr_origin_sql,
    "cst_nodes_attr_origin": cst_nodes_attr_origin_sql,
    "cst_edges_attr_origin": cst_edges_attr_origin_sql,
    "cst_ref_spans": cst_ref_spans_sql,
    "cst_callsite_spans": cst_callsite_spans_sql,
    "cst_def_spans": cst_def_spans_sql,
    "cst_ref_span_unnest": cst_ref_span_unnest_sql,
    "cst_callsite_span_unnest": cst_callsite_span_unnest_sql,
    "cst_def_span_unnest": cst_def_span_unnest_sql,
    "ast_nodes": ast_nodes_sql,
    "ast_edges": ast_edges_sql,
    "ast_errors": ast_errors_sql,
    "ast_docstrings": ast_docstrings_sql,
    "ast_imports": ast_imports_sql,
    "ast_defs": ast_defs_sql,
    "ast_calls": ast_calls_sql,
    "ast_type_ignores": ast_type_ignores_sql,
    "ts_nodes": tree_sitter_nodes_sql,
    "ts_edges": tree_sitter_edges_sql,
    "ts_errors": tree_sitter_errors_sql,
    "ts_missing": tree_sitter_missing_sql,
    "ts_captures": tree_sitter_captures_sql,
    "ts_defs": tree_sitter_defs_sql,
    "ts_calls": tree_sitter_calls_sql,
    "ts_imports": tree_sitter_imports_sql,
    "ts_docstrings": tree_sitter_docstrings_sql,
    "ts_stats": tree_sitter_stats_sql,
    "ts_ast_defs_check": ts_ast_defs_check_sql,
    "ts_ast_calls_check": ts_ast_calls_check_sql,
    "ts_ast_imports_check": ts_ast_imports_check_sql,
    "ts_cst_docstrings_check": ts_cst_docstrings_check_sql,
    "symtable_scopes": symtable_scopes_sql,
    "symtable_symbols": symtable_symbols_sql,
    "symtable_scope_edges": symtable_scope_edges_sql,
    "symtable_namespace_edges": symtable_namespace_edges_sql,
    "symtable_function_partitions": symtable_function_partitions_sql,
    "symtable_class_methods": symtable_class_methods_sql,
    "symtable_symbol_attrs": symtable_symbol_attrs_sql,
    "scip_metadata": scip_metadata_sql,
    "scip_index_stats": scip_index_stats_sql,
    "scip_documents": scip_documents_sql,
    "scip_document_texts": scip_document_texts_sql,
    "scip_occurrences": scip_occurrences_sql,
    "scip_symbol_information": scip_symbol_information_sql,
    "scip_document_symbols": scip_document_symbols_sql,
    "scip_external_symbol_information": scip_external_symbol_information_sql,
    "scip_symbol_relationships": scip_symbol_relationships_sql,
    "scip_signature_occurrences": scip_signature_occurrences_sql,
    "scip_diagnostics": scip_diagnostics_sql,
    "py_bc_code_units": bytecode_code_units_sql,
    "py_bc_consts": bytecode_consts_sql,
    "py_bc_instruction_attrs": bytecode_instruction_attrs_sql,
    "py_bc_instruction_attr_keys": bytecode_instruction_attr_keys_sql,
    "py_bc_instruction_attr_values": bytecode_instruction_attr_values_sql,
    "py_bc_instruction_spans": bytecode_instruction_spans_sql,
    "py_bc_instruction_span_fields": bytecode_instruction_span_fields_sql,
    "py_bc_line_table": bytecode_line_table_sql,
    "py_bc_instructions": bytecode_instructions_sql,
    "py_bc_metadata": bytecode_metadata_sql,
    "py_bc_cache_entries": bytecode_cache_entries_sql,
    "bytecode_exception_table": bytecode_exception_table_sql,
    "py_bc_cfg_edge_attrs": bytecode_cfg_edge_attrs_sql,
    "py_bc_blocks": bytecode_blocks_sql,
    "py_bc_dfg_edges": bytecode_dfg_edges_sql,
    "py_bc_flags_detail": bytecode_flags_detail_sql,
    "py_bc_cfg_edges": bytecode_cfg_edges_sql,
    "py_bc_error_attrs": bytecode_error_attrs_sql,
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
    "ast_calls_sql",
    "ast_defs_sql",
    "ast_docstrings_sql",
    "ast_edges_sql",
    "ast_errors_sql",
    "ast_imports_sql",
    "ast_nodes_sql",
    "ast_type_ignores_sql",
    "bytecode_blocks_sql",
    "bytecode_cache_entries_sql",
    "bytecode_cfg_edge_attrs_sql",
    "bytecode_cfg_edges_sql",
    "bytecode_code_units_sql",
    "bytecode_consts_sql",
    "bytecode_dfg_edges_sql",
    "bytecode_error_attrs_sql",
    "bytecode_errors_sql",
    "bytecode_exception_table_sql",
    "bytecode_flags_detail_sql",
    "bytecode_instruction_attr_keys_sql",
    "bytecode_instruction_attr_values_sql",
    "bytecode_instruction_attrs_sql",
    "bytecode_instruction_span_fields_sql",
    "bytecode_instruction_spans_sql",
    "bytecode_instructions_sql",
    "bytecode_line_table_sql",
    "bytecode_metadata_sql",
    "cst_callsite_span_unnest_sql",
    "cst_callsite_spans_sql",
    "cst_callsites_attr_origin_sql",
    "cst_callsites_attrs_sql",
    "cst_def_span_unnest_sql",
    "cst_def_spans_sql",
    "cst_defs_attr_origin_sql",
    "cst_defs_attrs_sql",
    "cst_edges_attr_origin_sql",
    "cst_edges_attrs_sql",
    "cst_imports_attr_origin_sql",
    "cst_imports_attrs_sql",
    "cst_nodes_attr_origin_sql",
    "cst_nodes_attrs_sql",
    "cst_ref_span_unnest_sql",
    "cst_ref_spans_sql",
    "cst_refs_attr_origin_sql",
    "cst_refs_attrs_sql",
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
    "libcst_schema_diagnostics_sql",
    "libcst_type_exprs_sql",
    "scip_diagnostics_sql",
    "scip_document_symbols_sql",
    "scip_document_texts_sql",
    "scip_documents_sql",
    "scip_external_symbol_information_sql",
    "scip_index_stats_sql",
    "scip_metadata_sql",
    "scip_occurrences_sql",
    "scip_signature_occurrences_sql",
    "scip_symbol_information_sql",
    "scip_symbol_relationships_sql",
    "symtable_class_methods_sql",
    "symtable_function_partitions_sql",
    "symtable_namespace_edges_sql",
    "symtable_scope_edges_sql",
    "symtable_scopes_sql",
    "symtable_symbol_attrs_sql",
    "symtable_symbols_sql",
    "tree_sitter_calls_sql",
    "tree_sitter_captures_sql",
    "tree_sitter_defs_sql",
    "tree_sitter_docstrings_sql",
    "tree_sitter_edges_sql",
    "tree_sitter_errors_sql",
    "tree_sitter_imports_sql",
    "tree_sitter_missing_sql",
    "tree_sitter_nodes_sql",
    "tree_sitter_stats_sql",
    "ts_ast_calls_check_sql",
    "ts_ast_defs_check_sql",
    "ts_ast_imports_check_sql",
    "ts_cst_docstrings_check_sql",
]
