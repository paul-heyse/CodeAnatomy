"""SQL fragment helpers for nested DataFusion schemas."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.df_builder import df_from_sqlglot
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.schema_registry import nested_base_sql, schema_for
from datafusion_engine.sql_options import sql_options_for_profile
from datafusion_engine.udf_registry import register_datafusion_udfs
from schema_spec.view_specs import ViewSpec, view_spec_from_builder
from sqlglot_tools.compat import Expression
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    SchemaMapping,
    default_sqlglot_policy,
    normalize_expr,
    parse_sql_strict,
)


def _map_value(map_name: str, key: str) -> str:
    return f"list_extract(map_extract({map_name}, '{key}'), 1)"


def _get_field(expr: str, field: str) -> str:
    return f"get_field({expr}, '{field}')"


def _get_field_path(expr: str, *fields: str) -> str:
    value = expr
    for field in fields:
        value = _get_field(value, field)
    return value


def _span_field_path(prefix: str, *fields: str) -> str:
    return _get_field_path(f"{prefix}.span", *fields)


def _span_expressions(prefix: str) -> dict[str, str]:
    return {
        "start_line": _span_field_path(prefix, "start", "line0"),
        "start_col": _span_field_path(prefix, "start", "col"),
        "end_line": _span_field_path(prefix, "end", "line0"),
        "end_col": _span_field_path(prefix, "end", "col"),
        "col_unit": _span_field_path(prefix, "col_unit"),
        "end_exclusive": _span_field_path(prefix, "end_exclusive"),
        "byte_start": _span_field_path(prefix, "byte_span", "byte_start"),
        "byte_len": _span_field_path(prefix, "byte_span", "byte_len"),
    }


def _union_tag(expr: str) -> str:
    return f"union_tag({expr})"


def _union_extract(expr: str, field: str) -> str:
    return f"union_extract({expr}, '{field}')"


def _named_struct(fields: Sequence[tuple[str, str]]) -> str:
    parts = ", ".join(f"'{name}', {expr}" for name, expr in fields)
    return f"named_struct({parts})"


def _ast_span_struct(prefix: str) -> str:
    span_expr = f"{prefix}.span"
    return _named_struct(
        (
            ("start", _get_field(span_expr, "start")),
            ("end", _get_field(span_expr, "end")),
            ("byte_span", _get_field(span_expr, "byte_span")),
            ("col_unit", _get_field(span_expr, "col_unit")),
            ("end_exclusive", _get_field(span_expr, "end_exclusive")),
        )
    )


_ARROW_CAST_TYPES: dict[str, str] = {
    "INT": "Int32",
    "INT32": "Int32",
    "INT64": "Int64",
    "BOOL": "Boolean",
    "BOOLEAN": "Boolean",
    "STRING": "Utf8",
    "UTF8": "Utf8",
}
_NULL_SEPARATOR = "\x1f"
_NULL_SENTINEL = "None"

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


def _stringify_value(value: str) -> str:
    return f"coalesce(CAST({value} AS STRING), '{_NULL_SENTINEL}')"


def _stable_key_expr(prefix: str, *values: str) -> str:
    parts = ", ".join([f"'{prefix}'", *(_stringify_value(value) for value in values)])
    return f"concat_ws('{_NULL_SEPARATOR}', {parts})"


def _stable_id_expr(prefix: str, *values: str) -> str:
    return f"stable_id('{prefix}', {_stable_key_expr(prefix, *values)})"


def _code_unit_id_expr(qualpath: str, co_name: str, firstlineno: str) -> str:
    return _stable_id_expr("code", qualpath, co_name, firstlineno)


def _attrs_view_sql(base: str, *, cols: Sequence[str]) -> str:
    select_cols = ",\n      ".join(f"base.{col} AS {col}" for col in cols)
    attr_key = _get_field("kv", "key")
    attr_value = _get_field("kv", "value")
    return f"""
    WITH base AS (
      SELECT *
      FROM {base}
    )
    SELECT
      {select_cols},
      {attr_key} AS attr_key,
      {attr_value} AS attr_value
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
    ref_id = _stable_id_expr("cst_ref", "base.file_id", "base.bstart", "base.bend")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      {ref_id} AS ref_id,
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
      arrow_cast(base.attrs, 'Map(Utf8, Utf8)') AS attrs
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
    call_id = _stable_id_expr("cst_call", "base.file_id", "base.call_bstart", "base.call_bend")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      {call_id} AS call_id,
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
    def_id = _stable_id_expr(
        "cst_def",
        "base.file_id",
        "base.kind",
        "base.def_bstart",
        "base.def_bend",
    )
    container_def_id = _stable_id_expr(
        "cst_def",
        "base.file_id",
        "base.container_def_kind",
        "base.container_def_bstart",
        "base.container_def_bend",
    )
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      {def_id} AS def_id,
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
      CASE
        WHEN base.container_def_kind IS NULL THEN NULL
        ELSE {container_def_id}
      END AS container_def_id,
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
    owner_def_id = _stable_id_expr(
        "cst_def",
        "base.file_id",
        "base.owner_kind",
        "base.owner_def_bstart",
        "base.owner_def_bend",
    )
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      CASE
        WHEN base.owner_def_bstart IS NULL OR base.owner_def_bend IS NULL THEN NULL
        ELSE {owner_def_id}
      END AS owner_def_id,
      base.owner_kind AS owner_kind,
      base.owner_def_bstart AS owner_def_bstart,
      base.owner_def_bend AS owner_def_bend,
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
    owner_def_id = _stable_id_expr(
        "cst_def",
        "base.file_id",
        "base.owner_kind",
        "base.owner_def_bstart",
        "base.owner_def_bend",
    )
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      CASE
        WHEN base.owner_def_bstart IS NULL OR base.owner_def_bend IS NULL THEN NULL
        ELSE {owner_def_id}
      END AS owner_def_id,
      base.owner_kind AS owner_kind,
      base.owner_def_bstart AS owner_def_bstart,
      base.owner_def_bend AS owner_def_bend,
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
    call_id = _stable_id_expr("cst_call", "base.file_id", "base.call_bstart", "base.call_bend")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.file_sha256 AS file_sha256,
      {call_id} AS call_id,
      base.call_bstart AS call_bstart,
      base.call_bend AS call_bend,
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
    bstart = _get_field("base.span", "bstart")
    bend = _get_field("base.span", "bend")
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
      {bstart} AS bstart,
      {bend} AS bend
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
    bstart = _get_field("base.span", "bstart")
    bend = _get_field("base.span", "bend")
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
      {bstart} AS bstart,
      {bend} AS bend
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
    bstart = _get_field("base.span", "bstart")
    bend = _get_field("base.span", "bend")
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
      {bstart} AS bstart,
      {bend} AS bend
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
    span_struct = _ast_span_struct("base")
    record_struct = _named_struct((("span", span_struct), ("attrs", "base.attrs")))
    span_start_line = _span_field_path("base", "start", "line0")
    span_start_col = _span_field_path("base", "start", "col")
    span_end_line = _span_field_path("base", "end", "line0")
    span_end_col = _span_field_path("base", "end", "col")
    span_col_unit = _span_field_path("base", "col_unit")
    span_end_exclusive = _span_field_path("base", "end_exclusive")
    span_byte_start = _span_field_path("base", "byte_span", "byte_start")
    span_byte_len = _span_field_path("base", "byte_span", "byte_len")
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
      {_arrow_cast(f"({span_start_line} + 1)", "Int64")} AS lineno,
      {_arrow_cast(span_start_col, "Int64")} AS col_offset,
      {_arrow_cast(f"({span_end_line} + 1)", "Int64")} AS end_lineno,
      {_arrow_cast(span_end_col, "Int64")} AS end_col_offset,
      {_arrow_cast("1", "Int32")} AS line_base,
      {_arrow_cast(span_col_unit, "Utf8")} AS col_unit,
      {_arrow_cast(span_end_exclusive, "Boolean")} AS end_exclusive,
      {_arrow_cast(span_byte_start, "Int64")} AS bstart,
      {
        _arrow_cast(
            f"({span_byte_start} + {span_byte_len})",
            "Int64",
        )
    } AS bend,
      {span_struct} AS span,
      base.attrs AS attrs,
      {record_struct} AS ast_record
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
      base.idx AS idx,
      arrow_cast(base.attrs, 'Map(Utf8, Utf8)') AS attrs
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
    span_struct = _ast_span_struct("base")
    record_struct = _named_struct((("span", span_struct), ("attrs", "base.attrs")))
    span_parts = _span_expressions("base")
    span_start_line = span_parts["start_line"]
    span_start_col = span_parts["start_col"]
    span_end_line = span_parts["end_line"]
    span_end_col = span_parts["end_col"]
    span_col_unit = span_parts["col_unit"]
    span_end_exclusive = span_parts["end_exclusive"]
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.error_type AS error_type,
      base.message AS message,
      {_arrow_cast(f"({span_start_line} + 1)", "Int64")} AS lineno,
      {_arrow_cast(span_start_col, "Int64")} AS col_offset,
      {_arrow_cast(f"({span_end_line} + 1)", "Int64")} AS end_lineno,
      {_arrow_cast(span_end_col, "Int64")} AS end_col_offset,
      {_arrow_cast("1", "Int32")} AS line_base,
      {_arrow_cast(span_col_unit, "Utf8")} AS col_unit,
      {_arrow_cast(span_end_exclusive, "Boolean")} AS end_exclusive,
      {span_struct} AS span,
      base.attrs AS attrs,
      {record_struct} AS ast_record
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
    span_struct = _ast_span_struct("base")
    record_struct = _named_struct((("span", span_struct), ("attrs", "base.attrs")))
    span_parts = _span_expressions("base")
    span_start_line = span_parts["start_line"]
    span_start_col = span_parts["start_col"]
    span_end_line = span_parts["end_line"]
    span_end_col = span_parts["end_col"]
    span_col_unit = span_parts["col_unit"]
    span_end_exclusive = span_parts["end_exclusive"]
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
      {_arrow_cast(f"({span_start_line} + 1)", "Int64")} AS lineno,
      {_arrow_cast(span_start_col, "Int64")} AS col_offset,
      {_arrow_cast(f"({span_end_line} + 1)", "Int64")} AS end_lineno,
      {_arrow_cast(span_end_col, "Int64")} AS end_col_offset,
      {_arrow_cast("1", "Int32")} AS line_base,
      {_arrow_cast(span_col_unit, "Utf8")} AS col_unit,
      {_arrow_cast(span_end_exclusive, "Boolean")} AS end_exclusive,
      {span_struct} AS span,
      base.attrs AS attrs,
      {record_struct} AS ast_record
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
    span_struct = _ast_span_struct("base")
    record_struct = _named_struct((("span", span_struct), ("attrs", "base.attrs")))
    span_parts = _span_expressions("base")
    span_start_line = span_parts["start_line"]
    span_start_col = span_parts["start_col"]
    span_end_line = span_parts["end_line"]
    span_end_col = span_parts["end_col"]
    span_col_unit = span_parts["col_unit"]
    span_end_exclusive = span_parts["end_exclusive"]
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
      {_arrow_cast(f"({span_start_line} + 1)", "Int64")} AS lineno,
      {_arrow_cast(span_start_col, "Int64")} AS col_offset,
      {_arrow_cast(f"({span_end_line} + 1)", "Int64")} AS end_lineno,
      {_arrow_cast(span_end_col, "Int64")} AS end_col_offset,
      {_arrow_cast("1", "Int32")} AS line_base,
      {_arrow_cast(span_col_unit, "Utf8")} AS col_unit,
      {_arrow_cast(span_end_exclusive, "Boolean")} AS end_exclusive,
      {span_struct} AS span,
      base.attrs AS attrs,
      {record_struct} AS ast_record
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
    span_struct = _ast_span_struct("base")
    record_struct = _named_struct((("span", span_struct), ("attrs", "base.attrs")))
    span_parts = _span_expressions("base")
    span_start_line = span_parts["start_line"]
    span_start_col = span_parts["start_col"]
    span_end_line = span_parts["end_line"]
    span_end_col = span_parts["end_col"]
    span_col_unit = span_parts["col_unit"]
    span_end_exclusive = span_parts["end_exclusive"]
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.ast_id AS ast_id,
      base.parent_ast_id AS parent_ast_id,
      base.kind AS kind,
      base.name AS name,
      {_map_cast("base.attrs", "decorator_count", "INT")} AS decorator_count,
      {_map_cast("base.attrs", "arg_count", "INT")} AS arg_count,
      {_map_cast("base.attrs", "posonly_count", "INT")} AS posonly_count,
      {_map_cast("base.attrs", "kwonly_count", "INT")} AS kwonly_count,
      {_map_cast("base.attrs", "type_params_count", "INT")} AS type_params_count,
      {_map_cast("base.attrs", "base_count", "INT")} AS base_count,
      {_map_cast("base.attrs", "keyword_count", "INT")} AS keyword_count,
      {_arrow_cast(f"({span_start_line} + 1)", "Int64")} AS lineno,
      {_arrow_cast(span_start_col, "Int64")} AS col_offset,
      {_arrow_cast(f"({span_end_line} + 1)", "Int64")} AS end_lineno,
      {_arrow_cast(span_end_col, "Int64")} AS end_col_offset,
      {_arrow_cast("1", "Int32")} AS line_base,
      {_arrow_cast(span_col_unit, "Utf8")} AS col_unit,
      {_arrow_cast(span_end_exclusive, "Boolean")} AS end_exclusive,
      {span_struct} AS span,
      base.attrs AS attrs,
      {record_struct} AS ast_record
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
    span_struct = _ast_span_struct("base")
    record_struct = _named_struct((("span", span_struct), ("attrs", "base.attrs")))
    span_parts = _span_expressions("base")
    span_start_line = span_parts["start_line"]
    span_start_col = span_parts["start_col"]
    span_end_line = span_parts["end_line"]
    span_end_col = span_parts["end_col"]
    span_col_unit = span_parts["col_unit"]
    span_end_exclusive = span_parts["end_exclusive"]
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.ast_id AS ast_id,
      base.parent_ast_id AS parent_ast_id,
      base.func_kind AS func_kind,
      base.func_name AS func_name,
      {_map_cast("base.attrs", "arg_count", "INT")} AS arg_count,
      {_map_cast("base.attrs", "keyword_count", "INT")} AS keyword_count,
      {_map_cast("base.attrs", "starred_count", "INT")} AS starred_count,
      {_map_cast("base.attrs", "kw_star_count", "INT")} AS kw_star_count,
      {_arrow_cast(f"({span_start_line} + 1)", "Int64")} AS lineno,
      {_arrow_cast(span_start_col, "Int64")} AS col_offset,
      {_arrow_cast(f"({span_end_line} + 1)", "Int64")} AS end_lineno,
      {_arrow_cast(span_end_col, "Int64")} AS end_col_offset,
      {_arrow_cast("1", "Int32")} AS line_base,
      {_arrow_cast(span_col_unit, "Utf8")} AS col_unit,
      {_arrow_cast(span_end_exclusive, "Boolean")} AS end_exclusive,
      {span_struct} AS span,
      base.attrs AS attrs,
      {record_struct} AS ast_record
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
    span_struct = _ast_span_struct("base")
    record_struct = _named_struct((("span", span_struct), ("attrs", "base.attrs")))
    span_parts = _span_expressions("base")
    span_start_line = span_parts["start_line"]
    span_start_col = span_parts["start_col"]
    span_end_line = span_parts["end_line"]
    span_end_col = span_parts["end_col"]
    span_col_unit = span_parts["col_unit"]
    span_end_exclusive = span_parts["end_exclusive"]
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.ast_id AS ast_id,
      base.tag AS tag,
      {_arrow_cast(f"({span_start_line} + 1)", "Int64")} AS lineno,
      {_arrow_cast(span_start_col, "Int64")} AS col_offset,
      {_arrow_cast(f"({span_end_line} + 1)", "Int64")} AS end_lineno,
      {_arrow_cast(span_end_col, "Int64")} AS end_col_offset,
      {_arrow_cast("1", "Int32")} AS line_base,
      {_arrow_cast(span_col_unit, "Utf8")} AS col_unit,
      {_arrow_cast(span_end_exclusive, "Boolean")} AS end_exclusive,
      {span_struct} AS span,
      base.attrs AS attrs,
      {record_struct} AS ast_record
    FROM base
    """


def ast_node_attrs_sql(table: str = "ast_nodes") -> str:
    """Return SQL for AST node attrs key/value rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_view_sql(
        table,
        cols=("file_id", "path", "ast_id", "parent_ast_id", "kind", "name"),
    )


def ast_def_attrs_sql(table: str = "ast_defs") -> str:
    """Return SQL for AST definition attrs key/value rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_view_sql(
        table,
        cols=("file_id", "path", "ast_id", "parent_ast_id", "kind", "name"),
    )


def ast_call_attrs_sql(table: str = "ast_calls") -> str:
    """Return SQL for AST call attrs key/value rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_view_sql(
        table,
        cols=("file_id", "path", "ast_id", "parent_ast_id", "func_kind", "func_name"),
    )


def ast_edge_attrs_sql(table: str = "ast_edges") -> str:
    """Return SQL for AST edge attrs key/value rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    return _attrs_view_sql(
        table,
        cols=("file_id", "path", "src", "dst", "kind", "slot", "idx"),
    )


def ast_span_metadata_sql(table: str = "ast_files_v1") -> str:
    """Return SQL for AST span metadata inspection.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      arrow_metadata(nodes.span, 'line_base') AS nodes_line_base,
      arrow_metadata(nodes.span, 'col_unit') AS nodes_col_unit,
      arrow_metadata(nodes.span, 'end_exclusive') AS nodes_end_exclusive,
      arrow_metadata(errors.span, 'line_base') AS errors_line_base,
      arrow_metadata(errors.span, 'col_unit') AS errors_col_unit,
      arrow_metadata(errors.span, 'end_exclusive') AS errors_end_exclusive,
      arrow_metadata(docstrings.span, 'line_base') AS docstrings_line_base,
      arrow_metadata(docstrings.span, 'col_unit') AS docstrings_col_unit,
      arrow_metadata(docstrings.span, 'end_exclusive') AS docstrings_end_exclusive,
      arrow_metadata(imports.span, 'line_base') AS imports_line_base,
      arrow_metadata(imports.span, 'col_unit') AS imports_col_unit,
      arrow_metadata(imports.span, 'end_exclusive') AS imports_end_exclusive,
      arrow_metadata(defs.span, 'line_base') AS defs_line_base,
      arrow_metadata(defs.span, 'col_unit') AS defs_col_unit,
      arrow_metadata(defs.span, 'end_exclusive') AS defs_end_exclusive,
      arrow_metadata(calls.span, 'line_base') AS calls_line_base,
      arrow_metadata(calls.span, 'col_unit') AS calls_col_unit,
      arrow_metadata(calls.span, 'end_exclusive') AS calls_end_exclusive,
      arrow_metadata(type_ignores.span, 'line_base') AS type_ignores_line_base,
      arrow_metadata(type_ignores.span, 'col_unit') AS type_ignores_col_unit,
      arrow_metadata(type_ignores.span, 'end_exclusive') AS type_ignores_end_exclusive
    FROM {table}
    LIMIT 1
    """


def ts_span_metadata_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter span metadata inspection.

    Returns
    -------
    str
        SQL fragment text.
    """
    return f"""
    SELECT
      arrow_metadata(nodes.span, 'line_base') AS nodes_line_base,
      arrow_metadata(nodes.span, 'col_unit') AS nodes_col_unit,
      arrow_metadata(nodes.span, 'end_exclusive') AS nodes_end_exclusive,
      arrow_metadata(errors.span, 'line_base') AS errors_line_base,
      arrow_metadata(errors.span, 'col_unit') AS errors_col_unit,
      arrow_metadata(errors.span, 'end_exclusive') AS errors_end_exclusive,
      arrow_metadata(missing.span, 'line_base') AS missing_line_base,
      arrow_metadata(missing.span, 'col_unit') AS missing_col_unit,
      arrow_metadata(missing.span, 'end_exclusive') AS missing_end_exclusive,
      arrow_metadata(captures.span, 'line_base') AS captures_line_base,
      arrow_metadata(captures.span, 'col_unit') AS captures_col_unit,
      arrow_metadata(captures.span, 'end_exclusive') AS captures_end_exclusive,
      arrow_metadata(defs.span, 'line_base') AS defs_line_base,
      arrow_metadata(defs.span, 'col_unit') AS defs_col_unit,
      arrow_metadata(defs.span, 'end_exclusive') AS defs_end_exclusive,
      arrow_metadata(calls.span, 'line_base') AS calls_line_base,
      arrow_metadata(calls.span, 'col_unit') AS calls_col_unit,
      arrow_metadata(calls.span, 'end_exclusive') AS calls_end_exclusive,
      arrow_metadata(imports.span, 'line_base') AS imports_line_base,
      arrow_metadata(imports.span, 'col_unit') AS imports_col_unit,
      arrow_metadata(imports.span, 'end_exclusive') AS imports_end_exclusive,
      arrow_metadata(docstrings.span, 'line_base') AS docstrings_line_base,
      arrow_metadata(docstrings.span, 'col_unit') AS docstrings_col_unit,
      arrow_metadata(docstrings.span, 'end_exclusive') AS docstrings_end_exclusive
    FROM {table}
    LIMIT 1
    """


def tree_sitter_nodes_sql(table: str = "tree_sitter_files_v1") -> str:
    """Return SQL for tree-sitter node rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("ts_nodes", table=table)
    span = _span_expressions("base")
    flags = {
        "is_named": _get_field("base.flags", "is_named"),
        "has_error": _get_field("base.flags", "has_error"),
        "is_error": _get_field("base.flags", "is_error"),
        "is_missing": _get_field("base.flags", "is_missing"),
        "is_extra": _get_field("base.flags", "is_extra"),
        "has_changes": _get_field("base.flags", "has_changes"),
    }
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
      {span["start_line"]} AS start_line,
      {span["start_col"]} AS start_col,
      {span["end_line"]} AS end_line,
      {span["end_col"]} AS end_col,
      CAST(0 AS INT) AS line_base,
      {span["col_unit"]} AS col_unit,
      {span["end_exclusive"]} AS end_exclusive,
      {span["byte_start"]} AS start_byte,
      ({span["byte_start"]} + {span["byte_len"]}) AS end_byte,
      {flags["is_named"]} AS is_named,
      {flags["has_error"]} AS has_error,
      {flags["is_error"]} AS is_error,
      {flags["is_missing"]} AS is_missing,
      {flags["is_extra"]} AS is_extra,
      {flags["has_changes"]} AS has_changes,
      arrow_cast(base.attrs, 'Map(Utf8, Utf8)') AS attrs
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
    span_parts = _span_expressions("base")
    span_start_line = span_parts["start_line"]
    span_start_col = span_parts["start_col"]
    span_end_line = span_parts["end_line"]
    span_end_col = span_parts["end_col"]
    span_col_unit = span_parts["col_unit"]
    span_end_exclusive = span_parts["end_exclusive"]
    span_byte_start = span_parts["byte_start"]
    span_byte_len = span_parts["byte_len"]
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.error_id AS ts_error_id,
      base.node_id AS ts_node_id,
      {span_start_line} AS start_line,
      {span_start_col} AS start_col,
      {span_end_line} AS end_line,
      {span_end_col} AS end_col,
      CAST(0 AS INT) AS line_base,
      {span_col_unit} AS col_unit,
      {span_end_exclusive} AS end_exclusive,
      {span_byte_start} AS start_byte,
      ({span_byte_start} + {span_byte_len}) AS end_byte,
      CAST(TRUE AS BOOLEAN) AS is_error,
      arrow_cast(base.attrs, 'Map(Utf8, Utf8)') AS attrs
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
    span_parts = _span_expressions("base")
    span_start_line = span_parts["start_line"]
    span_start_col = span_parts["start_col"]
    span_end_line = span_parts["end_line"]
    span_end_col = span_parts["end_col"]
    span_col_unit = span_parts["col_unit"]
    span_end_exclusive = span_parts["end_exclusive"]
    span_byte_start = span_parts["byte_start"]
    span_byte_len = span_parts["byte_len"]
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.missing_id AS ts_missing_id,
      base.node_id AS ts_node_id,
      {span_start_line} AS start_line,
      {span_start_col} AS start_col,
      {span_end_line} AS end_line,
      {span_end_col} AS end_col,
      CAST(0 AS INT) AS line_base,
      {span_col_unit} AS col_unit,
      {span_end_exclusive} AS end_exclusive,
      {span_byte_start} AS start_byte,
      ({span_byte_start} + {span_byte_len}) AS end_byte,
      CAST(TRUE AS BOOLEAN) AS is_missing,
      arrow_cast(base.attrs, 'Map(Utf8, Utf8)') AS attrs
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
      arrow_cast(base.attrs, 'Map(Utf8, Utf8)') AS attrs
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
    span_parts = _span_expressions("base")
    span_start_line = span_parts["start_line"]
    span_start_col = span_parts["start_col"]
    span_end_line = span_parts["end_line"]
    span_end_col = span_parts["end_col"]
    span_col_unit = span_parts["col_unit"]
    span_end_exclusive = span_parts["end_exclusive"]
    span_byte_start = span_parts["byte_start"]
    span_byte_len = span_parts["byte_len"]
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
      {span_start_line} AS start_line,
      {span_start_col} AS start_col,
      {span_end_line} AS end_line,
      {span_end_col} AS end_col,
      CAST(0 AS INT) AS line_base,
      {span_col_unit} AS col_unit,
      {span_end_exclusive} AS end_exclusive,
      {span_byte_start} AS start_byte,
      ({span_byte_start} + {span_byte_len}) AS end_byte,
      arrow_cast(base.attrs, 'Map(Utf8, Utf8)') AS attrs
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
    span_parts = _span_expressions("base")
    span_start_line = span_parts["start_line"]
    span_start_col = span_parts["start_col"]
    span_end_line = span_parts["end_line"]
    span_end_col = span_parts["end_col"]
    span_col_unit = span_parts["col_unit"]
    span_end_exclusive = span_parts["end_exclusive"]
    span_byte_start = span_parts["byte_start"]
    span_byte_len = span_parts["byte_len"]
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.node_id AS ts_node_id,
      base.parent_id AS parent_ts_id,
      base.kind AS kind,
      base.name AS name,
      {span_start_line} AS start_line,
      {span_start_col} AS start_col,
      {span_end_line} AS end_line,
      {span_end_col} AS end_col,
      CAST(0 AS INT) AS line_base,
      {span_col_unit} AS col_unit,
      {span_end_exclusive} AS end_exclusive,
      {span_byte_start} AS start_byte,
      ({span_byte_start} + {span_byte_len}) AS end_byte,
      arrow_cast(base.attrs, 'Map(Utf8, Utf8)') AS attrs
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
    span_parts = _span_expressions("base")
    span_start_line = span_parts["start_line"]
    span_start_col = span_parts["start_col"]
    span_end_line = span_parts["end_line"]
    span_end_col = span_parts["end_col"]
    span_col_unit = span_parts["col_unit"]
    span_end_exclusive = span_parts["end_exclusive"]
    span_byte_start = span_parts["byte_start"]
    span_byte_len = span_parts["byte_len"]
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
      {span_start_line} AS start_line,
      {span_start_col} AS start_col,
      {span_end_line} AS end_line,
      {span_end_col} AS end_col,
      CAST(0 AS INT) AS line_base,
      {span_col_unit} AS col_unit,
      {span_end_exclusive} AS end_exclusive,
      {span_byte_start} AS start_byte,
      ({span_byte_start} + {span_byte_len}) AS end_byte,
      arrow_cast(base.attrs, 'Map(Utf8, Utf8)') AS attrs
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
    span_parts = _span_expressions("base")
    span_start_line = span_parts["start_line"]
    span_start_col = span_parts["start_col"]
    span_end_line = span_parts["end_line"]
    span_end_col = span_parts["end_col"]
    span_col_unit = span_parts["col_unit"]
    span_end_exclusive = span_parts["end_exclusive"]
    span_byte_start = span_parts["byte_start"]
    span_byte_len = span_parts["byte_len"]
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
      {span_start_line} AS start_line,
      {span_start_col} AS start_col,
      {span_end_line} AS end_line,
      {span_end_col} AS end_col,
      CAST(0 AS INT) AS line_base,
      {span_col_unit} AS col_unit,
      {span_end_exclusive} AS end_exclusive,
      {span_byte_start} AS start_byte,
      ({span_byte_start} + {span_byte_len}) AS end_byte,
      arrow_cast(base.attrs, 'Map(Utf8, Utf8)') AS attrs
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
    span_parts = _span_expressions("base")
    span_start_line = span_parts["start_line"]
    span_start_col = span_parts["start_col"]
    span_end_line = span_parts["end_line"]
    span_end_col = span_parts["end_col"]
    span_col_unit = span_parts["col_unit"]
    span_end_exclusive = span_parts["end_exclusive"]
    span_byte_start = span_parts["byte_start"]
    span_byte_len = span_parts["byte_len"]
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
      {span_start_line} AS start_line,
      {span_start_col} AS start_col,
      {span_end_line} AS end_line,
      {span_end_col} AS end_col,
      CAST(0 AS INT) AS line_base,
      {span_col_unit} AS col_unit,
      {span_end_exclusive} AS end_exclusive,
      {span_byte_start} AS start_byte,
      ({span_byte_start} + {span_byte_len}) AS end_byte,
      arrow_cast(base.attrs, 'Map(Utf8, Utf8)') AS attrs
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
    """Return SQL for tree-sitter vs AST def span checks.

    Returns
    -------
    str
        SQL fragment text.
    """
    return """
    WITH ts AS (
      SELECT file_id, path, start_byte, end_byte
      FROM ts_defs
    ),
    ast_base AS (
      SELECT
        file_id,
        path,
        lineno,
        col_offset,
        end_lineno,
        end_col_offset,
        line_base
      FROM ast_defs
    ),
    ast AS (
      SELECT
        ast_base.file_id AS file_id,
        ast_base.path AS path,
        (start_idx.line_start_byte + CAST(ast_base.col_offset AS BIGINT)) AS start_byte,
        (end_idx.line_start_byte + CAST(ast_base.end_col_offset AS BIGINT)) AS end_byte
      FROM ast_base
      LEFT JOIN file_line_index_v1 AS start_idx
        ON ast_base.file_id = start_idx.file_id
        AND ast_base.path = start_idx.path
        AND (ast_base.lineno - ast_base.line_base) = start_idx.line_no
      LEFT JOIN file_line_index_v1 AS end_idx
        ON ast_base.file_id = end_idx.file_id
        AND ast_base.path = end_idx.path
        AND (ast_base.end_lineno - ast_base.line_base) = end_idx.line_no
    ),
    joined AS (
      SELECT
        COALESCE(ts.file_id, ast.file_id) AS file_id,
        COALESCE(ts.path, ast.path) AS path,
        ts.start_byte AS ts_start_byte,
        ts.end_byte AS ts_end_byte,
        ast.start_byte AS ast_start_byte,
        ast.end_byte AS ast_end_byte
      FROM ts
      FULL OUTER JOIN ast
        ON ts.file_id = ast.file_id
        AND ts.path = ast.path
        AND ts.start_byte = ast.start_byte
        AND ts.end_byte = ast.end_byte
    )
    SELECT
      file_id,
      path,
      SUM(CASE WHEN ts_start_byte IS NOT NULL THEN 1 ELSE 0 END) AS ts_defs,
      SUM(CASE WHEN ast_start_byte IS NOT NULL THEN 1 ELSE 0 END) AS ast_defs,
      SUM(
        CASE WHEN ts_start_byte IS NOT NULL AND ast_start_byte IS NULL THEN 1 ELSE 0 END
      ) AS ts_only,
      SUM(
        CASE WHEN ts_start_byte IS NULL AND ast_start_byte IS NOT NULL THEN 1 ELSE 0 END
      ) AS ast_only,
      SUM(
        CASE WHEN ts_start_byte IS NOT NULL AND ast_start_byte IS NULL THEN 1 ELSE 0 END
      )
      + SUM(
        CASE WHEN ts_start_byte IS NULL AND ast_start_byte IS NOT NULL THEN 1 ELSE 0 END
      ) AS mismatch_count,
      (
        SUM(
          CASE WHEN ts_start_byte IS NOT NULL AND ast_start_byte IS NULL THEN 1 ELSE 0 END
        )
        + SUM(
          CASE WHEN ts_start_byte IS NULL AND ast_start_byte IS NOT NULL THEN 1 ELSE 0 END
        )
      ) > 0 AS mismatch
    FROM joined
    GROUP BY file_id, path
    """


def ts_ast_calls_check_sql() -> str:
    """Return SQL for tree-sitter vs AST call span checks.

    Returns
    -------
    str
        SQL fragment text.
    """
    return """
    WITH ts AS (
      SELECT file_id, path, start_byte, end_byte
      FROM ts_calls
    ),
    ast_base AS (
      SELECT
        file_id,
        path,
        lineno,
        col_offset,
        end_lineno,
        end_col_offset,
        line_base
      FROM ast_calls
    ),
    ast AS (
      SELECT
        ast_base.file_id AS file_id,
        ast_base.path AS path,
        (start_idx.line_start_byte + CAST(ast_base.col_offset AS BIGINT)) AS start_byte,
        (end_idx.line_start_byte + CAST(ast_base.end_col_offset AS BIGINT)) AS end_byte
      FROM ast_base
      LEFT JOIN file_line_index_v1 AS start_idx
        ON ast_base.file_id = start_idx.file_id
        AND ast_base.path = start_idx.path
        AND (ast_base.lineno - ast_base.line_base) = start_idx.line_no
      LEFT JOIN file_line_index_v1 AS end_idx
        ON ast_base.file_id = end_idx.file_id
        AND ast_base.path = end_idx.path
        AND (ast_base.end_lineno - ast_base.line_base) = end_idx.line_no
    ),
    joined AS (
      SELECT
        COALESCE(ts.file_id, ast.file_id) AS file_id,
        COALESCE(ts.path, ast.path) AS path,
        ts.start_byte AS ts_start_byte,
        ts.end_byte AS ts_end_byte,
        ast.start_byte AS ast_start_byte,
        ast.end_byte AS ast_end_byte
      FROM ts
      FULL OUTER JOIN ast
        ON ts.file_id = ast.file_id
        AND ts.path = ast.path
        AND ts.start_byte = ast.start_byte
        AND ts.end_byte = ast.end_byte
    )
    SELECT
      file_id,
      path,
      SUM(CASE WHEN ts_start_byte IS NOT NULL THEN 1 ELSE 0 END) AS ts_calls,
      SUM(CASE WHEN ast_start_byte IS NOT NULL THEN 1 ELSE 0 END) AS ast_calls,
      SUM(
        CASE WHEN ts_start_byte IS NOT NULL AND ast_start_byte IS NULL THEN 1 ELSE 0 END
      ) AS ts_only,
      SUM(
        CASE WHEN ts_start_byte IS NULL AND ast_start_byte IS NOT NULL THEN 1 ELSE 0 END
      ) AS ast_only,
      SUM(
        CASE WHEN ts_start_byte IS NOT NULL AND ast_start_byte IS NULL THEN 1 ELSE 0 END
      )
      + SUM(
        CASE WHEN ts_start_byte IS NULL AND ast_start_byte IS NOT NULL THEN 1 ELSE 0 END
      ) AS mismatch_count,
      (
        SUM(
          CASE WHEN ts_start_byte IS NOT NULL AND ast_start_byte IS NULL THEN 1 ELSE 0 END
        )
        + SUM(
          CASE WHEN ts_start_byte IS NULL AND ast_start_byte IS NOT NULL THEN 1 ELSE 0 END
        )
      ) > 0 AS mismatch
    FROM joined
    GROUP BY file_id, path
    """


def ts_ast_imports_check_sql() -> str:
    """Return SQL for tree-sitter vs AST import span checks.

    Returns
    -------
    str
        SQL fragment text.
    """
    return """
    WITH ts AS (
      SELECT file_id, path, start_byte, end_byte
      FROM ts_imports
    ),
    ast_base AS (
      SELECT
        file_id,
        path,
        lineno,
        col_offset,
        end_lineno,
        end_col_offset,
        line_base
      FROM ast_imports
    ),
    ast AS (
      SELECT
        ast_base.file_id AS file_id,
        ast_base.path AS path,
        (start_idx.line_start_byte + CAST(ast_base.col_offset AS BIGINT)) AS start_byte,
        (end_idx.line_start_byte + CAST(ast_base.end_col_offset AS BIGINT)) AS end_byte
      FROM ast_base
      LEFT JOIN file_line_index_v1 AS start_idx
        ON ast_base.file_id = start_idx.file_id
        AND ast_base.path = start_idx.path
        AND (ast_base.lineno - ast_base.line_base) = start_idx.line_no
      LEFT JOIN file_line_index_v1 AS end_idx
        ON ast_base.file_id = end_idx.file_id
        AND ast_base.path = end_idx.path
        AND (ast_base.end_lineno - ast_base.line_base) = end_idx.line_no
    ),
    joined AS (
      SELECT
        COALESCE(ts.file_id, ast.file_id) AS file_id,
        COALESCE(ts.path, ast.path) AS path,
        ts.start_byte AS ts_start_byte,
        ts.end_byte AS ts_end_byte,
        ast.start_byte AS ast_start_byte,
        ast.end_byte AS ast_end_byte
      FROM ts
      FULL OUTER JOIN ast
        ON ts.file_id = ast.file_id
        AND ts.path = ast.path
        AND ts.start_byte = ast.start_byte
        AND ts.end_byte = ast.end_byte
    )
    SELECT
      file_id,
      path,
      SUM(CASE WHEN ts_start_byte IS NOT NULL THEN 1 ELSE 0 END) AS ts_imports,
      SUM(CASE WHEN ast_start_byte IS NOT NULL THEN 1 ELSE 0 END) AS ast_imports,
      SUM(
        CASE WHEN ts_start_byte IS NOT NULL AND ast_start_byte IS NULL THEN 1 ELSE 0 END
      ) AS ts_only,
      SUM(
        CASE WHEN ts_start_byte IS NULL AND ast_start_byte IS NOT NULL THEN 1 ELSE 0 END
      ) AS ast_only,
      SUM(
        CASE WHEN ts_start_byte IS NOT NULL AND ast_start_byte IS NULL THEN 1 ELSE 0 END
      )
      + SUM(
        CASE WHEN ts_start_byte IS NULL AND ast_start_byte IS NOT NULL THEN 1 ELSE 0 END
      ) AS mismatch_count,
      (
        SUM(
          CASE WHEN ts_start_byte IS NOT NULL AND ast_start_byte IS NULL THEN 1 ELSE 0 END
        )
        + SUM(
          CASE WHEN ts_start_byte IS NULL AND ast_start_byte IS NOT NULL THEN 1 ELSE 0 END
        )
      ) > 0 AS mismatch
    FROM joined
    GROUP BY file_id, path
    """


def ts_cst_docstrings_check_sql() -> str:
    """Return SQL for tree-sitter vs CST docstring span checks.

    Returns
    -------
    str
        SQL fragment text.
    """
    return """
    WITH ts AS (
      SELECT file_id, path, start_byte, end_byte
      FROM ts_docstrings
    ),
    cst AS (
      SELECT
        file_id,
        path,
        CAST(bstart AS BIGINT) AS start_byte,
        CAST(bend AS BIGINT) AS end_byte
      FROM cst_docstrings
    ),
    joined AS (
      SELECT
        COALESCE(ts.file_id, cst.file_id) AS file_id,
        COALESCE(ts.path, cst.path) AS path,
        ts.start_byte AS ts_start_byte,
        ts.end_byte AS ts_end_byte,
        cst.start_byte AS cst_start_byte,
        cst.end_byte AS cst_end_byte
      FROM ts
      FULL OUTER JOIN cst
        ON ts.file_id = cst.file_id
        AND ts.path = cst.path
        AND ts.start_byte = cst.start_byte
        AND ts.end_byte = cst.end_byte
    )
    SELECT
      file_id,
      path,
      SUM(CASE WHEN ts_start_byte IS NOT NULL THEN 1 ELSE 0 END) AS ts_docstrings,
      SUM(CASE WHEN cst_start_byte IS NOT NULL THEN 1 ELSE 0 END) AS cst_docstrings,
      SUM(
        CASE WHEN ts_start_byte IS NOT NULL AND cst_start_byte IS NULL THEN 1 ELSE 0 END
      ) AS ts_only,
      SUM(
        CASE WHEN ts_start_byte IS NULL AND cst_start_byte IS NOT NULL THEN 1 ELSE 0 END
      ) AS cst_only,
      SUM(
        CASE WHEN ts_start_byte IS NOT NULL AND cst_start_byte IS NULL THEN 1 ELSE 0 END
      )
      + SUM(
        CASE WHEN ts_start_byte IS NULL AND cst_start_byte IS NOT NULL THEN 1 ELSE 0 END
      ) AS mismatch_count,
      (
        SUM(
          CASE WHEN ts_start_byte IS NOT NULL AND cst_start_byte IS NULL THEN 1 ELSE 0 END
        )
        + SUM(
          CASE WHEN ts_start_byte IS NULL AND cst_start_byte IS NOT NULL THEN 1 ELSE 0 END
        )
      ) > 0 AS mismatch
    FROM joined
    GROUP BY file_id, path
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
    is_referenced = _get_field("base.flags", "is_referenced")
    is_imported = _get_field("base.flags", "is_imported")
    is_parameter = _get_field("base.flags", "is_parameter")
    is_type_parameter = _get_field("base.flags", "is_type_parameter")
    is_global = _get_field("base.flags", "is_global")
    is_nonlocal = _get_field("base.flags", "is_nonlocal")
    is_declared_global = _get_field("base.flags", "is_declared_global")
    is_local = _get_field("base.flags", "is_local")
    is_annotated = _get_field("base.flags", "is_annotated")
    is_free = _get_field("base.flags", "is_free")
    is_assigned = _get_field("base.flags", "is_assigned")
    is_namespace = _get_field("base.flags", "is_namespace")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.block_id AS table_id,
      base.scope_id AS scope_id,
      base.name AS symbol_name,
      CASE
        WHEN base.scope_id IS NULL OR base.name IS NULL
        THEN NULL
        ELSE prefixed_hash64('sym_symbol', concat_ws(':', base.scope_id, base.name))
      END AS sym_symbol_id,
      {is_referenced} AS is_referenced,
      {is_imported} AS is_imported,
      {is_parameter} AS is_parameter,
      {is_type_parameter} AS is_type_parameter,
      {is_global} AS is_global,
      {is_nonlocal} AS is_nonlocal,
      {is_declared_global} AS is_declared_global,
      {is_local} AS is_local,
      {is_annotated} AS is_annotated,
      {is_free} AS is_free,
      {is_assigned} AS is_assigned,
      {is_namespace} AS is_namespace,
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
    parameters = _get_field("base.function_partitions", "parameters")
    locals_expr = _get_field("base.function_partitions", "locals")
    globals_expr = _get_field("base.function_partitions", "globals")
    nonlocals_expr = _get_field("base.function_partitions", "nonlocals")
    frees = _get_field("base.function_partitions", "frees")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.scope_id AS scope_id,
      base.scope_name AS scope_name,
      {parameters} AS parameters,
      {locals_expr} AS locals,
      {globals_expr} AS globals,
      {nonlocals_expr} AS nonlocals,
      {frees} AS frees
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
    attr_key = _get_field("kv", "key")
    attr_value = _get_field("kv", "value")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.scope_id AS scope_id,
      base.name AS symbol_name,
      {attr_key} AS attr_key,
      {attr_value} AS attr_value
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
    index_id = _stable_id_expr("scip_index", "base.index_path")
    return f"""
    WITH base AS (
      SELECT * FROM {table}
    )
    SELECT
      CASE
        WHEN base.index_path IS NULL OR base.index_path = '' THEN NULL
        ELSE {index_id}
      END AS index_id,
      base.protocol_version AS protocol_version,
      base.tool_name AS tool_name,
      base.tool_version AS tool_version,
      base.tool_arguments AS tool_arguments,
      base.project_root AS project_root,
      base.text_document_encoding AS text_document_encoding,
      base.project_name AS project_name,
      base.project_version AS project_version,
      base.project_namespace AS project_namespace
    FROM base
    """


def scip_index_stats_sql(table: str = "scip_index_stats_v1") -> str:
    """Return SQL for SCIP index stats rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    index_id = _stable_id_expr("scip_index", "base.index_path")
    return f"""
    WITH base AS (
      SELECT * FROM {table}
    )
    SELECT
      CASE
        WHEN base.index_path IS NULL OR base.index_path = '' THEN NULL
        ELSE {index_id}
      END AS index_id,
      base.document_count AS document_count,
      base.occurrence_count AS occurrence_count,
      base.diagnostic_count AS diagnostic_count,
      base.symbol_count AS symbol_count,
      base.external_symbol_count AS external_symbol_count,
      base.missing_position_encoding_count AS missing_position_encoding_count,
      base.document_text_count AS document_text_count,
      base.document_text_bytes AS document_text_bytes
    FROM base
    """


def scip_documents_sql(table: str = "scip_documents_v1") -> str:
    """Return SQL for SCIP document rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    index_id = _stable_id_expr("scip_index", "base.index_path")
    document_id = _stable_id_expr("scip_doc", "base.path")
    return f"""
    WITH base AS (
      SELECT * FROM {table}
    )
    SELECT
      CASE
        WHEN base.index_path IS NULL OR base.index_path = '' THEN NULL
        ELSE {index_id}
      END AS index_id,
      CASE
        WHEN base.path IS NULL OR base.path = '' THEN NULL
        ELSE {document_id}
      END AS document_id,
      base.path AS path,
      base.language AS language,
      base.position_encoding AS position_encoding
    FROM base
    """


def scip_document_texts_sql(table: str = "scip_document_texts_v1") -> str:
    """Return SQL for SCIP document text rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    document_id = _stable_id_expr("scip_doc", "base.path")
    return f"""
    WITH base AS (
      SELECT * FROM {table}
    )
    SELECT
      CASE
        WHEN base.path IS NULL OR base.path = '' THEN NULL
        ELSE {document_id}
      END AS document_id,
      base.path AS path,
      base.text AS text
    FROM base
    """


def scip_occurrences_sql(table: str = "scip_occurrences_v1") -> str:
    """Return SQL for SCIP occurrence rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    document_id = _stable_id_expr("scip_doc", "base.path")
    return f"""
    WITH base AS (
      SELECT * FROM {table}
    )
    SELECT
      CASE
        WHEN base.path IS NULL OR base.path = '' THEN NULL
        ELSE {document_id}
      END AS document_id,
      base.path AS path,
      base.symbol AS symbol,
      base.symbol_roles AS symbol_roles,
      base.syntax_kind AS syntax_kind,
      base.syntax_kind_name AS syntax_kind_name,
      base.override_documentation AS override_documentation,
      base.range_raw AS range_raw,
      base.enclosing_range_raw AS enclosing_range_raw,
      base.start_line AS start_line,
      base.start_char AS start_char,
      base.end_line AS end_line,
      base.end_char AS end_char,
      base.range_len AS range_len,
      base.enc_start_line AS enc_start_line,
      base.enc_start_char AS enc_start_char,
      base.enc_end_line AS enc_end_line,
      base.enc_end_char AS enc_end_char,
      base.enc_range_len AS enc_range_len,
      base.line_base AS line_base,
      base.col_unit AS col_unit,
      base.end_exclusive AS end_exclusive,
      base.is_definition AS is_definition,
      base.is_import AS is_import,
      base.is_write AS is_write,
      base.is_read AS is_read,
      base.is_generated AS is_generated,
      base.is_test AS is_test,
      base.is_forward_definition AS is_forward_definition
    FROM base
    """


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
    document_id = _stable_id_expr("scip_doc", "base.path")
    return f"""
    WITH base AS (
      SELECT * FROM {table}
    )
    SELECT
      CASE
        WHEN base.path IS NULL OR base.path = '' THEN NULL
        ELSE {document_id}
      END AS document_id,
      base.path AS path,
      base.symbol AS symbol,
      base.display_name AS display_name,
      base.kind AS kind,
      base.kind_name AS kind_name,
      base.enclosing_symbol AS enclosing_symbol,
      base.documentation AS documentation,
      base.signature_text AS signature_text,
      base.signature_language AS signature_language
    FROM base
    """


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
    document_id = _stable_id_expr("scip_doc", "base.path")
    return f"""
    WITH base AS (
      SELECT * FROM {table}
    )
    SELECT
      CASE
        WHEN base.path IS NULL OR base.path = '' THEN NULL
        ELSE {document_id}
      END AS document_id,
      base.path AS path,
      base.severity AS severity,
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


def bytecode_code_units_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode code-unit rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_code_units", table=table)
    code_unit_id = _code_unit_id_expr("base.qualname", "base.name", "base.firstlineno1")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      {code_unit_id} AS code_unit_id,
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
    code_unit_id = _code_unit_id_expr(
        "base.code_unit_qualpath",
        "base.code_unit_name",
        "base.code_unit_firstlineno",
    )
    const_index = "base.const_index"
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      {code_unit_id} AS code_unit_id,
      {const_index} AS const_index,
      base.const_repr AS const_repr,
      {_hash_expr("bc_const", code_unit_id, const_index, "base.const_repr")} AS const_id
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
    code_unit_id = _code_unit_id_expr(
        "base.code_unit_qualpath",
        "base.code_unit_name",
        "base.code_unit_firstlineno",
    )
    attr_key = _get_field("kv", "key")
    attr_value = _get_field("kv", "value")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      {code_unit_id} AS code_unit_id,
      base.instr_index AS instr_index,
      base.offset AS offset,
      {attr_key} AS attr_key,
      {attr_value} AS attr_value,
      {_union_tag(attr_value)} AS attr_kind,
      {_union_extract(attr_value, "int_value")} AS attr_int,
      {_union_extract(attr_value, "bool_value")} AS attr_bool,
      {_union_extract(attr_value, "str_value")} AS attr_str
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
    code_unit_id = _code_unit_id_expr(
        "base.code_unit_qualpath",
        "base.code_unit_name",
        "base.code_unit_firstlineno",
    )
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      {code_unit_id} AS code_unit_id,
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
    code_unit_id = _code_unit_id_expr(
        "base.code_unit_qualpath",
        "base.code_unit_name",
        "base.code_unit_firstlineno",
    )
    return f"""
    WITH base AS ({base}),
    expanded AS (
      SELECT
        base.file_id AS file_id,
        base.path AS path,
        {code_unit_id} AS code_unit_id,
        base.instr_index AS instr_index,
        base.offset AS offset,
        unnest(map_values(base.attrs)) AS attr_value
      FROM base
    )
    SELECT
      expanded.file_id AS file_id,
      expanded.path AS path,
      expanded.code_unit_id AS code_unit_id,
      expanded.instr_index AS instr_index,
      expanded.offset AS offset,
      expanded.attr_value AS attr_value,
      {_union_tag("expanded.attr_value")} AS attr_kind,
      {_union_extract("expanded.attr_value", "int_value")} AS attr_int,
      {_union_extract("expanded.attr_value", "bool_value")} AS attr_bool,
      {_union_extract("expanded.attr_value", "str_value")} AS attr_str
    FROM expanded
    """


def bytecode_instructions_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode instruction rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_instructions", table=table)
    code_unit_id = _code_unit_id_expr(
        "base.code_unit_qualpath",
        "base.code_unit_name",
        "base.code_unit_firstlineno",
    )
    stack_depth_before = _union_extract(
        _map_value("base.attrs", "stack_depth_before"),
        "int_value",
    )
    stack_depth_after = _union_extract(
        _map_value("base.attrs", "stack_depth_after"),
        "int_value",
    )
    line_base = _metadata_cast("base.span", "line_base", "INT")
    col_unit = _metadata_value("base.span", "col_unit")
    end_exclusive = _metadata_bool("base.span", "end_exclusive")
    instr_index = "base.instr_index"
    offset = "base.offset"
    pos_start_line = _get_field_path("base.span", "start", "line0")
    pos_end_line = _get_field_path("base.span", "end", "line0")
    pos_start_col = _get_field_path("base.span", "start", "col")
    pos_end_col = _get_field_path("base.span", "end", "col")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      {code_unit_id} AS code_unit_id,
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
      {pos_start_line} AS pos_start_line,
      {pos_end_line} AS pos_end_line,
      {pos_start_col} AS pos_start_col,
      {pos_end_col} AS pos_end_col,
      {line_base} AS line_base,
      {col_unit} AS col_unit,
      {end_exclusive} AS end_exclusive,
      {stack_depth_before} AS stack_depth_before,
      {stack_depth_after} AS stack_depth_after,
      {_hash_expr("bc_instr", code_unit_id, instr_index, offset)} AS instr_id
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
    pos_start_line = _get_field_path("base.span", "start", "line0")
    pos_start_col = _get_field_path("base.span", "start", "col")
    pos_end_line = _get_field_path("base.span", "end", "line0")
    pos_end_col = _get_field_path("base.span", "end", "col")
    col_unit = _get_field("base.span", "col_unit")
    end_exclusive = _get_field("base.span", "end_exclusive")
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
      {pos_start_line} AS pos_start_line,
      {pos_start_col} AS pos_start_col,
      {pos_end_line} AS pos_end_line,
      {pos_end_col} AS pos_end_col,
      {col_unit} AS col_unit,
      {end_exclusive} AS end_exclusive
    FROM base
    """


def bytecode_cache_entries_sql(table: str = "bytecode_files_v1") -> str:
    """Return SQL for bytecode cache entry rows.

    Returns
    -------
    str
        SQL fragment text.
    """
    base = nested_base_sql("py_bc_cache_entries", table=table)
    code_unit_id = _code_unit_id_expr(
        "base.code_unit_qualpath",
        "base.code_unit_name",
        "base.code_unit_firstlineno",
    )
    instr_index = "base.instr_index"
    offset = "base.offset"
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      {code_unit_id} AS code_unit_id,
      {instr_index} AS instr_index,
      {offset} AS offset,
      base.name AS name,
      base.size AS size,
      base.data_hex AS data_hex,
      {_hash_expr("bc_instr", code_unit_id, instr_index, offset)} AS instr_id,
      {
        _hash_expr(
            "bc_cache",
            code_unit_id,
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
    code_unit_id = _code_unit_id_expr(
        "base.code_unit_qualpath",
        "base.code_unit_name",
        "base.code_unit_firstlineno",
    )
    exc_index = "base.exc_index"
    start_offset = "base.start_offset"
    end_offset = "base.end_offset"
    target_offset = "base.target_offset"
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      {code_unit_id} AS code_unit_id,
      {exc_index} AS exc_index,
      {start_offset} AS start_offset,
      {end_offset} AS end_offset,
      {target_offset} AS target_offset,
      base.depth AS depth,
      base.lasti AS lasti,
      {
        _hash_expr(
            "bc_exc",
            code_unit_id,
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
    code_unit_id = _code_unit_id_expr(
        "base.code_unit_qualpath",
        "base.code_unit_name",
        "base.code_unit_firstlineno",
    )
    line_base = _metadata_cast("base.line1", "line_base", "INT")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      {code_unit_id} AS code_unit_id,
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
    code_unit_id = _code_unit_id_expr(
        "base.code_unit_qualpath",
        "base.code_unit_name",
        "base.code_unit_firstlineno",
    )
    attr_key = _get_field("kv", "key")
    attr_value = _get_field("kv", "value")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      {code_unit_id} AS code_unit_id,
      base.edge_key AS edge_key,
      base.kind AS kind,
      {attr_key} AS attr_key,
      {attr_value} AS attr_value,
      {_union_tag(attr_value)} AS attr_kind,
      {_union_extract(attr_value, "int_value")} AS attr_int,
      {_union_extract(attr_value, "bool_value")} AS attr_bool,
      {_union_extract(attr_value, "str_value")} AS attr_str
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
    code_unit_id = _code_unit_id_expr(
        "base.code_unit_qualpath",
        "base.code_unit_name",
        "base.code_unit_firstlineno",
    )
    start_offset = "base.start_offset"
    end_offset = "base.end_offset"
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      {code_unit_id} AS code_unit_id,
      {start_offset} AS start_offset,
      {end_offset} AS end_offset,
      base.kind AS kind,
      {_hash_expr("bc_block", code_unit_id, start_offset, end_offset)} AS block_id
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
    code_unit_id = _code_unit_id_expr(
        "base.code_unit_qualpath",
        "base.code_unit_name",
        "base.code_unit_firstlineno",
    )
    src_index = "base.src_instr_index"
    dst_index = "base.dst_instr_index"
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      {code_unit_id} AS code_unit_id,
      {src_index} AS src_instr_index,
      {dst_index} AS dst_instr_index,
      base.kind AS kind,
      {_hash_expr("bc_dfg", code_unit_id, src_index, dst_index, "base.kind")} AS edge_id
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
    code_unit_id = _code_unit_id_expr("base.qualname", "base.name", "base.firstlineno1")
    flags_detail = "base.flags_detail"
    is_optimized = _get_field(flags_detail, "is_optimized")
    is_newlocals = _get_field(flags_detail, "is_newlocals")
    has_varargs = _get_field(flags_detail, "has_varargs")
    has_varkeywords = _get_field(flags_detail, "has_varkeywords")
    is_nested = _get_field(flags_detail, "is_nested")
    is_generator = _get_field(flags_detail, "is_generator")
    is_nofree = _get_field(flags_detail, "is_nofree")
    is_coroutine = _get_field(flags_detail, "is_coroutine")
    is_iterable_coroutine = _get_field(flags_detail, "is_iterable_coroutine")
    is_async_generator = _get_field(flags_detail, "is_async_generator")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      {code_unit_id} AS code_unit_id,
      {is_optimized} AS is_optimized,
      {is_newlocals} AS is_newlocals,
      {has_varargs} AS has_varargs,
      {has_varkeywords} AS has_varkeywords,
      {is_nested} AS is_nested,
      {is_generator} AS is_generator,
      {is_nofree} AS is_nofree,
      {is_coroutine} AS is_coroutine,
      {is_iterable_coroutine} AS is_iterable_coroutine,
      {is_async_generator} AS is_async_generator
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
    code_unit_id = _code_unit_id_expr(
        "base.code_unit_qualpath",
        "base.code_unit_name",
        "base.code_unit_firstlineno",
    )
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
      {code_unit_id} AS code_unit_id,
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
      {_hash_expr("bc_block", code_unit_id, src_start, src_end)} AS src_block_id,
      {_hash_expr("bc_block", code_unit_id, dst_start, dst_end)} AS dst_block_id,
      {_hash_expr("bc_instr", code_unit_id, cond_index, cond_offset)} AS cond_instr_id,
      {
        _hash_expr(
            "bc_edge",
            code_unit_id,
            _hash_expr("bc_block", code_unit_id, src_start, src_end),
            _hash_expr("bc_block", code_unit_id, dst_start, dst_end),
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
    attr_key = _get_field("kv", "key")
    attr_value = _get_field("kv", "value")
    return f"""
    WITH base AS ({base})
    SELECT
      base.file_id AS file_id,
      base.path AS path,
      base.error_type AS error_type,
      base.message AS message,
      {attr_key} AS attr_key,
      {attr_value} AS attr_value,
      {_union_tag(attr_value)} AS attr_kind,
      {_union_extract(attr_value, "int_value")} AS attr_int,
      {_union_extract(attr_value, "bool_value")} AS attr_bool,
      {_union_extract(attr_value, "str_value")} AS attr_str
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
    "ast_node_attrs": ast_node_attrs_sql,
    "ast_def_attrs": ast_def_attrs_sql,
    "ast_call_attrs": ast_call_attrs_sql,
    "ast_edge_attrs": ast_edge_attrs_sql,
    "ast_span_metadata": ast_span_metadata_sql,
    "ts_span_metadata": ts_span_metadata_sql,
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


def fragment_view_specs(
    ctx: SessionContext,
    *,
    exclude: Sequence[str] | None = None,
) -> tuple[ViewSpec, ...]:
    """Return ViewSpecs for SQL fragments using DataFusion schema inference.

    Parameters
    ----------
    ctx:
        DataFusion session context used to infer fragment schemas.
    exclude:
        Optional fragment names to skip when building view specs.

    Returns
    -------
    tuple[ViewSpec, ...]
        View specifications derived from fragment SQL.
    """
    excluded = set(exclude or ())
    names = sorted(name for name in _FRAGMENT_SQL_BUILDERS if name not in excluded)
    sql_options = sql_options_for_profile(None)
    schema_map = SchemaIntrospector(ctx, sql_options=sql_options).schema_map()
    specs: list[ViewSpec] = []
    for name in names:
        expr = _normalize_fragment_expr(
            _FRAGMENT_SQL_BUILDERS[name](),
            schema_map=schema_map,
        )
        specs.append(
            view_spec_from_builder(
                ctx,
                name=name,
                builder=_fragment_df_builder(expr),
                sql=None,
            )
        )
    return tuple(specs)


def _fragment_df_builder(expr: Expression) -> Callable[[SessionContext], DataFrame]:
    def _build(ctx: SessionContext) -> DataFrame:
        register_datafusion_udfs(ctx)
        return df_from_sqlglot(ctx, expr.copy())

    return _build


def _normalize_fragment_expr(sql: str, *, schema_map: SchemaMapping | None) -> Expression:
    policy = default_sqlglot_policy()
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    normalized = normalize_expr(
        expr,
        options=NormalizeExprOptions(
            schema=schema_map,
            policy=policy,
            sql=sql,
        ),
    )
    return normalized


__all__ = [
    "ast_call_attrs_sql",
    "ast_calls_sql",
    "ast_def_attrs_sql",
    "ast_defs_sql",
    "ast_docstrings_sql",
    "ast_edge_attrs_sql",
    "ast_edges_sql",
    "ast_errors_sql",
    "ast_imports_sql",
    "ast_node_attrs_sql",
    "ast_nodes_sql",
    "ast_span_metadata_sql",
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
    "ts_span_metadata_sql",
]
