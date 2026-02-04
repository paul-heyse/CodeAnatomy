"""Provide runtime stubs for datafusion_ext when Rust extensions are unavailable."""

from __future__ import annotations

from datafusion import Expr, SessionContext, lit

IS_STUB: bool = True


def plugin_library_path() -> str:
    """Return a placeholder plugin library path for stubs.

    Returns
    -------
    str
        Empty string placeholder.
    """
    return ""


def plugin_manifest(path: str | None = None) -> dict[str, object]:
    """Return a stub plugin manifest payload.

    Returns
    -------
    dict[str, object]
        Stub manifest payload.
    """
    _ = path
    return {"stub": True}


def _stub_expr(*values: object) -> Expr:
    """Return a placeholder DataFusion expression.

    Parameters
    ----------
    *values
        Values referenced to satisfy unused-argument checks.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    _ = values
    return lit(None)


class _StubPluginHandle:
    """Placeholder handle for DataFusion plugin stubs."""


def load_df_plugin(path: str) -> object:
    """Return a stub DataFusion plugin handle.

    Parameters
    ----------
    path
        Plugin library path.

    Returns
    -------
    object
        Stub plugin handle instance.
    """
    _ = path
    return _StubPluginHandle()


def register_df_plugin_udfs(
    ctx: SessionContext,
    plugin: object,
    options_json: str | None = None,
) -> None:
    """Register stub plugin UDFs in a session context."""
    _ = (ctx, plugin, options_json)


def register_df_plugin_table_functions(ctx: SessionContext, plugin: object) -> None:
    """Register stub plugin table functions in a session context."""
    _ = (ctx, plugin)


def register_df_plugin_table_providers(
    ctx: SessionContext,
    plugin: object,
    table_names: object | None = None,
    options_json: object | None = None,
) -> None:
    """Register stub plugin table providers in a session context."""
    _ = (ctx, plugin, table_names, options_json)


def create_df_plugin_table_provider(
    plugin: object,
    provider_name: str,
    options_json: str | None = None,
) -> object:
    """Return a stub table provider capsule.

    Parameters
    ----------
    plugin
        Plugin handle placeholder.
    provider_name
        Provider name to register.
    options_json
        Optional provider options payload.

    Returns
    -------
    object
        Stub table provider capsule.
    """
    _ = (plugin, provider_name, options_json)
    return object()


def register_df_plugin(
    ctx: SessionContext,
    plugin: object,
    table_names: object | None = None,
    options_json: object | None = None,
) -> None:
    """Register stub plugin UDFs and table providers."""
    _ = (ctx, plugin, table_names, options_json)


def install_function_factory(ctx: SessionContext, payload: bytes) -> None:
    """Install a stub FunctionFactory extension.

    Parameters
    ----------
    ctx
        DataFusion session context.
    payload
        Serialized policy payload.
    """
    _ = (ctx, payload)


def udf_expr(name: str, *args: object, ctx: SessionContext | None = None) -> Expr:
    """Return a stub expression for udf_expr.

    Returns
    -------
    Expr
        Stub expression instance.
    """
    _ = (name, ctx)
    return _stub_expr(*args)


def install_expr_planners(ctx: SessionContext, planners: object) -> None:
    """Install stub ExprPlanner extensions.

    Parameters
    ----------
    ctx
        DataFusion session context.
    planners
        Planner names or payload.
    """
    _ = (ctx, planners)


def delta_scan_config_from_session(ctx: SessionContext, *_args: object) -> dict[str, object]:
    """Return a stub Delta scan config payload.

    Returns
    -------
    dict[str, object]
        Stub Delta scan config payload.
    """
    _ = ctx
    return {
        "scan_config_version": 1,
        "file_column_name": None,
        "enable_parquet_pushdown": False,
        "schema_force_view_types": False,
        "wrap_partition_values": False,
        "has_schema": False,
        "schema_ipc": None,
    }


def capabilities_snapshot() -> dict[str, object]:
    """Return a stub capabilities snapshot payload.

    Returns
    -------
    dict[str, object]
        Stub capabilities snapshot payload.
    """
    return {
        "stub": True,
        "datafusion_version": None,
        "arrow_version": None,
        "plugin_abi": {"major": None, "minor": None},
        "udf_registry": {
            "scalar": 0,
            "aggregate": 0,
            "window": 0,
            "table": 0,
            "custom": 0,
            "hash": "",
        },
    }


def arrow_stream_to_batches(obj: object) -> object:
    """Return a stub Arrow stream conversion result.

    Returns
    -------
    object
        Stub conversion result.
    """
    return obj


def install_delta_table_factory(ctx: SessionContext, name: str | None = None) -> None:
    """Install a stub Delta table factory.

    Parameters
    ----------
    ctx
        DataFusion session context.
    name
        Optional factory name override.
    """
    _ = (ctx, name)


def registry_snapshot(ctx: SessionContext) -> dict[str, object]:
    """Return an empty registry snapshot payload.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns
    -------
    dict[str, object]
        Snapshot mapping with required keys.
    """
    _ = ctx
    return {
        "scalar": [],
        "aggregate": [],
        "window": [],
        "table": [],
        "pycapsule_udfs": [],
        "aliases": {},
        "parameter_names": {},
        "volatility": {},
        "rewrite_tags": {},
        "signature_inputs": {},
        "return_types": {},
        "simplify": {},
        "coerce_types": {},
        "short_circuits": {},
        "custom_udfs": [],
    }


def udf_docs_snapshot(ctx: SessionContext) -> dict[str, object]:
    """Return an empty UDF docs snapshot payload.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns
    -------
    dict[str, object]
        Empty docs snapshot mapping.
    """
    _ = ctx
    return {}


def map_entries(expr: Expr) -> Expr:
    """Return a stub expression for map_entries.

    Parameters
    ----------
    expr
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def map_keys(expr: Expr) -> Expr:
    """Return a stub expression for map_keys.

    Parameters
    ----------
    expr
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def map_values(expr: Expr) -> Expr:
    """Return a stub expression for map_values.

    Parameters
    ----------
    expr
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def map_extract(expr: Expr, key: str) -> Expr:
    """Return a stub expression for map_extract.

    Parameters
    ----------
    expr
        Input expression.
    key
        Map key to extract.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr, key)


def list_extract(expr: Expr, index: int) -> Expr:
    """Return a stub expression for list_extract.

    Parameters
    ----------
    expr
        Input expression.
    index
        List index to extract.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr, index)


def list_unique(expr: Expr) -> Expr:
    """Return a stub expression for list_unique.

    Parameters
    ----------
    expr
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def first_value_agg(expr: Expr) -> Expr:
    """Return a stub expression for first_value_agg.

    Parameters
    ----------
    expr
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def last_value_agg(expr: Expr) -> Expr:
    """Return a stub expression for last_value_agg.

    Parameters
    ----------
    expr
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def count_distinct_agg(expr: Expr) -> Expr:
    """Return a stub expression for count_distinct_agg.

    Parameters
    ----------
    expr
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def string_agg(value: Expr, delimiter: Expr) -> Expr:
    """Return a stub expression for string_agg.

    Parameters
    ----------
    value
        Input expression to aggregate.
    delimiter
        Delimiter expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(value, delimiter)


def row_number_window(expr: Expr) -> Expr:
    """Return a stub expression for row_number_window.

    Parameters
    ----------
    expr
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def lag_window(expr: Expr) -> Expr:
    """Return a stub expression for lag_window.

    Parameters
    ----------
    expr
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def lead_window(expr: Expr) -> Expr:
    """Return a stub expression for lead_window.

    Parameters
    ----------
    expr
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def arrow_metadata(expr: Expr, key: str | None = None) -> Expr:
    """Return a stub expression for arrow_metadata.

    Parameters
    ----------
    expr
        Input expression.
    key
        Optional metadata key.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr, key)


def union_tag(expr: Expr) -> Expr:
    """Return a stub expression for union_tag.

    Parameters
    ----------
    expr
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr)


def union_extract(expr: Expr, tag: str) -> Expr:
    """Return a stub expression for union_extract.

    Parameters
    ----------
    expr
        Input expression.
    tag
        Union tag to extract.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(expr, tag)


def stable_hash64(value: Expr) -> Expr:
    """Return a stub expression for stable_hash64.

    Parameters
    ----------
    value
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(value)


def stable_hash128(value: Expr) -> Expr:
    """Return a stub expression for stable_hash128.

    Parameters
    ----------
    value
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(value)


def prefixed_hash64(prefix: str, value: Expr) -> Expr:
    """Return a stub expression for prefixed_hash64.

    Parameters
    ----------
    prefix
        Hash prefix.
    value
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(prefix, value)


def stable_id(prefix: str, value: Expr) -> Expr:
    """Return a stub expression for stable_id.

    Parameters
    ----------
    prefix
        Identifier prefix.
    value
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(prefix, value)


def semantic_tag(semantic_type: str, value: Expr) -> Expr:
    """Return a stub expression for semantic_tag.

    Parameters
    ----------
    semantic_type
        Semantic type tag.
    value
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(semantic_type, value)


def stable_id_parts(prefix: str, part1: Expr, *parts: Expr) -> Expr:
    """Return a stub expression for stable_id_parts.

    Parameters
    ----------
    prefix
        Identifier prefix.
    part1
        First expression part.
    *parts
        Additional expression parts.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(prefix, part1, *parts)


def prefixed_hash_parts64(prefix: str, part1: Expr, *parts: Expr) -> Expr:
    """Return a stub expression for prefixed_hash_parts64.

    Parameters
    ----------
    prefix
        Hash prefix.
    part1
        First expression part.
    *parts
        Additional expression parts.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(prefix, part1, *parts)


def stable_hash_any(
    value: Expr,
    *,
    canonical: bool | None = None,
    null_sentinel: str | None = None,
) -> Expr:
    """Return a stub expression for stable_hash_any.

    Parameters
    ----------
    value
        Input expression.
    canonical
        Whether to apply canonicalization.
    null_sentinel
        Null sentinel value.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(value, canonical, null_sentinel)


def span_make(
    bstart: Expr,
    bend: Expr,
    line_base: Expr | None = None,
    col_unit: Expr | None = None,
    end_exclusive: Expr | None = None,
) -> Expr:
    """Return a stub expression for span_make.

    Parameters
    ----------
    bstart
        Byte start expression.
    bend
        Byte end expression.
    line_base
        Optional line-base expression.
    col_unit
        Optional column unit expression.
    end_exclusive
        Optional end-exclusive flag expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(bstart, bend, line_base, col_unit, end_exclusive)


def span_len(span: Expr) -> Expr:
    """Return a stub expression for span_len.

    Parameters
    ----------
    span
        Span expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(span)


def span_overlaps(span_a: Expr, span_b: Expr) -> Expr:
    """Return a stub expression for span_overlaps.

    Parameters
    ----------
    span_a
        First span expression.
    span_b
        Second span expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(span_a, span_b)


def span_contains(span_a: Expr, span_b: Expr) -> Expr:
    """Return a stub expression for span_contains.

    Parameters
    ----------
    span_a
        First span expression.
    span_b
        Second span expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(span_a, span_b)


def interval_align_score(
    left_start: Expr,
    left_end: Expr,
    right_start: Expr,
    right_end: Expr,
) -> Expr:
    """Return a stub expression for interval_align_score.

    Parameters
    ----------
    left_start
        Left interval start expression.
    left_end
        Left interval end expression.
    right_start
        Right interval start expression.
    right_end
        Right interval end expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(left_start, left_end, right_start, right_end)


def span_id(
    prefix: str,
    path: Expr,
    bstart: Expr,
    bend: Expr,
    *,
    kind: Expr | None = None,
) -> Expr:
    """Return a stub expression for span_id.

    Parameters
    ----------
    prefix
        Identifier prefix.
    path
        Path expression.
    bstart
        Byte start expression.
    bend
        Byte end expression.
    kind
        Optional kind expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(prefix, path, bstart, bend, kind)


def utf8_normalize(
    value: Expr,
    *,
    form: str | None = None,
    casefold: bool | None = None,
    collapse_ws: bool | None = None,
) -> Expr:
    """Return a stub expression for utf8_normalize.

    Parameters
    ----------
    value
        Input expression.
    form
        Unicode normalization form.
    casefold
        Whether to casefold.
    collapse_ws
        Whether to collapse whitespace.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(value, form, casefold, collapse_ws)


def utf8_null_if_blank(value: Expr) -> Expr:
    """Return a stub expression for utf8_null_if_blank.

    Parameters
    ----------
    value
        Input expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(value)


def qname_normalize(
    symbol: Expr,
    *,
    module: Expr | None = None,
    lang: Expr | None = None,
) -> Expr:
    """Return a stub expression for qname_normalize.

    Parameters
    ----------
    symbol
        Symbol expression.
    module
        Optional module expression.
    lang
        Optional language expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(symbol, module, lang)


def map_get_default(map_expr: Expr, key: str, default_value: Expr) -> Expr:
    """Return a stub expression for map_get_default.

    Parameters
    ----------
    map_expr
        Map expression.
    key
        Map key to look up.
    default_value
        Default value expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(map_expr, key, default_value)


def map_normalize(
    map_expr: Expr,
    *,
    key_case: str | None = None,
    sort_keys: bool | None = None,
) -> Expr:
    """Return a stub expression for map_normalize.

    Parameters
    ----------
    map_expr
        Map expression.
    key_case
        Optional key-case mode.
    sort_keys
        Whether to sort keys.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(map_expr, key_case, sort_keys)


def list_compact(list_expr: Expr) -> Expr:
    """Return a stub expression for list_compact.

    Parameters
    ----------
    list_expr
        List expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(list_expr)


def list_unique_sorted(list_expr: Expr) -> Expr:
    """Return a stub expression for list_unique_sorted.

    Parameters
    ----------
    list_expr
        List expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(list_expr)


def struct_pick(struct_expr: Expr, *fields: str) -> Expr:
    """Return a stub expression for struct_pick.

    Parameters
    ----------
    struct_expr
        Struct expression.
    *fields
        Field names to select.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(struct_expr, *fields)


def cdf_change_rank(change_type: Expr) -> Expr:
    """Return a stub expression for cdf_change_rank.

    Parameters
    ----------
    change_type
        Change type expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(change_type)


def cdf_is_upsert(change_type: Expr) -> Expr:
    """Return a stub expression for cdf_is_upsert.

    Parameters
    ----------
    change_type
        Change type expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(change_type)


def cdf_is_delete(change_type: Expr) -> Expr:
    """Return a stub expression for cdf_is_delete.

    Parameters
    ----------
    change_type
        Change type expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(change_type)


def col_to_byte(line_text: Expr, col_index: Expr, col_unit: Expr) -> Expr:
    """Return a stub expression for col_to_byte.

    Parameters
    ----------
    line_text
        Line text expression.
    col_index
        Column index expression.
    col_unit
        Column unit expression.

    Returns
    -------
    Expr
        Placeholder expression.
    """
    return _stub_expr(line_text, col_index, col_unit)


__all__ = [
    "arrow_metadata",
    "arrow_stream_to_batches",
    "capabilities_snapshot",
    "cdf_change_rank",
    "cdf_is_delete",
    "cdf_is_upsert",
    "col_to_byte",
    "count_distinct_agg",
    "create_df_plugin_table_provider",
    "delta_scan_config_from_session",
    "first_value_agg",
    "install_delta_table_factory",
    "install_expr_planners",
    "install_function_factory",
    "interval_align_score",
    "lag_window",
    "last_value_agg",
    "lead_window",
    "list_compact",
    "list_extract",
    "list_unique",
    "list_unique_sorted",
    "load_df_plugin",
    "map_entries",
    "map_extract",
    "map_get_default",
    "map_keys",
    "map_normalize",
    "map_values",
    "prefixed_hash64",
    "prefixed_hash_parts64",
    "qname_normalize",
    "register_df_plugin",
    "register_df_plugin_table_functions",
    "register_df_plugin_table_providers",
    "register_df_plugin_udfs",
    "registry_snapshot",
    "row_number_window",
    "semantic_tag",
    "span_contains",
    "span_id",
    "span_len",
    "span_make",
    "span_overlaps",
    "stable_hash64",
    "stable_hash128",
    "stable_hash_any",
    "stable_id",
    "stable_id_parts",
    "string_agg",
    "struct_pick",
    "udf_docs_snapshot",
    "udf_expr",
    "union_extract",
    "union_tag",
    "utf8_normalize",
    "utf8_null_if_blank",
]
