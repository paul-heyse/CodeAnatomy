"""Dynamic view registry for DataFusion schema views."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from functools import partial
from typing import Final

from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr

from datafusion_engine.schema_registry import nested_base_df, nested_dataset_names
from datafusion_engine.view_registry_defs import VIEW_BASE_TABLE, VIEW_SELECT_EXPRS
from datafusion_ext import map_entries, map_keys, map_values, union_extract, union_tag
from schema_spec.view_specs import ViewSpec, view_spec_from_builder

NESTED_VIEW_NAMES: Final[frozenset[str]] = frozenset(nested_dataset_names())

ATTRS_VIEW_BASE: Final[dict[str, str]] = {
    "ast_call_attrs": "ast_calls",
    "ast_def_attrs": "ast_defs",
    "ast_edge_attrs": "ast_edges",
    "ast_node_attrs": "ast_nodes",
    "cst_callsites_attrs": "cst_callsites",
    "cst_defs_attrs": "cst_defs",
    "cst_edges_attrs": "cst_edges",
    "cst_imports_attrs": "cst_imports",
    "cst_nodes_attrs": "cst_nodes",
    "cst_refs_attrs": "cst_refs",
    "py_bc_cfg_edge_attrs": "py_bc_cfg_edges",
    "py_bc_error_attrs": "bytecode_errors",
    "py_bc_instruction_attrs": "py_bc_instructions",
    "symtable_symbol_attrs": "symtable_symbols",
}

ATTRS_RAW_BASE: Final[frozenset[str]] = frozenset(
    {
        "py_bc_cfg_edge_attrs",
        "py_bc_error_attrs",
        "py_bc_instruction_attrs",
        "symtable_symbol_attrs",
    }
)

MAP_KEYS_VIEW_BASE: Final[dict[str, str]] = {
    "py_bc_instruction_attr_keys": "py_bc_instructions",
}

MAP_VALUES_VIEW_BASE: Final[dict[str, str]] = {
    "py_bc_instruction_attr_values": "py_bc_instructions",
}

CST_SPAN_UNNEST_BASE: Final[dict[str, str]] = {
    "cst_callsite_span_unnest": "cst_callsites",
    "cst_def_span_unnest": "cst_defs",
    "cst_ref_span_unnest": "cst_refs",
}


def _nested_base_df(ctx: SessionContext, name: str) -> DataFrame:
    return nested_base_df(ctx, name)


def _should_skip_exprs(exprs: Sequence[object]) -> bool:
    return not exprs


def _arrow_cast(expr: Expr, data_type: str) -> Expr:
    return f.arrow_cast(expr, lit(data_type))


def _base_df(ctx: SessionContext, name: str) -> DataFrame:
    if name in NESTED_VIEW_NAMES:
        return _nested_base_df(ctx, name)
    base_table = VIEW_BASE_TABLE.get(name)
    if base_table is None:
        msg = f"Missing base table mapping for view {name!r}."
        raise KeyError(msg)
    return ctx.table(base_table)


def _map_entries_view_df(
    ctx: SessionContext,
    *,
    name: str,
    base_view: str,
    raw_base: bool,
) -> DataFrame:
    base_df = _nested_base_df(ctx, base_view) if raw_base else _view_df(ctx, base_view)
    df = base_df.with_column("kv", map_entries(col("attrs")))
    df = df.unnest_columns("kv")
    exprs = VIEW_SELECT_EXPRS[name]
    return df.select(*exprs)


def _map_keys_view_df(ctx: SessionContext, *, name: str, base_view: str) -> DataFrame:
    base_df = _nested_base_df(ctx, base_view)
    df = base_df.with_column("attr_key", map_keys(col("attrs")))
    df = df.unnest_columns("attr_key")
    exprs = [expr for expr in VIEW_SELECT_EXPRS[name] if expr.schema_name() != "attr_key"]
    exprs.append(col("attr_key").alias("attr_key"))
    return df.select(*exprs)


def _map_values_view_df(ctx: SessionContext, *, _name: str, base_view: str) -> DataFrame:
    base_df = _nested_base_df(ctx, base_view)
    df = base_df.with_column("attr_value", map_values(col("attrs")))
    df = df.unnest_columns("attr_value")
    return df.select(
        col("file_id"),
        col("path"),
        col("code_unit_id"),
        col("instr_index"),
        col("offset"),
        col("attr_value"),
        union_tag(col("attr_value")).alias("attr_kind"),
        union_extract(col("attr_value"), "int_value").alias("attr_int"),
        union_extract(col("attr_value"), "bool_value").alias("attr_bool"),
        union_extract(col("attr_value"), "str_value").alias("attr_str"),
    )


def _cst_span_unnest_df(ctx: SessionContext, *, name: str, base_view: str) -> DataFrame:
    base_df = _view_df(ctx, base_view)
    if name == "cst_callsite_span_unnest":
        return base_df.select(
            col("file_id"),
            col("path"),
            col("call_id"),
            col("call_bstart").alias("bstart"),
            col("call_bend").alias("bend"),
        )
    if name == "cst_def_span_unnest":
        return base_df.select(
            col("file_id"),
            col("path"),
            col("def_id"),
            col("def_bstart").alias("bstart"),
            col("def_bend").alias("bend"),
        )
    return base_df.select(
        col("file_id"),
        col("path"),
        col("ref_id"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
    )


def _symtable_class_methods_df(ctx: SessionContext) -> DataFrame:
    scopes = _view_df(ctx, "symtable_scopes")
    expanded = scopes.unnest_columns("class_methods")
    return expanded.select(
        col("file_id"),
        col("path"),
        col("scope_id"),
        col("scope_name"),
        col("class_methods").alias("method_name"),
    )


def _symtable_function_partitions_df(ctx: SessionContext) -> DataFrame:
    scopes = _view_df(ctx, "symtable_scopes")
    filtered = scopes.filter(~col("function_partitions").is_null())
    exprs = VIEW_SELECT_EXPRS["symtable_function_partitions"]
    return filtered.select(*exprs)


def _symtable_scope_edges_df(ctx: SessionContext) -> DataFrame:
    scopes = _view_df(ctx, "symtable_scopes")
    parent = scopes.select(
        col("table_id").alias("parent_table_id"),
        col("scope_id").alias("parent_scope_id"),
        col("path").alias("parent_path"),
    )
    child = scopes.select(
        col("table_id").alias("child_table_id"),
        col("scope_id").alias("child_scope_id"),
        col("path").alias("child_path"),
    )
    joined = scopes.join(
        parent,
        join_keys=(["parent_table_id", "path"], ["parent_table_id", "parent_path"]),
        how="left",
        coalesce_duplicate_keys=True,
    )
    joined = joined.join(
        child,
        join_keys=(["table_id", "path"], ["child_table_id", "child_path"]),
        how="left",
        coalesce_duplicate_keys=True,
    )
    return joined.select(
        col("file_id"),
        col("path"),
        col("parent_table_id"),
        col("table_id").alias("child_table_id"),
        col("parent_scope_id"),
        col("child_scope_id"),
    )


def _symtable_namespace_edges_df(ctx: SessionContext) -> DataFrame:
    symbols = _view_df(ctx, "symtable_symbols")
    expanded = symbols.with_column("child_table_id", col("namespace_block_ids")).unnest_columns(
        "child_table_id"
    )
    scopes = _view_df(ctx, "symtable_scopes").select(
        col("table_id").alias("child_table_id"),
        col("scope_id").alias("child_scope_id"),
        col("path").alias("child_path"),
    )
    joined = expanded.join(
        scopes,
        join_keys=(["child_table_id", "path"], ["child_table_id", "child_path"]),
        how="left",
        coalesce_duplicate_keys=True,
    )
    return joined.select(
        col("file_id"),
        col("path"),
        col("scope_id"),
        col("name").alias("symbol_name"),
        col("child_table_id"),
        col("child_scope_id"),
        col("namespace_count"),
    )


def _ts_ast_check_df(ctx: SessionContext, *, ts_view: str, ast_view: str, label: str) -> DataFrame:
    ts = _view_df(ctx, ts_view).select(
        col("file_id"),
        col("path"),
        col("start_byte").alias("ts_start_byte"),
        col("end_byte").alias("ts_end_byte"),
    )
    ast_base = _view_df(ctx, ast_view).select(
        col("file_id"),
        col("path"),
        col("lineno"),
        col("col_offset"),
        col("end_lineno"),
        col("end_col_offset"),
        col("line_base"),
    )
    ast_base = ast_base.with_column("start_line_no", col("lineno") - col("line_base")).with_column(
        "end_line_no",
        col("end_lineno") - col("line_base"),
    )
    start_idx = ctx.table("file_line_index_v1").select(
        col("file_id").alias("start_file_id"),
        col("path").alias("start_path"),
        col("line_no").alias("start_line_no"),
        col("line_start_byte").alias("start_line_start_byte"),
    )
    end_idx = ctx.table("file_line_index_v1").select(
        col("file_id").alias("end_file_id"),
        col("path").alias("end_path"),
        col("line_no").alias("end_line_no"),
        col("line_start_byte").alias("end_line_start_byte"),
    )
    ast_joined = ast_base.join(
        start_idx,
        join_keys=(["file_id", "path", "start_line_no"], ["start_file_id", "start_path", "start_line_no"]),
        how="left",
        coalesce_duplicate_keys=True,
    )
    ast_joined = ast_joined.join(
        end_idx,
        join_keys=(["file_id", "path", "end_line_no"], ["end_file_id", "end_path", "end_line_no"]),
        how="left",
        coalesce_duplicate_keys=True,
    )
    ast = ast_joined.select(
        col("file_id"),
        col("path"),
        (col("start_line_start_byte") + _arrow_cast(col("col_offset"), "Int64")).alias(
            "ast_start_byte"
        ),
        (col("end_line_start_byte") + _arrow_cast(col("end_col_offset"), "Int64")).alias(
            "ast_end_byte"
        ),
    )
    joined = ts.join(
        ast,
        join_keys=(["file_id", "path", "ts_start_byte", "ts_end_byte"], ["file_id", "path", "ast_start_byte", "ast_end_byte"]),
        how="full",
        coalesce_duplicate_keys=True,
    )
    ts_present = col("ts_start_byte").is_not_null()
    ast_present = col("ast_start_byte").is_not_null()
    ts_only_expr = f.when(ts_present & col("ast_start_byte").is_null(), lit(1)).otherwise(lit(0))
    ast_only_expr = f.when(col("ts_start_byte").is_null() & ast_present, lit(1)).otherwise(lit(0))
    agg = joined.aggregate(
        [col("file_id"), col("path")],
        [
            f.sum(f.when(ts_present, lit(1)).otherwise(lit(0))).alias(f"ts_{label}"),
            f.sum(f.when(ast_present, lit(1)).otherwise(lit(0))).alias(f"ast_{label}"),
            f.sum(ts_only_expr).alias("ts_only"),
            f.sum(ast_only_expr).alias("ast_only"),
        ],
    )
    agg = agg.with_column("mismatch_count", col("ts_only") + col("ast_only"))
    return agg.with_column("mismatch", col("mismatch_count") > lit(0))


def _ts_cst_docstrings_check_df(ctx: SessionContext) -> DataFrame:
    ts = _view_df(ctx, "ts_docstrings").select(
        col("file_id"),
        col("path"),
        col("start_byte").alias("ts_start_byte"),
        col("end_byte").alias("ts_end_byte"),
    )
    cst = _view_df(ctx, "cst_docstrings").select(
        col("file_id"),
        col("path"),
        _arrow_cast(col("bstart"), "Int64").alias("cst_start_byte"),
        _arrow_cast(col("bend"), "Int64").alias("cst_end_byte"),
    )
    joined = ts.join(
        cst,
        join_keys=(["file_id", "path", "ts_start_byte", "ts_end_byte"], ["file_id", "path", "cst_start_byte", "cst_end_byte"]),
        how="full",
        coalesce_duplicate_keys=True,
    )
    ts_present = col("ts_start_byte").is_not_null()
    cst_present = col("cst_start_byte").is_not_null()
    ts_only_expr = f.when(ts_present & col("cst_start_byte").is_null(), lit(1)).otherwise(lit(0))
    cst_only_expr = f.when(col("ts_start_byte").is_null() & cst_present, lit(1)).otherwise(lit(0))
    agg = joined.aggregate(
        [col("file_id"), col("path")],
        [
            f.sum(f.when(ts_present, lit(1)).otherwise(lit(0))).alias("ts_docstrings"),
            f.sum(f.when(cst_present, lit(1)).otherwise(lit(0))).alias("cst_docstrings"),
            f.sum(ts_only_expr).alias("ts_only"),
            f.sum(cst_only_expr).alias("cst_only"),
        ],
    )
    agg = agg.with_column("mismatch_count", col("ts_only") + col("cst_only"))
    return agg.with_column("mismatch", col("mismatch_count") > lit(0))


def _view_df(ctx: SessionContext, name: str) -> DataFrame:
    builder = _CUSTOM_BUILDERS.get(name)
    if builder is not None:
        return builder(ctx)
    base_df = _base_df(ctx, name)
    if name in NESTED_VIEW_NAMES:
        return base_df
    exprs = VIEW_SELECT_EXPRS.get(name, ())
    if _should_skip_exprs(exprs):
        return base_df
    return base_df.select(*exprs)


def _register_builder_map() -> dict[str, Callable[[SessionContext], DataFrame]]:
    builders: dict[str, Callable[[SessionContext], DataFrame]] = {}
    for view_name, base_view in ATTRS_VIEW_BASE.items():
        raw_base = view_name in ATTRS_RAW_BASE
        builders[view_name] = partial(
            _map_entries_view_df,
            name=view_name,
            base_view=base_view,
            raw_base=raw_base,
        )
    for view_name, base_view in MAP_KEYS_VIEW_BASE.items():
        builders[view_name] = partial(
            _map_keys_view_df,
            name=view_name,
            base_view=base_view,
        )
    for view_name, base_view in MAP_VALUES_VIEW_BASE.items():
        builders[view_name] = partial(
            _map_values_view_df,
            _name=view_name,
            base_view=base_view,
        )
    for view_name, base_view in CST_SPAN_UNNEST_BASE.items():
        builders[view_name] = partial(
            _cst_span_unnest_df,
            name=view_name,
            base_view=base_view,
        )
    builders["symtable_class_methods"] = _symtable_class_methods_df
    builders["symtable_function_partitions"] = _symtable_function_partitions_df
    builders["symtable_namespace_edges"] = _symtable_namespace_edges_df
    builders["symtable_scope_edges"] = _symtable_scope_edges_df
    builders["ts_ast_calls_check"] = partial(
        _ts_ast_check_df,
        ts_view="ts_calls",
        ast_view="ast_calls",
        label="calls",
    )
    builders["ts_ast_defs_check"] = partial(
        _ts_ast_check_df,
        ts_view="ts_defs",
        ast_view="ast_defs",
        label="defs",
    )
    builders["ts_ast_imports_check"] = partial(
        _ts_ast_check_df,
        ts_view="ts_imports",
        ast_view="ast_imports",
        label="imports",
    )
    builders["ts_cst_docstrings_check"] = _ts_cst_docstrings_check_df
    return builders


_CUSTOM_BUILDERS: Final[dict[str, Callable[[SessionContext], DataFrame]]] = _register_builder_map()


def registry_view_specs(
    ctx: SessionContext,
    *,
    exclude: Sequence[str] | None = None,
) -> tuple[ViewSpec, ...]:
    """Return ViewSpecs for registry views using builder definitions.

    Parameters
    ----------
    ctx:
        DataFusion session context used to infer schemas.
    exclude:
        Optional view names to skip.

    Returns
    -------
    tuple[ViewSpec, ...]
        View specifications for registry views.
    """
    excluded = set(exclude or ())
    specs: list[ViewSpec] = []
    for name in sorted(VIEW_SELECT_EXPRS):
        if name in excluded:
            continue
        builder = partial(_view_df, name=name)
        specs.append(view_spec_from_builder(ctx, name=name, builder=builder, sql=None))
    return tuple(specs)


def register_all_views(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: object | None = None,
    include_registry_views: bool = True,
) -> None:
    """Register registry + pipeline views in dependency order."""
    if include_registry_views:
        from datafusion_engine.runtime import register_view_specs

        fragment_views = registry_view_specs(ctx)
        if fragment_views:
            register_view_specs(
                ctx,
                views=fragment_views,
                runtime_profile=runtime_profile,
                validate=True,
            )
    from datafusion_engine.view_graph_registry import register_view_graph
    from datafusion_engine.view_registry_specs import view_graph_nodes

    nodes = view_graph_nodes(ctx, snapshot=snapshot)
    register_view_graph(ctx, nodes=nodes, snapshot=snapshot)


__all__ = ["register_all_views", "registry_view_specs"]
