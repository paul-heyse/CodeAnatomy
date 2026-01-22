"""DataFrame builders for symtable-derived CPG views."""

from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr

from datafusion_engine.udf_registry import datafusion_scalar_udf_map, register_datafusion_udfs


def _prefixed_hash_expr(ctx: SessionContext, prefix: str, *parts: Expr) -> Expr:
    register_datafusion_udfs(ctx)
    udf_map = datafusion_scalar_udf_map()
    udf = udf_map.get("prefixed_hash64")
    if udf is None:
        msg = "prefixed_hash64 UDF is not registered."
        raise ValueError(msg)
    combined = f.concat_ws(":", *parts)
    return udf(lit(prefix), combined)


def _scope_type_filter(scope_type: Expr) -> Expr:
    return (scope_type == lit("TYPE_PARAMETERS")) | (scope_type == lit("TYPE_VARIABLE"))


def symtable_bindings_df(ctx: SessionContext) -> DataFrame:
    """Return a DataFrame for symtable binding rows.

    Returns
    -------
        DataFusion DataFrame of binding records.
    """
    scopes = ctx.table("symtable_scopes").select(
        col("scope_id"),
        col("path"),
        col("scope_type"),
        col("scope_name"),
        col("lineno").alias("scope_lineno"),
    )
    symbols = ctx.table("symtable_symbols").select(
        col("file_id"),
        col("path"),
        col("scope_id"),
        col("symbol_name"),
        col("is_referenced"),
        col("is_assigned"),
        col("is_nonlocal"),
        col("is_global"),
        col("is_declared_global"),
        col("is_free"),
        col("is_parameter"),
        col("is_imported"),
        col("is_namespace"),
        col("is_annotated"),
    )
    joined = symbols.join(
        scopes,
        join_keys=(["scope_id", "path"], ["scope_id", "path"]),
        how="left",
        coalesce_duplicate_keys=True,
    )
    binding_id = f.concat_ws(":", col("scope_id"), lit("BIND"), col("symbol_name"))
    binding_kind = (
        f.when(col("is_nonlocal"), lit("nonlocal_ref"))
        .when(col("is_global") | col("is_declared_global"), lit("global_ref"))
        .when(col("is_free"), lit("free_ref"))
        .when(col("is_parameter"), lit("param"))
        .when(col("is_imported"), lit("import"))
        .when(col("is_namespace"), lit("namespace"))
        .when(col("is_annotated") & ~col("is_assigned"), lit("annot_only"))
        .otherwise(lit("local"))
    )
    declared_here = (
        col("is_parameter")
        | col("is_assigned")
        | col("is_imported")
        | col("is_namespace")
        | col("is_annotated")
    )
    return joined.select(
        col("file_id"),
        col("path"),
        col("scope_id"),
        col("scope_type"),
        col("scope_name"),
        col("scope_lineno"),
        col("symbol_name").alias("name"),
        binding_id.alias("binding_id"),
        binding_kind.alias("binding_kind"),
        declared_here.alias("declared_here"),
        col("is_referenced").alias("referenced_here"),
        col("is_assigned").alias("assigned_here"),
        col("is_annotated").alias("annotated_here"),
        col("is_imported").alias("is_imported"),
        col("is_parameter").alias("is_parameter"),
        col("is_free").alias("is_free"),
        col("is_nonlocal").alias("is_nonlocal"),
        col("is_global").alias("is_global"),
        col("is_declared_global").alias("is_declared_global"),
    )


def symtable_def_sites_df(ctx: SessionContext) -> DataFrame:
    """Return a DataFrame for symtable definition site rows.

    Returns
    -------
        DataFusion DataFrame of definition site records.
    """
    bindings = symtable_bindings_df(ctx)
    defs = ctx.table("cst_defs").select(
        col("def_id"),
        col("path"),
        col("name"),
        col("name_bstart"),
        col("name_bend"),
        col("def_bstart"),
        col("def_bend"),
        col("kind"),
    )
    joined = bindings.join(
        defs,
        join_keys=(["path", "name"], ["path", "name"]),
        how="inner",
        coalesce_duplicate_keys=True,
    )
    filtered = joined.filter(col("declared_here"))
    bstart = f.coalesce(col("name_bstart"), col("def_bstart"))
    bend = f.coalesce(col("name_bend"), col("def_bend"))
    name_present = ~col("name_bstart").is_null()
    def_site_kind = (
        f.when(col("kind") == lit("class"), lit("class"))
        .when(
            (col("kind") == lit("function")) | (col("kind") == lit("async_function")),
            lit("function"),
        )
        .otherwise(lit("other"))
    )
    anchor_confidence = f.when(name_present, lit(0.95)).otherwise(lit(0.85))
    anchor_reason = f.when(name_present, lit("cst_name_span")).otherwise(lit("cst_def_span"))
    def_site_id = _prefixed_hash_expr(ctx, "py_def_site", col("binding_id"), bstart, bend)
    return filtered.select(
        col("file_id"),
        col("path"),
        col("scope_id"),
        col("binding_id"),
        col("name"),
        col("def_id"),
        bstart.alias("bstart"),
        bend.alias("bend"),
        def_site_kind.alias("def_site_kind"),
        anchor_confidence.alias("anchor_confidence"),
        anchor_reason.alias("anchor_reason"),
        col("def_id").alias("ambiguity_group_id"),
        def_site_id.alias("def_site_id"),
    )


def symtable_use_sites_df(ctx: SessionContext) -> DataFrame:
    """Return a DataFrame for symtable use site rows.

    Returns
    -------
        DataFusion DataFrame of use site records.
    """
    bindings = symtable_bindings_df(ctx)
    refs = ctx.table("cst_refs").select(
        col("ref_id"),
        col("path"),
        col("ref_text"),
        col("bstart"),
        col("bend"),
        col("expr_ctx"),
    )
    joined = bindings.join(
        refs,
        join_keys=(["path", "name"], ["path", "ref_text"]),
        how="inner",
        coalesce_duplicate_keys=True,
    )
    use_kind = (
        f.when(col("expr_ctx") == lit("Store"), lit("write"))
        .when(col("expr_ctx") == lit("Del"), lit("del"))
        .otherwise(lit("read"))
    )
    use_site_id = _prefixed_hash_expr(
        ctx,
        "py_use_site",
        col("binding_id"),
        col("bstart"),
        col("bend"),
    )
    return joined.select(
        col("file_id"),
        col("path"),
        col("scope_id"),
        col("binding_id"),
        col("name"),
        col("ref_id"),
        col("bstart"),
        col("bend"),
        use_kind.alias("use_kind"),
        lit(0.85).alias("anchor_confidence"),
        lit("cst_ref").alias("anchor_reason"),
        col("ref_id").alias("ambiguity_group_id"),
        use_site_id.alias("use_site_id"),
    )


def symtable_type_params_df(ctx: SessionContext) -> DataFrame:
    """Return a DataFrame for symtable type parameter rows.

    Returns
    -------
        DataFusion DataFrame of type parameter records.
    """
    scopes = ctx.table("symtable_scopes")
    filtered = scopes.filter(_scope_type_filter(col("scope_type")))
    variance = lit(None).cast(pa.string())
    return filtered.select(
        col("file_id"),
        col("path"),
        col("scope_id").alias("type_param_id"),
        col("scope_id"),
        col("scope_name").alias("name"),
        variance.alias("variance"),
    )


def symtable_type_param_edges_df(ctx: SessionContext) -> DataFrame:
    """Return a DataFrame for symtable type-parameter edge rows.

    Returns
    -------
        DataFusion DataFrame of type-parameter edge records.
    """
    edges = ctx.table("symtable_scope_edges").select(
        col("path"),
        col("child_scope_id"),
        col("parent_scope_id"),
    )
    scopes = ctx.table("symtable_scopes").select(
        col("scope_id"),
        col("path"),
        col("scope_type"),
    )
    joined = edges.join(
        scopes,
        join_keys=(["child_scope_id", "path"], ["scope_id", "path"]),
        how="inner",
        coalesce_duplicate_keys=True,
    )
    filtered = joined.filter(_scope_type_filter(col("scope_type")))
    return filtered.select(
        col("path"),
        col("child_scope_id").alias("type_param_id"),
        col("parent_scope_id").alias("owner_scope_id"),
        col("scope_type"),
    )


__all__ = [
    "symtable_bindings_df",
    "symtable_def_sites_df",
    "symtable_type_param_edges_df",
    "symtable_type_params_df",
    "symtable_use_sites_df",
]
