"""Build DataFusion views for symtable-derived datasets."""

from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr

from datafusion_engine.udf.expr import udf_expr

MAX_SCOPE_PARENT_DEPTH = 16


def _prefixed_hash_expr(_ctx: SessionContext, prefix: str, *parts: Expr) -> Expr:
    combined = f.concat_ws(":", *parts)
    return udf_expr("prefixed_hash64", prefix, combined)


def _scope_type_filter(scope_type: Expr) -> Expr:
    return (scope_type == lit("TYPE_PARAMETERS")) | (scope_type == lit("TYPE_VARIABLE"))


def _scope_type_expr(scope_df: DataFrame) -> Expr:
    schema_names: list[str] = (
        list(scope_df.schema().names) if hasattr(scope_df.schema(), "names") else []
    )
    if "scope_type" in schema_names:
        return col("scope_type")
    if "block_type" in schema_names:
        return col("block_type")
    return col("scope_type_value")


def _schema_names(df: DataFrame) -> list[str]:
    if hasattr(df.schema(), "names"):
        return list(df.schema().names)
    return []


def _has_column(schema_names: list[str], name: str) -> bool:
    if name in schema_names:
        return True
    dotted_suffix = f".{name}"
    return any(schema_name.endswith(dotted_suffix) for schema_name in schema_names)


def _resolved_column_expr(schema_names: list[str], *candidates: str) -> Expr | None:
    for name in candidates:
        if name in schema_names:
            return col(name)
    for name in candidates:
        dotted_suffix = f".{name}"
        for schema_name in schema_names:
            if schema_name.endswith(dotted_suffix):
                return col(schema_name)
    return None


def _scope_column_spec(scope_df: DataFrame) -> tuple[Expr, str, str]:
    schema_names = _schema_names(scope_df)
    scope_type_expr = _scope_type_expr(scope_df)
    scope_name_col = "scope_name" if "scope_name" in schema_names else "name"
    scope_lineno_col = "lineno" if "lineno" in schema_names else "lineno1"
    return scope_type_expr, scope_name_col, scope_lineno_col


def _symbol_flag_exprs(symbols_df: DataFrame) -> tuple[str, dict[str, Expr]]:
    schema_names = _schema_names(symbols_df)
    symbol_name_col = "symbol_name" if "symbol_name" in schema_names else "name"
    flags_expr = col("flags") if "flags" in schema_names else None

    def _flag(name: str) -> Expr:
        if flags_expr is None:
            return col(name)
        return flags_expr[name]

    return symbol_name_col, {
        "is_referenced": _flag("is_referenced"),
        "is_assigned": _flag("is_assigned"),
        "is_nonlocal": _flag("is_nonlocal"),
        "is_global": _flag("is_global"),
        "is_declared_global": _flag("is_declared_global"),
        "is_free": _flag("is_free"),
        "is_parameter": _flag("is_parameter"),
        "is_imported": _flag("is_imported"),
        "is_namespace": _flag("is_namespace"),
        "is_annotated": _flag("is_annotated"),
    }


def _binding_field_expr(
    schema_names: list[str],
    *,
    canonical: str,
    alternatives: tuple[str, ...] = (),
    default: Expr | None = None,
    relation: str = "symtable_bindings",
) -> Expr:
    resolved = _resolved_column_expr(schema_names, canonical, *alternatives)
    if resolved is not None:
        return resolved
    if default is not None:
        return default
    msg = f"{relation} is missing required column {canonical!r} (alternatives={alternatives!r})."
    raise ValueError(msg)


def _normalized_bindings_df(bindings_df: DataFrame) -> DataFrame:
    """Return ``symtable_bindings`` projected to canonical columns.

    Raises:
        ValueError: If required canonical binding columns are missing.
    """
    schema_names = _schema_names(bindings_df)
    required_columns = (
        "file_id",
        "path",
        "scope_id",
        "binding_id",
        "name",
        "binding_kind",
        "declared_here",
    )
    missing = [name for name in required_columns if not _has_column(schema_names, name)]
    if missing:
        msg = (
            "symtable_bindings must expose canonical columns "
            f"{required_columns!r}; missing={tuple(missing)!r}."
        )
        raise ValueError(msg)
    return bindings_df.select(
        _binding_field_expr(schema_names, canonical="file_id").alias("file_id"),
        _binding_field_expr(schema_names, canonical="path").alias("path"),
        _binding_field_expr(schema_names, canonical="scope_id").alias("scope_id"),
        _binding_field_expr(schema_names, canonical="binding_id").alias("binding_id"),
        _binding_field_expr(schema_names, canonical="name").alias("name"),
        _binding_field_expr(schema_names, canonical="binding_kind").alias("binding_kind"),
        _binding_field_expr(schema_names, canonical="declared_here")
        .cast(pa.bool_())
        .alias("declared_here"),
    )


def _scope_edges_df(ctx: SessionContext) -> DataFrame:
    edges_df = ctx.table("symtable_scope_edges")
    edge_schema_names = _schema_names(edges_df)
    if _has_column(edge_schema_names, "child_scope_id") and _has_column(
        edge_schema_names, "parent_scope_id"
    ):
        return edges_df.select(
            _binding_field_expr(
                edge_schema_names,
                canonical="path",
                relation="symtable_scope_edges",
            ).alias("path"),
            _binding_field_expr(
                edge_schema_names,
                canonical="child_scope_id",
                relation="symtable_scope_edges",
            ).alias("child_scope_id"),
            _binding_field_expr(
                edge_schema_names,
                canonical="parent_scope_id",
                relation="symtable_scope_edges",
            ).alias("parent_scope_id"),
        )
    scopes = ctx.table("symtable_scopes")
    children = scopes.select(
        col("path"),
        col("block_id").alias("child_block_id"),
        col("parent_block_id").alias("child_parent_block_id"),
        col("scope_id").alias("child_scope_id"),
    )
    parents = scopes.select(
        col("path").alias("parent_path"),
        col("block_id").alias("parent_block_id"),
        col("scope_id").alias("parent_scope_id"),
    )
    return children.join(
        parents,
        join_keys=(["path", "child_parent_block_id"], ["parent_path", "parent_block_id"]),
        how="left",
        coalesce_duplicate_keys=True,
    ).select(
        col("path"),
        col("child_scope_id"),
        col("parent_scope_id"),
    )


def symtable_bindings_df(ctx: SessionContext) -> DataFrame:
    """Return a DataFrame for symtable binding rows.

    Returns:
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame of binding records.
    """
    scope_df = ctx.table("symtable_scopes")
    scope_type_expr, scope_name_col, scope_lineno_col = _scope_column_spec(scope_df)
    scopes = scope_df.select(
        col("scope_id"),
        col("path"),
        scope_type_expr.alias("scope_type"),
        col(scope_name_col).alias("scope_name"),
        col(scope_lineno_col).alias("scope_lineno"),
    )
    symbols_df = ctx.table("symtable_symbols")
    symbol_name_col, flag_exprs = _symbol_flag_exprs(symbols_df)
    symbols = symbols_df.select(
        col("file_id"),
        col("path"),
        col("scope_id"),
        col(symbol_name_col).alias("symbol_name"),
        flag_exprs["is_referenced"].alias("is_referenced"),
        flag_exprs["is_assigned"].alias("is_assigned"),
        flag_exprs["is_nonlocal"].alias("is_nonlocal"),
        flag_exprs["is_global"].alias("is_global"),
        flag_exprs["is_declared_global"].alias("is_declared_global"),
        flag_exprs["is_free"].alias("is_free"),
        flag_exprs["is_parameter"].alias("is_parameter"),
        flag_exprs["is_imported"].alias("is_imported"),
        flag_exprs["is_namespace"].alias("is_namespace"),
        flag_exprs["is_annotated"].alias("is_annotated"),
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

    Returns:
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame of definition site records.
    """
    bindings = _normalized_bindings_df(ctx.table("symtable_bindings"))
    defs = ctx.table("cst_defs").select(
        col("def_id").alias("def_id"),
        col("path").alias("def_path"),
        col("name").alias("def_name"),
        col("name_bstart").alias("name_bstart"),
        col("name_bend").alias("name_bend"),
        col("def_bstart").alias("def_bstart"),
        col("def_bend").alias("def_bend"),
        col("kind").alias("kind"),
    )
    joined = bindings.join(
        defs,
        left_on=["path", "name"],
        right_on=["def_path", "def_name"],
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

    Returns:
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame of use site records.
    """
    bindings = _normalized_bindings_df(ctx.table("symtable_bindings"))
    refs = ctx.table("cst_refs").select(
        col("ref_id").alias("ref_id"),
        col("path").alias("ref_path"),
        col("ref_text").alias("ref_text"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
        col("expr_ctx").alias("expr_ctx"),
    )
    joined = bindings.join(
        refs,
        left_on=["path", "name"],
        right_on=["ref_path", "ref_text"],
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

    Returns:
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame of type parameter records.
    """
    scopes = ctx.table("symtable_scopes")
    scope_schema_names: list[str] = (
        list(scopes.schema().names) if hasattr(scopes.schema(), "names") else []
    )
    scope_name_col = "scope_name" if "scope_name" in scope_schema_names else "name"
    scope_type_expr = _scope_type_expr(scopes)
    filtered = scopes.filter(_scope_type_filter(scope_type_expr))
    variance = lit(None).cast(pa.string())
    return filtered.select(
        col("file_id"),
        col("path"),
        col("scope_id").alias("type_param_id"),
        col("scope_id"),
        col(scope_name_col).alias("name"),
        variance.alias("variance"),
    )


def symtable_type_param_edges_df(ctx: SessionContext) -> DataFrame:
    """Return a DataFrame for symtable type-parameter edge rows.

    Returns:
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame of type-parameter edge records.
    """
    edges = _scope_edges_df(ctx)
    scope_df = ctx.table("symtable_scopes")
    scope_type_expr = _scope_type_expr(scope_df)
    scopes = scope_df.select(
        col("scope_id"),
        col("path").alias("scope_path"),
        scope_type_expr.alias("scope_type"),
    )
    joined = edges.join(
        scopes,
        join_keys=(["child_scope_id", "path"], ["scope_id", "scope_path"]),
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


def symtable_binding_resolutions_df(ctx: SessionContext) -> DataFrame:
    """Return a DataFrame resolving symtable bindings to outer scopes.

    Returns:
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame of binding resolution edges.
    """
    bindings = _resolution_bindings(ctx)
    declared = _declared_bindings(ctx)
    module_scopes = _module_scopes(ctx)
    scope_types = _scope_types(ctx)
    ancestors = _scope_ancestor_edges(ctx, max_depth=MAX_SCOPE_PARENT_DEPTH)

    global_edges = _global_resolution_edges(bindings, module_scopes, declared)
    nonlocal_edges = _nonlocal_resolution_edges(bindings, ancestors, declared, scope_types)
    free_edges = _free_resolution_edges(bindings, ancestors, declared, module_scopes)

    combined = global_edges.union(nonlocal_edges).union(free_edges)
    return combined.distinct()


def _global_resolution_edges(
    bindings: DataFrame,
    module_scopes: DataFrame,
    declared: DataFrame,
) -> DataFrame:
    global_bindings = bindings.filter(col("binding_kind") == lit("global_ref"))
    global_candidates = _module_candidates(global_bindings, module_scopes, declared)
    return _resolution_output(
        global_candidates,
        kind="GLOBAL",
        reason="module_scope",
        confidence=0.9,
    )


def _nonlocal_resolution_edges(
    bindings: DataFrame,
    ancestors: DataFrame,
    declared: DataFrame,
    scope_types: DataFrame,
) -> DataFrame:
    nonlocal_bindings = bindings.filter(col("binding_kind") == lit("nonlocal_ref"))
    nonlocal_candidates = _ancestor_candidates(nonlocal_bindings, ancestors, declared)
    nonlocal_scoped = _with_scope_type(nonlocal_candidates, scope_types)
    nonlocal_filtered = nonlocal_scoped.filter(
        col("scope_type").is_null() | (col("scope_type") != lit("MODULE"))
    )
    return _resolution_output(
        nonlocal_filtered,
        kind="NONLOCAL",
        reason="enclosing_scope",
        confidence=0.85,
    )


def _free_resolution_edges(
    bindings: DataFrame,
    ancestors: DataFrame,
    declared: DataFrame,
    module_scopes: DataFrame,
) -> DataFrame:
    free_bindings = bindings.filter(col("binding_kind") == lit("free_ref"))
    free_candidates = _ancestor_candidates(free_bindings, ancestors, declared)
    free_edges = _resolution_output(
        free_candidates,
        kind="FREE",
        reason="enclosing_or_module_scope",
        confidence=0.75,
    )
    free_module_edges = _resolution_output(
        _module_candidates(free_bindings, module_scopes, declared),
        kind="FREE",
        reason="enclosing_or_module_scope",
        confidence=0.75,
    )
    return free_edges.union(free_module_edges)


def _resolution_bindings(ctx: SessionContext) -> DataFrame:
    bindings = _normalized_bindings_df(ctx.table("symtable_bindings"))
    return bindings.select(
        col("binding_id").cast(pa.string()),
        col("scope_id").cast(pa.string()),
        col("name").cast(pa.string()),
        col("binding_kind").cast(pa.string()),
        col("path").cast(pa.string()),
    )


def _declared_bindings(ctx: SessionContext) -> DataFrame:
    bindings = _normalized_bindings_df(ctx.table("symtable_bindings"))
    return bindings.filter(col("declared_here")).select(
        col("path").cast(pa.string()).alias("declared_path"),
        col("scope_id").cast(pa.string()).alias("declared_scope_id"),
        col("name").cast(pa.string()).alias("declared_name"),
        col("binding_id").cast(pa.string()).alias("outer_binding_id"),
    )


def _module_scopes(ctx: SessionContext) -> DataFrame:
    scopes = ctx.table("symtable_scopes")
    scope_type_expr = _scope_type_expr(scopes)
    return scopes.filter(scope_type_expr == lit("MODULE")).select(
        col("path").cast(pa.string()).alias("module_path"),
        col("scope_id").cast(pa.string()).alias("module_scope_id"),
    )


def _scope_types(ctx: SessionContext) -> DataFrame:
    scopes = ctx.table("symtable_scopes")
    scope_type_expr = _scope_type_expr(scopes)
    return scopes.select(
        col("path").cast(pa.string()).alias("scope_path"),
        col("scope_id").cast(pa.string()).alias("scope_scope_id"),
        scope_type_expr.cast(pa.string()).alias("scope_type"),
    )


def _scope_ancestor_edges(ctx: SessionContext, *, max_depth: int) -> DataFrame:
    edges = _scope_edges_df(ctx).select(
        col("path").cast(pa.string()).alias("edge_path"),
        col("child_scope_id").cast(pa.string()).alias("edge_child_scope_id"),
        col("parent_scope_id").cast(pa.string()).alias("edge_parent_scope_id"),
    )
    ancestors = edges.select(
        col("edge_path").alias("path"),
        col("edge_child_scope_id").alias("child_scope_id"),
        col("edge_parent_scope_id").alias("ancestor_scope_id"),
        lit(1).alias("depth"),
    )
    current = ancestors
    for depth in range(2, max_depth + 1):
        current = current.join(
            edges,
            join_keys=(["path", "ancestor_scope_id"], ["edge_path", "edge_child_scope_id"]),
            how="inner",
            coalesce_duplicate_keys=True,
        ).select(
            col("path"),
            col("child_scope_id"),
            col("edge_parent_scope_id").alias("ancestor_scope_id"),
            lit(depth).alias("depth"),
        )
        ancestors = ancestors.union(current)
    return ancestors


def _ancestor_candidates(
    bindings: DataFrame,
    ancestors: DataFrame,
    declared: DataFrame,
) -> DataFrame:
    ancestor_keys = ancestors.select(
        col("path").alias("ancestor_path"),
        col("child_scope_id").alias("ancestor_child_scope_id"),
        col("ancestor_scope_id").alias("ancestor_scope_id"),
    )
    chained = bindings.join(
        ancestor_keys,
        join_keys=(["path", "scope_id"], ["ancestor_path", "ancestor_child_scope_id"]),
        how="inner",
        coalesce_duplicate_keys=True,
    )
    return chained.join(
        declared,
        join_keys=(
            ["path", "ancestor_scope_id", "name"],
            ["declared_path", "declared_scope_id", "declared_name"],
        ),
        how="inner",
        coalesce_duplicate_keys=True,
    )


def _module_candidates(
    bindings: DataFrame,
    module_scopes: DataFrame,
    declared: DataFrame,
) -> DataFrame:
    with_module = bindings.join(
        module_scopes,
        join_keys=(["path"], ["module_path"]),
        how="inner",
        coalesce_duplicate_keys=True,
    )
    return with_module.join(
        declared,
        join_keys=(
            ["path", "module_scope_id", "name"],
            ["declared_path", "declared_scope_id", "declared_name"],
        ),
        how="inner",
        coalesce_duplicate_keys=True,
    )


def _with_scope_type(candidates: DataFrame, scope_types: DataFrame) -> DataFrame:
    return candidates.join(
        scope_types,
        join_keys=(["path", "ancestor_scope_id"], ["scope_path", "scope_scope_id"]),
        how="left",
        coalesce_duplicate_keys=True,
    )


def _resolution_output(
    candidates: DataFrame,
    *,
    kind: str,
    reason: str,
    confidence: float,
) -> DataFrame:
    return candidates.select(
        col("binding_id"),
        col("outer_binding_id"),
        lit(kind).alias("kind"),
        lit(reason).alias("reason"),
        col("path"),
        f.concat_ws(":", col("binding_id"), lit(kind)).alias("ambiguity_group_id"),
        lit(confidence).cast(pa.float32()).alias("confidence"),
    )


__all__ = [
    "symtable_binding_resolutions_df",
    "symtable_bindings_df",
    "symtable_def_sites_df",
    "symtable_type_param_edges_df",
    "symtable_type_params_df",
    "symtable_use_sites_df",
]
