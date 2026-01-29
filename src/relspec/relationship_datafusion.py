"""DataFusion-native builders for relationship plans.

This module provides DataFusion expression/DataFrame builders for relationship
outputs, replacing the deprecated SQLGlot AST builders in relationship_sql.py.
"""

from __future__ import annotations

from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame

from cpg.kind_catalog import (
    EDGE_KIND_PY_CALLS_QNAME,
    EDGE_KIND_PY_CALLS_SYMBOL,
    EDGE_KIND_PY_DEFINES_SYMBOL,
    EDGE_KIND_PY_IMPORTS_SYMBOL,
    EDGE_KIND_PY_REFERENCES_SYMBOL,
)
from datafusion_engine.expr_udf_shims import stable_id_parts


def build_rel_name_symbol_df(
    ctx: SessionContext,
    *,
    task_name: str,
    task_priority: int,
) -> DataFrame:
    """Build DataFusion DataFrame for name-based symbol relations.

    Parameters
    ----------
    ctx
        DataFusion session context for table access.
    task_name
        Task name for provenance metadata.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    DataFrame
        DataFusion DataFrame for name symbol relations.
    """
    source = ctx.table("cst_refs")
    return source.select(
        col("ref_id").alias("ref_id"),
        col("ref_text").alias("symbol"),
        col("symbol_roles").alias("symbol_roles"),
        col("path").alias("path"),
        f.coalesce(col("edge_owner_file_id"), col("file_id")).alias("edge_owner_file_id"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
        lit("cst_ref_text").alias("resolution_method"),
        lit(0.5).alias("confidence"),
        lit(0.5).alias("score"),
        lit(task_name).alias("task_name"),
        lit(task_priority).alias("task_priority"),
    )


def build_rel_import_symbol_df(
    ctx: SessionContext,
    *,
    task_name: str,
    task_priority: int,
) -> DataFrame:
    """Build DataFusion DataFrame for import-based symbol relations.

    Parameters
    ----------
    ctx
        DataFusion session context for table access.
    task_name
        Task name for provenance metadata.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    DataFrame
        DataFusion DataFrame for import symbol relations.
    """
    source = ctx.table("cst_imports")
    symbol = f.coalesce(col("name"), col("module"))
    return source.select(
        f.coalesce(col("import_alias_id"), col("import_id")).alias("import_alias_id"),
        symbol.alias("symbol"),
        col("symbol_roles").alias("symbol_roles"),
        col("path").alias("path"),
        f.coalesce(col("edge_owner_file_id"), col("file_id")).alias("edge_owner_file_id"),
        f.coalesce(col("alias_bstart"), col("stmt_bstart")).alias("bstart"),
        f.coalesce(col("alias_bend"), col("stmt_bend")).alias("bend"),
        lit("cst_import_name").alias("resolution_method"),
        lit(0.5).alias("confidence"),
        lit(0.5).alias("score"),
        lit(task_name).alias("task_name"),
        lit(task_priority).alias("task_priority"),
    )


def build_rel_def_symbol_df(
    ctx: SessionContext,
    *,
    task_name: str,
    task_priority: int,
) -> DataFrame:
    """Build DataFusion DataFrame for definition-based symbol relations.

    Parameters
    ----------
    ctx
        DataFusion session context for table access.
    task_name
        Task name for provenance metadata.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    DataFrame
        DataFusion DataFrame for definition symbol relations.
    """
    source = ctx.table("cst_defs")
    return source.select(
        col("def_id").alias("def_id"),
        col("name").alias("symbol"),
        col("symbol_roles").alias("symbol_roles"),
        col("path").alias("path"),
        f.coalesce(col("edge_owner_file_id"), col("file_id")).alias("edge_owner_file_id"),
        f.coalesce(col("name_bstart"), col("def_bstart")).alias("bstart"),
        f.coalesce(col("name_bend"), col("def_bend")).alias("bend"),
        lit("cst_def_name").alias("resolution_method"),
        lit(0.6).alias("confidence"),
        lit(0.6).alias("score"),
        lit(task_name).alias("task_name"),
        lit(task_priority).alias("task_priority"),
    )


def build_rel_callsite_symbol_df(
    ctx: SessionContext,
    *,
    task_name: str,
    task_priority: int,
) -> DataFrame:
    """Build DataFusion DataFrame for callsite symbol relations.

    Parameters
    ----------
    ctx
        DataFusion session context for table access.
    task_name
        Task name for provenance metadata.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    DataFrame
        DataFusion DataFrame for callsite symbol relations.
    """
    source = ctx.table("cst_callsites")
    symbol = f.coalesce(col("callee_text"), col("callee_dotted"))
    return source.select(
        col("call_id").alias("call_id"),
        symbol.alias("symbol"),
        col("symbol_roles").alias("symbol_roles"),
        col("path").alias("path"),
        f.coalesce(col("edge_owner_file_id"), col("file_id")).alias("edge_owner_file_id"),
        col("call_bstart").alias("call_bstart"),
        col("call_bend").alias("call_bend"),
        lit("cst_callsite").alias("resolution_method"),
        lit(0.6).alias("confidence"),
        lit(0.6).alias("score"),
        lit(task_name).alias("task_name"),
        lit(task_priority).alias("task_priority"),
    )


def build_rel_callsite_qname_df(
    ctx: SessionContext,
    *,
    task_name: str,
    task_priority: int,
) -> DataFrame:
    """Build DataFusion DataFrame for callsite qname relations.

    Parameters
    ----------
    ctx
        DataFusion session context for table access.
    task_name
        Task name for provenance metadata.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    DataFrame
        DataFusion DataFrame for callsite qname relations.
    """
    source = ctx.table("callsite_qname_candidates_v1")
    qname = col("qname")
    null_utf8 = f.arrow_cast(lit(None), lit("Utf8"))
    qname_id = f.when(qname.is_not_null(), stable_id_parts("qname", qname)).otherwise(null_utf8)
    return source.select(
        col("call_id").alias("call_id"),
        qname_id.alias("qname_id"),
        col("qname_source").alias("qname_source"),
        col("path").alias("path"),
        col("edge_owner_file_id").alias("edge_owner_file_id"),
        col("call_bstart").alias("call_bstart"),
        col("call_bend").alias("call_bend"),
        lit(0.5).alias("confidence"),
        lit(0.5).alias("score"),
        col("ambiguity_group_id").alias("ambiguity_group_id"),
        lit(task_name).alias("task_name"),
        lit(task_priority).alias("task_priority"),
    )


def build_relation_output_df(ctx: SessionContext) -> DataFrame:
    """Build DataFusion DataFrame for the relation output union.

    Parameters
    ----------
    ctx
        DataFusion session context for table access.

    Returns
    -------
    DataFrame
        DataFusion DataFrame for relation outputs.
    """
    rel_name = _relation_output_from_name(ctx)
    rel_import = _relation_output_from_import(ctx)
    rel_def = _relation_output_from_def(ctx)
    rel_call_symbol = _relation_output_from_call_symbol(ctx)
    rel_call_qname = _relation_output_from_call_qname(ctx)

    return rel_name.union(rel_import).union(rel_def).union(rel_call_symbol).union(rel_call_qname)


def _relation_output_from_name(ctx: SessionContext) -> DataFrame:
    """Build relation output from name symbol relations.

    Returns
    -------
    DataFrame
        Relation output DataFrame for name-based symbol relations.
    """
    source = ctx.table("rel_name_symbol_v1")
    return _relation_output_base(
        source,
        src_col="ref_id",
        dst_col="symbol",
        bstart_col="bstart",
        bend_col="bend",
        kind=str(EDGE_KIND_PY_REFERENCES_SYMBOL),
        origin="cst",
    )


def _relation_output_from_import(ctx: SessionContext) -> DataFrame:
    """Build relation output from import symbol relations.

    Returns
    -------
    DataFrame
        Relation output DataFrame for import-based symbol relations.
    """
    source = ctx.table("rel_import_symbol_v1")
    return _relation_output_base(
        source,
        src_col="import_alias_id",
        dst_col="symbol",
        bstart_col="bstart",
        bend_col="bend",
        kind=str(EDGE_KIND_PY_IMPORTS_SYMBOL),
        origin="cst",
    )


def _relation_output_from_def(ctx: SessionContext) -> DataFrame:
    """Build relation output from def symbol relations.

    Returns
    -------
    DataFrame
        Relation output DataFrame for definition-based symbol relations.
    """
    source = ctx.table("rel_def_symbol_v1")
    return _relation_output_base(
        source,
        src_col="def_id",
        dst_col="symbol",
        bstart_col="bstart",
        bend_col="bend",
        kind=str(EDGE_KIND_PY_DEFINES_SYMBOL),
        origin="cst",
    )


def _relation_output_from_call_symbol(ctx: SessionContext) -> DataFrame:
    """Build relation output from callsite symbol relations.

    Returns
    -------
    DataFrame
        Relation output DataFrame for callsite symbol relations.
    """
    source = ctx.table("rel_callsite_symbol_v1")
    return _relation_output_base(
        source,
        src_col="call_id",
        dst_col="symbol",
        bstart_col="call_bstart",
        bend_col="call_bend",
        kind=str(EDGE_KIND_PY_CALLS_SYMBOL),
        origin="cst",
    )


def _relation_output_from_call_qname(ctx: SessionContext) -> DataFrame:
    """Build relation output from callsite qname relations.

    Returns
    -------
    DataFrame
        Relation output DataFrame for callsite qname relations.
    """
    source = ctx.table("rel_callsite_qname_v1")
    return _relation_output_base(
        source,
        src_col="call_id",
        dst_col="qname_id",
        bstart_col="call_bstart",
        bend_col="call_bend",
        kind=str(EDGE_KIND_PY_CALLS_QNAME),
        origin="cst",
        qname_source_col="qname_source",
        ambiguity_group_col="ambiguity_group_id",
    )


def _relation_output_base(  # noqa: PLR0913
    source: DataFrame,
    *,
    src_col: str,
    dst_col: str,
    bstart_col: str,
    bend_col: str,
    kind: str,
    origin: str,
    qname_source_col: str | None = None,
    ambiguity_group_col: str | None = None,
) -> DataFrame:
    """Build base relation output schema from a source DataFrame.

    Parameters
    ----------
    source
        Source DataFrame with relation data.
    src_col
        Column name for source ID.
    dst_col
        Column name for destination ID or symbol.
    bstart_col
        Column name for byte start position.
    bend_col
        Column name for byte end position.
    kind
        Edge kind constant.
    origin
        Origin label (e.g., "cst").
    qname_source_col
        Optional column name for qname source.
    ambiguity_group_col
        Optional column name for ambiguity group ID.

    Returns
    -------
    DataFrame
        Unified relation output DataFrame.
    """
    import pyarrow as pa

    qname_source = col(qname_source_col) if qname_source_col else lit(None).cast(pa.string())
    ambiguity_group_id = (
        col(ambiguity_group_col) if ambiguity_group_col else lit(None).cast(pa.string())
    )

    return source.select(
        col(src_col).alias("src"),
        col(dst_col).alias("dst"),
        col("path").alias("path"),
        f.coalesce(col("edge_owner_file_id"), col("file_id")).alias("edge_owner_file_id"),
        col(bstart_col).alias("bstart"),
        col(bend_col).alias("bend"),
        lit(origin).alias("origin"),
        col("resolution_method").alias("resolution_method"),
        col("binding_kind").alias("binding_kind"),
        col("def_site_kind").alias("def_site_kind"),
        col("use_kind").alias("use_kind"),
        lit(kind).alias("kind"),
        col("reason").alias("reason"),
        col("confidence").alias("confidence"),
        col("score").alias("score"),
        col("symbol_roles").alias("symbol_roles"),
        qname_source.alias("qname_source"),
        ambiguity_group_id.alias("ambiguity_group_id"),
        col("diag_source").alias("diag_source"),
        col("severity").alias("severity"),
        col("task_name").alias("task_name"),
        col("task_priority").alias("task_priority"),
    )


__all__ = [
    "build_rel_callsite_qname_df",
    "build_rel_callsite_symbol_df",
    "build_rel_def_symbol_df",
    "build_rel_import_symbol_df",
    "build_rel_name_symbol_df",
    "build_relation_output_df",
]
