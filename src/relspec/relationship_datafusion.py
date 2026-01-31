"""DataFusion-native builders for relationship plans.

This module provides DataFusion expression/DataFrame builders for relationship
outputs, using the declarative specs from cpg.relationship_specs.

The public API functions delegate to the generic spec-based builders while
maintaining backward compatibility with existing callers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f

from cpg.kind_catalog import (
    EDGE_KIND_PY_CALLS_QNAME,
    EDGE_KIND_PY_CALLS_SYMBOL,
    EDGE_KIND_PY_DEFINES_SYMBOL,
    EDGE_KIND_PY_IMPORTS_SYMBOL,
    EDGE_KIND_PY_REFERENCES_SYMBOL,
)
from cpg.relationship_builder import (
    build_qname_relation_df,
    build_symbol_relation_df,
)
from cpg.relationship_specs import (
    REL_CALLSITE_QNAME_SPEC,
    REL_CALLSITE_SYMBOL_SPEC,
    REL_DEF_SYMBOL_SPEC,
    REL_IMPORT_SYMBOL_SPEC,
    REL_NAME_SYMBOL_SPEC,
)

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame


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
    return build_symbol_relation_df(
        ctx,
        REL_NAME_SYMBOL_SPEC,
        task_name=task_name,
        task_priority=task_priority,
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
    return build_symbol_relation_df(
        ctx,
        REL_IMPORT_SYMBOL_SPEC,
        task_name=task_name,
        task_priority=task_priority,
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
    return build_symbol_relation_df(
        ctx,
        REL_DEF_SYMBOL_SPEC,
        task_name=task_name,
        task_priority=task_priority,
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
    return build_symbol_relation_df(
        ctx,
        REL_CALLSITE_SYMBOL_SPEC,
        task_name=task_name,
        task_priority=task_priority,
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
    return build_qname_relation_df(
        ctx,
        REL_CALLSITE_QNAME_SPEC,
        task_name=task_name,
        task_priority=task_priority,
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
