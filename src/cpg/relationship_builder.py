"""Generic relationship DataFrame builders driven by declarative specs.

This module provides the generic builder functions that use RelationshipSpec
and QNameRelationshipSpec to construct DataFusion DataFrames, eliminating
the need for copy-paste code in relationship_datafusion.py.

.. deprecated::
    This module is deprecated. Use :mod:`semantics.compiler.SemanticCompiler`
    instead. Planned for removal in a future version.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

# Deprecation: Remove this module in a future version once all callers
# have migrated to semantics.compiler.SemanticCompiler.
warnings.warn(
    "cpg.relationship_builder is deprecated. Use semantics.compiler.SemanticCompiler instead.",
    DeprecationWarning,
    stacklevel=2,
)

from datafusion import col, lit
from datafusion import functions as f

from cpg.relationship_specs import (
    QNAME_RELATIONSHIP_SPECS,
    SYMBOL_RELATIONSHIP_SPECS,
    ColumnRef,
    ColumnSpec,
    QNameRelationshipSpec,
    RelationshipSpec,
)
from datafusion_engine.udf.shims import stable_id_parts

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame
    from datafusion.expr import Expr


def _column_expr(spec: ColumnSpec) -> Expr:
    """Build a DataFusion expression from a column specification.

    Parameters
    ----------
    spec
        Column specification (single or coalesced).

    Returns
    -------
    Expr
        DataFusion expression for the column.
    """
    if isinstance(spec, ColumnRef):
        return col(spec.name)
    return f.coalesce(*[col(name) for name in spec.columns])


def build_symbol_relation_df(
    ctx: SessionContext,
    spec: RelationshipSpec,
    *,
    task_name: str,
    task_priority: int,
) -> DataFrame:
    """Build a DataFusion DataFrame for a symbol relationship from spec.

    Parameters
    ----------
    ctx
        DataFusion session context for table access.
    spec
        Relationship specification defining the extraction parameters.
    task_name
        Task name for provenance metadata.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    DataFrame
        DataFusion DataFrame for the symbol relationship.
    """
    source = ctx.table(spec.src_table)

    entity_id = _column_expr(spec.entity_id_col)
    symbol = _column_expr(spec.symbol_col)
    bstart = _column_expr(spec.bstart_col)
    bend = _column_expr(spec.bend_col)

    edge_owner = f.coalesce(
        col(spec.edge_owner_file_id_col),
        col(spec.edge_owner_file_id_fallback),
    )

    select_exprs = [
        entity_id.alias(spec.entity_id_alias),
        symbol.alias("symbol"),
    ]

    if spec.symbol_roles_col is not None:
        select_exprs.append(col(spec.symbol_roles_col).alias("symbol_roles"))

    select_exprs.extend(
        [
            col(spec.path_col).alias("path"),
            edge_owner.alias("edge_owner_file_id"),
            bstart.alias("bstart"),
            bend.alias("bend"),
            lit(spec.resolution_method).alias("resolution_method"),
            lit(spec.confidence).alias("confidence"),
            lit(spec.score).alias("score"),
            lit(task_name).alias("task_name"),
            lit(task_priority).alias("task_priority"),
        ]
    )

    return source.select(*select_exprs)


def build_qname_relation_df(
    ctx: SessionContext,
    spec: QNameRelationshipSpec,
    *,
    task_name: str,
    task_priority: int,
) -> DataFrame:
    """Build a DataFusion DataFrame for a qname relationship from spec.

    Parameters
    ----------
    ctx
        DataFusion session context for table access.
    spec
        QName relationship specification defining the extraction parameters.
    task_name
        Task name for provenance metadata.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    DataFrame
        DataFusion DataFrame for the qname relationship.
    """
    source = ctx.table(spec.src_table)

    entity_id = _column_expr(spec.entity_id_col)
    bstart = _column_expr(spec.bstart_col)
    bend = _column_expr(spec.bend_col)

    qname = col(spec.qname_col)
    null_utf8 = f.arrow_cast(lit(None), lit("Utf8"))
    qname_id = f.when(qname.is_not_null(), stable_id_parts("qname", qname)).otherwise(null_utf8)

    return source.select(
        entity_id.alias(spec.entity_id_alias),
        qname_id.alias("qname_id"),
        col(spec.qname_source_col).alias("qname_source"),
        col(spec.path_col).alias("path"),
        col(spec.edge_owner_file_id_col).alias("edge_owner_file_id"),
        bstart.alias("call_bstart"),
        bend.alias("call_bend"),
        lit(spec.confidence).alias("confidence"),
        lit(spec.score).alias("score"),
        col(spec.ambiguity_group_col).alias("ambiguity_group_id"),
        lit(task_name).alias("task_name"),
        lit(task_priority).alias("task_priority"),
    )


def build_relation_df_from_spec(
    ctx: SessionContext,
    spec: RelationshipSpec | QNameRelationshipSpec,
    *,
    task_name: str,
    task_priority: int,
) -> DataFrame:
    """Build a DataFusion DataFrame for any relationship spec.

    Parameters
    ----------
    ctx
        DataFusion session context for table access.
    spec
        Relationship specification (symbol or qname).
    task_name
        Task name for provenance metadata.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    DataFrame
        DataFusion DataFrame for the relationship.

    Raises
    ------
    TypeError
        Raised when an unknown spec type is provided.
    """
    if isinstance(spec, RelationshipSpec):
        return build_symbol_relation_df(
            ctx,
            spec,
            task_name=task_name,
            task_priority=task_priority,
        )
    if isinstance(spec, QNameRelationshipSpec):
        return build_qname_relation_df(
            ctx,
            spec,
            task_name=task_name,
            task_priority=task_priority,
        )
    msg = f"Unknown relationship spec type: {type(spec)!r}"
    raise TypeError(msg)


def build_all_symbol_relations_dfs(
    ctx: SessionContext,
    *,
    task_priority: int,
) -> dict[str, DataFrame]:
    """Build DataFrames for all symbol relationship specs.

    Parameters
    ----------
    ctx
        DataFusion session context for table access.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    dict[str, DataFrame]
        Mapping of output view names to DataFrames.
    """
    results: dict[str, DataFrame] = {}
    for spec in SYMBOL_RELATIONSHIP_SPECS:
        task_name = f"rel.{spec.name}"
        df = build_symbol_relation_df(
            ctx,
            spec,
            task_name=task_name,
            task_priority=task_priority,
        )
        results[spec.output_view_name] = df
    return results


def build_all_qname_relations_dfs(
    ctx: SessionContext,
    *,
    task_priority: int,
) -> dict[str, DataFrame]:
    """Build DataFrames for all qname relationship specs.

    Parameters
    ----------
    ctx
        DataFusion session context for table access.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    dict[str, DataFrame]
        Mapping of output view names to DataFrames.
    """
    results: dict[str, DataFrame] = {}
    for spec in QNAME_RELATIONSHIP_SPECS:
        task_name = f"rel.{spec.name}"
        df = build_qname_relation_df(
            ctx,
            spec,
            task_name=task_name,
            task_priority=task_priority,
        )
        results[spec.output_view_name] = df
    return results


__all__ = [
    "build_all_qname_relations_dfs",
    "build_all_symbol_relations_dfs",
    "build_qname_relation_df",
    "build_relation_df_from_spec",
    "build_symbol_relation_df",
]
