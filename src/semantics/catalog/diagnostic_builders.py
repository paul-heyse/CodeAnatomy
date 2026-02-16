"""Diagnostic and quality analysis builders."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from datafusion import SessionContext, col, lit

from datafusion_engine.hashing import DIAG_ID_SPEC
from datafusion_engine.sql.guard import safe_sql
from semantics.catalog.builder_exprs import (
    _coalesce_cols,
    _null_expr,
    _span_expr,
    _stable_id_expr,
)

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame


logger = logging.getLogger(__name__)


def diagnostics_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for normalized diagnostics.

    Returns:
        DataFrame: Canonical diagnostics rows from tree-sitter error nodes.
    """
    try:
        ts_errors = ctx.table("ts_errors")

        bstart = _coalesce_cols(ts_errors, "bstart", "start_byte", default_expr=_null_expr("Int64"))
        bend = _coalesce_cols(ts_errors, "bend", "end_byte", default_expr=_null_expr("Int64"))

        span = _span_expr(bstart=bstart, bend=bend)

        df = (
            ts_errors.with_column("span", span)
            .with_column("severity", lit("ERROR"))
            .with_column("message", lit("tree-sitter error node"))
            .with_column("diag_source", lit("treesitter"))
            .with_column("code", _null_expr("Utf8"))
        )

        diag_id = _stable_id_expr(
            DIAG_ID_SPEC.prefix,
            (col("path"), bstart, bend, col("diag_source"), col("message")),
            null_sentinel=DIAG_ID_SPEC.null_sentinel,
        )

        return df.with_column("diag_id", diag_id)
    except (RuntimeError, KeyError, ValueError):
        logger.debug("Tree-sitter diagnostics table unavailable; returning empty diagnostics.")
        return safe_sql(
            ctx,
            """
            SELECT
                CAST(NULL AS Utf8) AS file_id,
                CAST(NULL AS Utf8) AS path,
                CAST(NULL AS Utf8) AS diag_id,
                CAST(NULL AS Utf8) AS severity,
                CAST(NULL AS Utf8) AS message,
                CAST(NULL AS Utf8) AS diag_source,
                CAST(NULL AS Utf8) AS code
            WHERE FALSE
        """,
        )


def span_errors_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for span error rows.

    Returns:
        DataFrame: Existing span-error rows from ``span_errors``.
    """
    return ctx.table("span_errors")


def file_quality_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame for file quality signals.

    Returns:
        DataFrame: Per-file quality scoring diagnostics.
    """
    from semantics.signals import build_file_quality_view

    return build_file_quality_view(ctx)


def relationship_quality_metrics_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame summarizing relationship quality metrics.

    Returns:
        DataFrame: Union of relationship quality metric rows for all relationships.

    Raises:
        ValueError: If no relationship metric rows can be generated.
    """
    from semantics.diagnostics import build_relationship_quality_metrics
    from semantics.registry import relationship_names

    reports: list[DataFrame] = []
    for name in relationship_names():
        metrics = build_relationship_quality_metrics(
            ctx,
            name,
            source_column="entity_id",
            target_column="symbol",
        )
        if metrics is not None:
            reports.append(metrics)
    if not reports:
        msg = "No relationship metrics could be generated for relationship_quality_metrics."
        raise ValueError(msg)
    result = reports[0]
    for report in reports[1:]:
        result = result.union(report)
    return result


def relationship_ambiguity_report_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame summarizing relationship ambiguity metrics.

    Returns:
        DataFrame: Union of relationship ambiguity rows for all relationships.

    Raises:
        ValueError: If no ambiguity report rows can be generated.
    """
    from semantics.diagnostics import build_ambiguity_analysis
    from semantics.registry import relationship_names

    reports: list[DataFrame] = []
    for name in relationship_names():
        report = build_ambiguity_analysis(
            ctx,
            name,
            source_column="entity_id",
        )
        if report is not None:
            reports.append(report)
    if not reports:
        msg = "No relationship ambiguity reports could be generated."
        raise ValueError(msg)
    result = reports[0]
    for report in reports[1:]:
        result = result.union(report)
    return result


def relationship_candidates_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame of relationship candidates for diagnostics.

    Returns:
        DataFrame: Flattened relationship candidate diagnostics.
    """
    from semantics.diagnostics import build_relationship_candidates_view

    return build_relationship_candidates_view(ctx)


def relationship_decisions_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame of relationship decisions for diagnostics.

    Returns:
        DataFrame: Flattened relationship decision diagnostics.
    """
    from semantics.diagnostics import build_relationship_candidates_view

    return build_relationship_candidates_view(ctx)


def schema_anomalies_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame of schema anomaly diagnostics.

    Returns:
        DataFrame: Schema-contract anomaly diagnostics for semantic outputs.
    """
    from semantics.diagnostics import build_schema_anomalies_view

    return build_schema_anomalies_view(ctx)


def file_coverage_report_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame reporting extraction coverage per file.

    Returns:
        DataFrame: Per-file extraction coverage diagnostics.

    Raises:
        ValueError: If required file-index inputs are unavailable.
    """
    from semantics.diagnostics import build_file_coverage_report

    report = build_file_coverage_report(ctx)
    if report is None:
        msg = "file_coverage_report requires repo_files_v1 (or file_index) input."
        raise ValueError(msg)
    return report


def exported_defs_df_builder(ctx: SessionContext) -> DataFrame:
    """Build a DataFrame of exported definitions for incremental analysis.

    Returns:
        DataFrame: Exported definition rows used by incremental graph pruning.
    """
    from semantics.incremental.export_builders import exported_defs_df_builder as _builder

    return _builder(ctx)


__all__ = [
    "diagnostics_df_builder",
    "exported_defs_df_builder",
    "file_coverage_report_df_builder",
    "file_quality_df_builder",
    "relationship_ambiguity_report_df_builder",
    "relationship_candidates_df_builder",
    "relationship_decisions_df_builder",
    "relationship_quality_metrics_df_builder",
    "schema_anomalies_df_builder",
    "span_errors_df_builder",
]
