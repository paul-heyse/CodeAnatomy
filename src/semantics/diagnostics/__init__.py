"""Quality diagnostics and coverage reporting for semantic relationships."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame


SEMANTIC_DIAGNOSTIC_VIEW_NAMES: tuple[str, ...] = (
    "file_quality",
    "relationship_quality_metrics",
    "relationship_ambiguity_report",
    "file_coverage_report",
    "relationship_candidates",
    "relationship_decisions",
    "schema_anomalies",
)

from semantics.diagnostics.ambiguity import build_ambiguity_analysis
from semantics.diagnostics.coverage import MIN_EXTRACTION_COUNT, build_file_coverage_report
from semantics.diagnostics.issue_batching import (
    DEFAULT_MAX_ISSUE_ROWS,
    FILE_QUALITY_SCORE_THRESHOLD,
    SemanticIssueBatch,
    build_quality_summary,
    dataframe_row_count,
    semantic_quality_issue_batches,
)
from semantics.diagnostics.quality_metrics import (
    build_relationship_candidates_view,
    build_relationship_quality_metrics,
)
from semantics.diagnostics.schema_anomalies import build_schema_anomalies_view


def semantic_diagnostic_view_builders() -> dict[str, Callable[[SessionContext], DataFrame]]:
    """Return DataFrame builders for semantic diagnostic views."""
    from semantics.catalog.analysis_builders import (
        file_coverage_report_df_builder,
        file_quality_df_builder,
        relationship_ambiguity_report_df_builder,
        relationship_decisions_df_builder,
        relationship_quality_metrics_df_builder,
    )

    return {
        "file_quality": file_quality_df_builder,
        "relationship_quality_metrics": relationship_quality_metrics_df_builder,
        "relationship_ambiguity_report": relationship_ambiguity_report_df_builder,
        "file_coverage_report": file_coverage_report_df_builder,
        "relationship_candidates": build_relationship_candidates_view,
        "relationship_decisions": relationship_decisions_df_builder,
        "schema_anomalies": build_schema_anomalies_view,
    }


__all__ = [
    "DEFAULT_MAX_ISSUE_ROWS",
    "FILE_QUALITY_SCORE_THRESHOLD",
    "MIN_EXTRACTION_COUNT",
    "SEMANTIC_DIAGNOSTIC_VIEW_NAMES",
    "SemanticIssueBatch",
    "build_ambiguity_analysis",
    "build_file_coverage_report",
    "build_quality_summary",
    "build_relationship_candidates_view",
    "build_relationship_quality_metrics",
    "build_schema_anomalies_view",
    "dataframe_row_count",
    "semantic_diagnostic_view_builders",
    "semantic_quality_issue_batches",
]
