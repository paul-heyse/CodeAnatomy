"""Issue batching utilities for semantic diagnostics emission."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from datafusion import col, lit
from datafusion import functions as f

from obs.metrics import quality_issue_rows
from obs.otel import SCOPE_SEMANTICS, stage_span
from semantics.diagnostics.builder_base import DiagnosticBatchBuilder
from semantics.diagnostics.coverage import MIN_EXTRACTION_COUNT
from semantics.diagnostics.quality_metrics import build_relationship_quality_metrics

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame


FILE_QUALITY_SCORE_THRESHOLD: float = 800.0
DEFAULT_MAX_ISSUE_ROWS: int = 200


@dataclass(frozen=True)
class SemanticIssueBatch:
    """Batch of issue rows for diagnostics emission."""

    issue_kind: str
    rows: tuple[dict[str, object], ...]


def dataframe_row_count(df: DataFrame) -> int:
    """Return a row count for a DataFusion DataFrame."""
    count_df = df.aggregate([], [f.count(lit(1)).alias("row_count")])
    table = count_df.to_arrow_table()
    if table.num_rows == 0:
        return 0
    rows = cast("list[dict[str, object]]", table.to_pylist())
    value = rows[0].get("row_count")
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        return int(value)
    return 0


def semantic_quality_issue_batches(
    *,
    view_name: str,
    df: DataFrame,
    max_rows: int = DEFAULT_MAX_ISSUE_ROWS,
) -> Sequence[SemanticIssueBatch]:
    """Return issue batches for a diagnostic view."""
    if view_name == "file_quality":
        issue_kind = f"file_quality_score_below_{int(FILE_QUALITY_SCORE_THRESHOLD)}"
        issue_df = df.filter(col("file_quality_score") < lit(FILE_QUALITY_SCORE_THRESHOLD)).select(
            col("file_id").alias("entity_id"),
            lit(issue_kind).alias("issue"),
        )
        return _issue_batches(
            issue_kind=issue_kind,
            entity_kind="file",
            source_table=view_name,
            df=issue_df,
            max_rows=max_rows,
        )
    if view_name == "file_coverage_report":
        issue_kind = "missing_extraction_sources"
        issue_df = df.filter(col("extraction_count") < lit(MIN_EXTRACTION_COUNT)).select(
            col("file_id").alias("entity_id"),
            lit(issue_kind).alias("issue"),
        )
        return _issue_batches(
            issue_kind=issue_kind,
            entity_kind="file",
            source_table=view_name,
            df=issue_df,
            max_rows=max_rows,
        )
    if view_name == "relationship_quality_metrics":
        issue_kind = "low_confidence_edges"
        issue_df = df.filter(col("low_confidence_edges") > lit(0)).select(
            col("relationship_name").alias("entity_id"),
            lit(issue_kind).alias("issue"),
        )
        return _issue_batches(
            issue_kind=issue_kind,
            entity_kind="relationship",
            source_table=view_name,
            df=issue_df,
            max_rows=max_rows,
        )
    if view_name == "relationship_ambiguity_report":
        issue_kind = "ambiguous_sources"
        issue_df = df.filter(col("ambiguous_sources") > lit(0)).select(
            col("relationship_name").alias("entity_id"),
            lit(issue_kind).alias("issue"),
        )
        return _issue_batches(
            issue_kind=issue_kind,
            entity_kind="relationship",
            source_table=view_name,
            df=issue_df,
            max_rows=max_rows,
        )
    return ()


def _issue_batches(
    *,
    issue_kind: str,
    entity_kind: str,
    source_table: str,
    df: DataFrame,
    max_rows: int,
) -> list[SemanticIssueBatch]:
    rows = _issue_rows_from_df(df, max_rows=max_rows)
    normalized = quality_issue_rows(
        entity_kind=entity_kind,
        rows=rows,
        source_table=source_table,
    )
    if not normalized:
        return []
    return [
        SemanticIssueBatch(
            issue_kind=issue_kind,
            rows=tuple(normalized),
        )
    ]


def _issue_rows_from_df(
    df: DataFrame,
    *,
    max_rows: int,
) -> list[dict[str, object]]:
    limited = df.limit(max_rows)
    table = limited.to_arrow_table()
    builder = DiagnosticBatchBuilder()
    builder.add_many(cast("list[dict[str, object]]", table.to_pylist()))
    return builder.rows()


def build_quality_summary(
    ctx: SessionContext,
    relationship_names: list[str],
) -> DataFrame | None:
    """Build aggregate quality summary across multiple relationships.

    Returns:
        DataFrame | None: Unioned relationship quality metrics, or ``None`` when empty.
    """
    with stage_span(
        "semantics.build_quality_summary",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
        attributes={"codeanatomy.relationship_count": len(relationship_names)},
    ):
        dfs: list[DataFrame] = []
        for name in relationship_names:
            metrics = build_relationship_quality_metrics(ctx, name)
            if metrics is not None:
                dfs.append(metrics)

        if not dfs:
            return None

        result = dfs[0]
        for df in dfs[1:]:
            result = result.union(df)

        return result


__all__ = [
    "DEFAULT_MAX_ISSUE_ROWS",
    "FILE_QUALITY_SCORE_THRESHOLD",
    "SemanticIssueBatch",
    "build_quality_summary",
    "dataframe_row_count",
    "semantic_quality_issue_batches",
]
