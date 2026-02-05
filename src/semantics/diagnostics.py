"""Quality diagnostics and coverage reporting for semantic relationships.

This module provides functions to compute quality metrics, coverage reports,
and ambiguity analysis for quality-aware relationship compilation.

Usage
-----
>>> from semantics.diagnostics import build_relationship_quality_metrics
>>> from datafusion import SessionContext

>>> ctx = SessionContext()
>>> # ... register relationship tables ...
>>> metrics_df = build_relationship_quality_metrics(ctx, "rel_docstring_owner")
>>> print(metrics_df.to_polars())
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f

from datafusion_engine.arrow.interop import empty_table_for_schema
from obs.metrics import quality_issue_rows

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame
    from datafusion.expr import Expr


def _table_exists(ctx: SessionContext, name: str) -> bool:
    """Check if a table exists in the session context.

    Returns
    -------
    bool
        True if table exists, False otherwise.
    """
    try:
        ctx.table(name)
    except (KeyError, OSError, RuntimeError, TypeError, ValueError):
        return False
    return True


def _empty_table(
    ctx: SessionContext, schema_fields: Sequence[tuple[str, pa.DataType]]
) -> DataFrame:
    schema = pa.schema(schema_fields)
    table = empty_table_for_schema(schema)
    return ctx.from_arrow(table)


def _optional_col(df: DataFrame, name: str, dtype: pa.DataType) -> Expr:
    names = set(df.schema().names)
    if name in names:
        return col(name).cast(dtype)
    return lit(None).cast(dtype)


def _relationship_diag_frame(
    df: DataFrame,
    *,
    relationship_name: str,
    entity_id_col: str,
) -> DataFrame:
    base = [
        lit(relationship_name).alias("relationship_name"),
        _optional_col(df, entity_id_col, pa.string()).alias("src"),
        _optional_col(df, "symbol", pa.string()).alias("dst"),
        _optional_col(df, "path", pa.string()).alias("path"),
        _optional_col(df, "bstart", pa.int64()).alias("bstart"),
        _optional_col(df, "bend", pa.int64()).alias("bend"),
        _optional_col(df, "confidence", pa.float64()).alias("confidence"),
        _optional_col(df, "score", pa.float64()).alias("score"),
        _optional_col(df, "ambiguity_group_id", pa.string()).alias("ambiguity_group_id"),
        _optional_col(df, "task_name", pa.string()).alias("task_name"),
        _optional_col(df, "task_priority", pa.int32()).alias("task_priority"),
        _optional_col(df, "edge_kind", pa.string()).alias("edge_kind"),
    ]
    extras: list[Expr] = []
    for name, dtype in _RELATIONSHIP_DIAG_SCHEMA:
        if not name.startswith(("feat_", "hard_")):
            continue
        extras.append(_optional_col(df, name, dtype).alias(name))
    return df.select(*base, *extras)


SEMANTIC_DIAGNOSTIC_VIEW_NAMES: tuple[str, ...] = (
    "file_quality",
    "relationship_quality_metrics",
    "relationship_ambiguity_report",
    "file_coverage_report",
    "relationship_candidates",
    "relationship_decisions",
    "schema_anomalies",
)

FILE_QUALITY_SCORE_THRESHOLD: float = 800.0
MIN_EXTRACTION_COUNT: int = 1
DEFAULT_MAX_ISSUE_ROWS: int = 200


def _relationship_diag_schema() -> tuple[tuple[str, pa.DataType], ...]:
    from semantics.registry import RELATIONSHIP_SPECS

    base = [
        ("relationship_name", pa.string()),
        ("src", pa.string()),
        ("dst", pa.string()),
        ("path", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
        ("confidence", pa.float64()),
        ("score", pa.float64()),
        ("ambiguity_group_id", pa.string()),
        ("task_name", pa.string()),
        ("task_priority", pa.int32()),
        ("edge_kind", pa.string()),
    ]
    max_hard = max((len(spec.signals.hard) for spec in RELATIONSHIP_SPECS), default=0)
    hard_fields = [(f"hard_{index}", pa.bool_()) for index in range(1, max_hard + 1)]
    feature_names = sorted(
        {feature.name for spec in RELATIONSHIP_SPECS for feature in spec.signals.features}
    )
    extra = [(f"feat_{name}", pa.float64()) for name in feature_names]
    return tuple(base + hard_fields + extra)


_RELATIONSHIP_DIAG_SCHEMA: tuple[tuple[str, pa.DataType], ...] = _relationship_diag_schema()

_SCHEMA_ANOMALY_SCHEMA: tuple[tuple[str, pa.DataType], ...] = (
    ("view_name", pa.string()),
    ("violation_type", pa.string()),
    ("column_name", pa.string()),
    ("detail", pa.string()),
)


@dataclass(frozen=True)
class SemanticIssueBatch:
    """Batch of issue rows for diagnostics emission."""

    issue_kind: str
    rows: tuple[dict[str, object], ...]


def build_relationship_quality_metrics(
    ctx: SessionContext,
    relationship_name: str,
    *,
    source_column: str = "entity_id",
    target_column: str = "symbol",
) -> DataFrame | None:
    """Build aggregate quality metrics for a compiled relationship.

    Computes summary statistics including edge counts, confidence
    distribution, score ranges, and ambiguity metrics.

    Parameters
    ----------
    ctx
        DataFusion session context with relationship table registered.
    relationship_name
        Name of the relationship table to analyze.
    source_column
        Column name containing source entity identifiers.
    target_column
        Column name containing target entity identifiers.

    Returns
    -------
    DataFrame | None
        Quality metrics DataFrame with columns:
        - relationship_name: Name of the relationship
        - total_edges: Total number of edges
        - distinct_sources: Number of unique source entities
        - distinct_targets: Number of unique target entities
        - avg_confidence: Mean confidence score
        - min_confidence: Minimum confidence
        - max_confidence: Maximum confidence
        - avg_score: Mean relationship score
        - min_score: Minimum score
        - max_score: Maximum score
        - low_confidence_edges: Edges with confidence < 0.5

        Returns None if table doesn't exist.

    Notes
    -----
    Requires the relationship table to have `confidence` and `score` columns.
    If these columns don't exist, appropriate defaults are used.
    """
    if not _table_exists(ctx, relationship_name):
        return None

    rel_df = ctx.table(relationship_name)
    schema_names: list[str] = (
        list(rel_df.schema().names) if hasattr(rel_df.schema(), "names") else []
    )

    # Check for required columns
    has_confidence = "confidence" in schema_names
    has_score = "score" in schema_names
    has_src = source_column in schema_names
    has_dst = target_column in schema_names

    # Build aggregation expressions
    agg_exprs = [f.count(lit(1)).alias("total_edges")]

    if has_src:
        agg_exprs.append(f.count(col(source_column), distinct=True).alias("distinct_sources"))
    else:
        agg_exprs.append(lit(0).alias("distinct_sources"))

    if has_dst:
        agg_exprs.append(f.count(col(target_column), distinct=True).alias("distinct_targets"))
    else:
        agg_exprs.append(lit(0).alias("distinct_targets"))

    if has_confidence:
        agg_exprs.extend(
            [
                f.avg(col("confidence")).alias("avg_confidence"),
                f.min(col("confidence")).alias("min_confidence"),
                f.max(col("confidence")).alias("max_confidence"),
                f.sum(f.when(col("confidence") < lit(0.5), lit(1)).otherwise(lit(0))).alias(
                    "low_confidence_edges"
                ),
            ]
        )
    else:
        agg_exprs.extend(
            [
                lit(None).cast("float64").alias("avg_confidence"),
                lit(None).cast("float64").alias("min_confidence"),
                lit(None).cast("float64").alias("max_confidence"),
                lit(0).alias("low_confidence_edges"),
            ]
        )

    if has_score:
        agg_exprs.extend(
            [
                f.avg(col("score")).alias("avg_score"),
                f.min(col("score")).alias("min_score"),
                f.max(col("score")).alias("max_score"),
            ]
        )
    else:
        agg_exprs.extend(
            [
                lit(None).cast("float64").alias("avg_score"),
                lit(None).cast("float64").alias("min_score"),
                lit(None).cast("float64").alias("max_score"),
            ]
        )

    return rel_df.aggregate([], agg_exprs).with_column(
        "relationship_name",
        lit(relationship_name),
    )


def build_file_coverage_report(
    ctx: SessionContext,
    *,
    base_table: str = "repo_files_v1",
) -> DataFrame | None:
    """Build extraction coverage report per file.

    Summarizes which extraction sources have data for each file,
    useful for identifying gaps in extraction coverage.

    Parameters
    ----------
    ctx
        DataFusion session context with extraction tables registered.
    base_table
        Name of the base file table (default: "repo_files_v1").

    Returns
    -------
    DataFrame | None
        Coverage report DataFrame with columns:
        - file_id: File identifier
        - has_cst: 1 if CST data exists, 0 otherwise
        - has_tree_sitter: 1 if tree-sitter data exists, 0 otherwise
        - has_scip: 1 if SCIP data exists, 0 otherwise
        - extraction_count: Number of extraction sources with data

        Returns None if base_table doesn't exist.
    """
    if not _table_exists(ctx, base_table):
        for fallback in ("file_index", "repo_files"):
            if _table_exists(ctx, fallback):
                base_table = fallback
                break
        else:
            return None

    base = ctx.table(base_table).select(col("file_id"))

    # CST coverage
    if _table_exists(ctx, "cst_defs_norm"):
        cst_files = (
            ctx.table("cst_defs_norm")
            .select(col("file_id").alias("cst_file_id"))
            .distinct()
            .with_column("has_cst", lit(1))
        )
        base = base.join(
            cst_files,
            left_on=["file_id"],
            right_on=["cst_file_id"],
            how="left",
        )
    else:
        base = base.with_column("has_cst", lit(0))

    # Tree-sitter coverage
    if _table_exists(ctx, "tree_sitter_files_v1"):
        ts_files = (
            ctx.table("tree_sitter_files_v1")
            .select(col("file_id").alias("ts_file_id"))
            .distinct()
            .with_column("has_tree_sitter", lit(1))
        )
        base = base.join(
            ts_files,
            left_on=["file_id"],
            right_on=["ts_file_id"],
            how="left",
        )
    else:
        base = base.with_column("has_tree_sitter", lit(0))

    # SCIP coverage
    if _table_exists(ctx, "scip_documents"):
        scip_files = (
            ctx.table("scip_documents")
            .select(col("document_id").alias("scip_file_id"))
            .distinct()
            .with_column("has_scip", lit(1))
        )
        base = base.join(
            scip_files,
            left_on=["file_id"],
            right_on=["scip_file_id"],
            how="left",
        )
    else:
        base = base.with_column("has_scip", lit(0))

    # Coalesce and compute extraction count
    return base.select(
        col("file_id"),
        f.coalesce(col("has_cst"), lit(0)).alias("has_cst"),
        f.coalesce(col("has_tree_sitter"), lit(0)).alias("has_tree_sitter"),
        f.coalesce(col("has_scip"), lit(0)).alias("has_scip"),
        (
            f.coalesce(col("has_cst"), lit(0))
            + f.coalesce(col("has_tree_sitter"), lit(0))
            + f.coalesce(col("has_scip"), lit(0))
        ).alias("extraction_count"),
    )


def build_ambiguity_analysis(
    ctx: SessionContext,
    relationship_name: str,
    *,
    ambiguity_column: str = "ambiguity_group_id",
    source_column: str = "src",
) -> DataFrame | None:
    """Analyze ambiguity patterns in a compiled relationship.

    Identifies sources with multiple candidate matches and computes
    ambiguity statistics for quality assessment.

    Parameters
    ----------
    ctx
        DataFusion session context with relationship table registered.
    relationship_name
        Name of the relationship table to analyze.
    ambiguity_column
        Column containing ambiguity group identifiers.
    source_column
        Column containing source entity identifiers.

    Returns
    -------
    DataFrame | None
        Ambiguity analysis DataFrame with columns:
        - relationship_name: Name of the relationship
        - total_sources: Total unique source entities
        - ambiguous_sources: Sources with multiple matches
        - ambiguity_rate: Fraction of sources with ambiguity
        - max_candidates: Maximum candidates per source
        - avg_candidates: Average candidates per ambiguous source

        Returns None if table doesn't exist.
    """
    if not _table_exists(ctx, relationship_name):
        return None

    rel_df = ctx.table(relationship_name)
    schema_names: list[str] = (
        list(rel_df.schema().names) if hasattr(rel_df.schema(), "names") else []
    )

    # Check for required columns
    has_ambiguity = ambiguity_column in schema_names
    has_source = source_column in schema_names

    if not has_source:
        # Cannot analyze without source column
        return None

    if not has_ambiguity:
        # Without ambiguity column, count matches per source
        per_source = rel_df.aggregate(
            [col(source_column)],
            [f.count(lit(1)).alias("match_count")],
        )

        # Compute all metrics in a single aggregation to avoid cross join
        return (
            per_source.aggregate(
                [],
                [
                    f.count(lit(1)).alias("total_sources"),
                    f.sum(f.when(col("match_count") > lit(1), lit(1)).otherwise(lit(0))).alias(
                        "ambiguous_sources"
                    ),
                    f.max(col("match_count")).alias("max_candidates"),
                    f.avg(
                        f.when(col("match_count") > lit(1), col("match_count")).otherwise(lit(None))
                    ).alias("avg_candidates"),
                ],
            )
            .with_column("relationship_name", lit(relationship_name))
            .with_column(
                "ambiguity_rate",
                col("ambiguous_sources").cast(float) / col("total_sources").cast(float),
            )
        )

    # With ambiguity column, use it for grouping
    per_group = rel_df.aggregate(
        [col(ambiguity_column)],
        [
            f.count(lit(1)).alias("match_count"),
            f.first_value(col(source_column)).alias("first_source"),
        ],
    )

    return (
        per_group.aggregate(
            [],
            [
                f.count(lit(1)).alias("total_sources"),
                f.sum(f.when(col("match_count") > lit(1), lit(1)).otherwise(lit(0))).alias(
                    "ambiguous_sources"
                ),
                f.max(col("match_count")).alias("max_candidates"),
                f.avg(
                    f.when(col("match_count") > lit(1), col("match_count")).otherwise(lit(None))
                ).alias("avg_candidates"),
            ],
        )
        .with_column("relationship_name", lit(relationship_name))
        .with_column(
            "ambiguity_rate",
            col("ambiguous_sources").cast(float) / col("total_sources").cast(float),
        )
    )


def build_relationship_candidates_view(ctx: SessionContext) -> DataFrame:
    """Build relationship candidate diagnostics view.

    Returns
    -------
    DataFrame
        Relationship candidates view.
    """
    frames: list[DataFrame] = []
    from semantics.registry import RELATIONSHIP_SPECS

    for spec in RELATIONSHIP_SPECS:
        rel_name = spec.name
        if not _table_exists(ctx, rel_name):
            continue
        df = ctx.table(rel_name)
        frames.append(
            _relationship_diag_frame(
                df,
                relationship_name=rel_name,
                entity_id_col="entity_id",
            )
        )
    if not frames:
        return _empty_table(ctx, _RELATIONSHIP_DIAG_SCHEMA)
    result = frames[0]
    for frame in frames[1:]:
        result = result.union(frame)
    return result


def build_relationship_decisions_view(ctx: SessionContext) -> DataFrame:
    """Build relationship decision diagnostics view.

    Returns
    -------
    DataFrame
        Relationship decisions view.
    """
    return build_relationship_candidates_view(ctx)


def build_schema_anomalies_view(ctx: SessionContext) -> DataFrame:
    """Build schema anomalies diagnostics view.

    Returns
    -------
    DataFrame
        Schema anomaly view with contract violations.
    """
    from datafusion_engine.schema.catalog_contracts import contract_violations_for_schema
    from datafusion_engine.schema.contracts import schema_contract_from_dataset_spec
    from datafusion_engine.views.bundle_extraction import arrow_schema_from_df
    from semantics.catalog.dataset_specs import dataset_specs

    rows: list[dict[str, object]] = []
    for spec in dataset_specs():
        from schema_spec.dataset_spec_ops import dataset_spec_name

        name = dataset_spec_name(spec)
        if not _table_exists(ctx, name):
            continue
        df = ctx.table(name)
        schema = arrow_schema_from_df(df)
        contract = schema_contract_from_dataset_spec(name=name, spec=spec)
        violations = contract_violations_for_schema(contract=contract, schema=schema)
        rows.extend(
            [
                {
                    "view_name": name,
                    "violation_type": violation.violation_type.value,
                    "column_name": violation.column_name,
                    "detail": str(violation),
                }
                for violation in violations
            ]
        )
    if not rows:
        return _empty_table(ctx, _SCHEMA_ANOMALY_SCHEMA)
    table = pa.Table.from_pylist(rows, schema=pa.schema(_SCHEMA_ANOMALY_SCHEMA))
    return ctx.from_arrow(table)


def semantic_diagnostic_view_builders() -> dict[str, Callable[[SessionContext], DataFrame]]:
    """Return DataFrame builders for semantic diagnostic views.

    Returns
    -------
    dict[str, Callable[[SessionContext], DataFrame]]
        Mapping of diagnostic view names to builder callables.
    """
    from semantics.catalog.analysis_builders import (
        file_coverage_report_df_builder,
        file_quality_df_builder,
        relationship_ambiguity_report_df_builder,
        relationship_quality_metrics_df_builder,
    )

    return {
        "file_quality": file_quality_df_builder,
        "relationship_quality_metrics": relationship_quality_metrics_df_builder,
        "relationship_ambiguity_report": relationship_ambiguity_report_df_builder,
        "file_coverage_report": file_coverage_report_df_builder,
        "relationship_candidates": build_relationship_candidates_view,
        "relationship_decisions": build_relationship_decisions_view,
        "schema_anomalies": build_schema_anomalies_view,
    }


def dataframe_row_count(df: DataFrame) -> int:
    """Return a row count for a DataFusion DataFrame.

    Returns
    -------
    int
        Row count for the DataFrame.
    """
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
    """Return issue batches for a diagnostic view.

    Returns
    -------
    Sequence[SemanticIssueBatch]
        Issue batches for diagnostics emission.
    """
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
    return cast("list[dict[str, object]]", table.to_pylist())


def build_quality_summary(
    ctx: SessionContext,
    relationship_names: list[str],
) -> DataFrame | None:
    """Build aggregate quality summary across multiple relationships.

    Combines metrics from multiple relationships into a single summary.

    Parameters
    ----------
    ctx
        DataFusion session context with relationship tables registered.
    relationship_names
        List of relationship table names to analyze.

    Returns
    -------
    DataFrame | None
        Combined quality summary DataFrame, or None if no tables exist.
    """
    dfs: list[DataFrame] = []
    for name in relationship_names:
        metrics = build_relationship_quality_metrics(ctx, name)
        if metrics is not None:
            dfs.append(metrics)

    if not dfs:
        return None

    # Union all metrics
    result = dfs[0]
    for df in dfs[1:]:
        result = result.union(df)

    return result


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
    "build_relationship_decisions_view",
    "build_relationship_quality_metrics",
    "build_schema_anomalies_view",
    "dataframe_row_count",
    "semantic_diagnostic_view_builders",
    "semantic_quality_issue_batches",
]
