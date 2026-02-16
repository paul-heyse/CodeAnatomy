"""Relationship-quality metric and diagnostics builders."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f

from datafusion_engine.schema.introspection_core import table_names_snapshot
from obs.otel import SCOPE_SEMANTICS, stage_span
from semantics.diagnostics._utils import empty_diagnostic_frame

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame
    from datafusion.expr import Expr


def _optional_col(df: DataFrame, name: str, dtype: pa.DataType) -> Expr:
    names = set(df.schema().names)
    if name in names:
        return col(name).cast(dtype)
    return lit(None).cast(dtype)


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


def build_relationship_quality_metrics(
    ctx: SessionContext,
    relationship_name: str,
    *,
    source_column: str = "entity_id",
    target_column: str = "symbol",
) -> DataFrame | None:
    """Build aggregate quality metrics for a compiled relationship.

    Returns:
        DataFrame | None: Aggregate metric row for the relationship, or ``None`` if absent.
    """
    with stage_span(
        "semantics.build_relationship_quality_metrics",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
        attributes={"codeanatomy.relationship": relationship_name},
    ):
        available = table_names_snapshot(ctx)
        if relationship_name not in available:
            return None

        rel_df = ctx.table(relationship_name)
        schema_names: list[str] = (
            list(rel_df.schema().names) if hasattr(rel_df.schema(), "names") else []
        )

        has_confidence = "confidence" in schema_names
        has_score = "score" in schema_names
        has_src = source_column in schema_names
        has_dst = target_column in schema_names

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


def build_relationship_candidates_view(ctx: SessionContext) -> DataFrame:
    """Build relationship candidate diagnostics view.

    Returns:
        DataFrame: Candidate diagnostics rows for all available relationships.
    """
    with stage_span(
        "semantics.build_relationship_candidates_view",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
    ):
        available = table_names_snapshot(ctx)
        frames: list[DataFrame] = []
        from semantics.registry import RELATIONSHIP_SPECS

        for spec in RELATIONSHIP_SPECS:
            rel_name = spec.name
            if rel_name not in available:
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
            return empty_diagnostic_frame(ctx, pa.schema(_RELATIONSHIP_DIAG_SCHEMA))
        result = frames[0]
        for frame in frames[1:]:
            result = result.union(frame)
        return result


__all__ = [
    "build_relationship_candidates_view",
    "build_relationship_quality_metrics",
]
