"""Ambiguity analysis diagnostics for compiled relationships."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion import col, lit
from datafusion import functions as f

from datafusion_engine.schema.introspection import table_names_snapshot
from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame


def build_ambiguity_analysis(
    ctx: SessionContext,
    relationship_name: str,
    *,
    ambiguity_column: str = "ambiguity_group_id",
    source_column: str = "src",
) -> DataFrame | None:
    """Analyze ambiguity patterns in a compiled relationship.

    Returns:
        DataFrame | None: Aggregate ambiguity metrics, or ``None`` when inputs are unavailable.
    """
    with stage_span(
        "semantics.build_ambiguity_analysis",
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

        has_ambiguity = ambiguity_column in schema_names
        has_source = source_column in schema_names

        if not has_source:
            return None

        if not has_ambiguity:
            per_source = rel_df.aggregate(
                [col(source_column)],
                [f.count(lit(1)).alias("match_count")],
            )

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
                            f.when(col("match_count") > lit(1), col("match_count")).otherwise(
                                lit(None)
                            )
                        ).alias("avg_candidates"),
                    ],
                )
                .with_column("relationship_name", lit(relationship_name))
                .with_column(
                    "ambiguity_rate",
                    col("ambiguous_sources").cast(float) / col("total_sources").cast(float),
                )
            )

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


__all__ = ["build_ambiguity_analysis"]
