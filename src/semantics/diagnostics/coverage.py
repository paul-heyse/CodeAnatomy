"""Coverage diagnostics builders for semantic extraction evidence."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion import col, lit
from datafusion import functions as f

from datafusion_engine.schema.introspection_core import table_names_snapshot
from obs.otel import SCOPE_SEMANTICS, stage_span

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame


MIN_EXTRACTION_COUNT: int = 1


def build_file_coverage_report(
    ctx: SessionContext,
    *,
    base_table: str = "repo_files_v1",
) -> DataFrame | None:
    """Build extraction coverage report per file.

    Returns:
        DataFrame | None: Coverage report rows, or ``None`` when no file index is available.
    """
    with stage_span(
        "semantics.build_file_coverage_report",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
        attributes={"codeanatomy.base_table": base_table},
    ):
        available = table_names_snapshot(ctx)
        if base_table not in available:
            for fallback in ("file_index", "repo_files"):
                if fallback in available:
                    base_table = fallback
                    break
            else:
                return None

        base = ctx.table(base_table).select(col("file_id"))

        if "cst_defs_norm" in available:
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

        if "tree_sitter_files_v1" in available:
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

        if "scip_documents" in available:
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


__all__ = ["MIN_EXTRACTION_COUNT", "build_file_coverage_report"]
