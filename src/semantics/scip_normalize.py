"""Normalize SCIP occurrences to byte spans using a line index."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f

from datafusion_engine.arrow.interop import empty_table_for_schema
from datafusion_engine.schema.introspection import table_names_snapshot
from datafusion_engine.udf.expr import udf_expr
from datafusion_engine.udf.runtime import rust_udf_snapshot, validate_required_udfs
from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext
    from datafusion.expr import Expr


LOGGER = logging.getLogger(__name__)


def _empty_scip_occurrences_norm(ctx: SessionContext) -> DataFrame:
    from semantics.catalog.dataset_specs import dataset_schema

    schema = dataset_schema("scip_occurrences_norm_v1")
    table = empty_table_for_schema(cast("pa.Schema", schema))
    return ctx.from_arrow(table)


def scip_to_byte_offsets(
    ctx: SessionContext,
    *,
    occurrences_table: str = "scip_occurrences",
    line_index_table: str = "file_line_index_v1",
) -> DataFrame:
    """Return SCIP occurrences with byte-span columns added.

    Parameters
    ----------
    ctx
        DataFusion session context with SCIP and line index tables registered.
    occurrences_table
        Table/view name for SCIP occurrences.
    line_index_table
        Table/view name for line index rows (line_start_byte + line_text).

    Returns
    -------
    DataFrame
        SCIP occurrences with ``bstart``/``bend`` columns.

    Notes
    -----
    Missing input tables or required UDFs return an empty normalized table
    so downstream builds can continue while diagnostics capture the gaps.
    """
    with stage_span(
        "semantics.scip_normalize",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
        attributes={
            "codeanatomy.occurrences_table": occurrences_table,
            "codeanatomy.line_index_table": line_index_table,
        },
    ):
        table_names = table_names_snapshot(ctx)
        if occurrences_table not in table_names:
            LOGGER.warning(
                "Missing SCIP occurrences table %r; returning empty normalized table.",
                occurrences_table,
            )
            return _empty_scip_occurrences_norm(ctx)
        if line_index_table not in table_names:
            LOGGER.warning(
                "Missing line index table %r; returning empty normalized table.",
                line_index_table,
            )
            return _empty_scip_occurrences_norm(ctx)

        snapshot = rust_udf_snapshot(ctx)
        try:
            validate_required_udfs(snapshot, required=("col_to_byte",))
        except ValueError as exc:
            LOGGER.warning(
                "Missing required UDFs for SCIP normalization; returning empty table. %s",
                exc,
            )
            return _empty_scip_occurrences_norm(ctx)

        scip = ctx.table(occurrences_table)
        scip_names = set(scip.schema().names)
        bool_columns = (
            "is_definition",
            "is_import",
            "is_read",
            "is_write",
            "is_generated",
            "is_test",
            "is_forward_definition",
        )
        for col_name in bool_columns:
            if col_name in scip_names:
                scip = scip.with_column(col_name, col(col_name).cast(pa.bool_()))
        scip = scip.with_column("start_line_no", col("start_line") - col("line_base"))
        scip = scip.with_column("end_line_no", col("end_line") - col("line_base"))

        line_index = ctx.table(line_index_table)
        start_idx = line_index.select(
            col("file_id").alias("start_file_id"),
            col("path").alias("start_path"),
            col("line_no").alias("start_line_no_idx"),
            col("line_start_byte").alias("start_line_start_byte"),
            col("line_text").alias("start_line_text"),
        )
        end_idx = line_index.select(
            col("file_id").alias("end_file_id"),
            col("path").alias("end_path"),
            col("line_no").alias("end_line_no_idx"),
            col("line_start_byte").alias("end_line_start_byte"),
            col("line_text").alias("end_line_text"),
        )

        joined = scip.join(
            start_idx,
            join_keys=(
                ["path", "start_line_no"],
                ["start_path", "start_line_no_idx"],
            ),
            how="left",
            coalesce_duplicate_keys=True,
        )
        joined = joined.join(
            end_idx,
            join_keys=(
                ["path", "end_line_no"],
                ["end_path", "end_line_no_idx"],
            ),
            how="left",
            coalesce_duplicate_keys=True,
        )
        joined = joined.with_column(
            "file_id",
            f.coalesce(col("start_file_id"), col("end_file_id")),
        )

        def _byte_offset(line_start: str, line_text: str, col_name: str) -> Expr:
            base = col(line_start).cast(pa.int64())
            char_col = col(col_name).cast(pa.int64())
            offset = udf_expr("col_to_byte", col(line_text), char_col, col("col_unit"))
            guard = (
                col(line_start).is_null()
                | col(line_text).is_null()
                | col(col_name).is_null()
                | col("col_unit").is_null()
            )
            return f.when(guard, lit(None).cast(pa.int64())).otherwise(base + offset)

        df = joined.with_column(
            "bstart",
            _byte_offset("start_line_start_byte", "start_line_text", "start_char"),
        )
        df = df.with_column(
            "bend",
            _byte_offset("end_line_start_byte", "end_line_text", "end_char"),
        )
        df = df.with_column("span", udf_expr("span_make", col("bstart"), col("bend")))

        return df.drop(
            "start_line_no",
            "end_line_no",
            "start_file_id",
            "end_file_id",
            "start_line_no_idx",
            "end_line_no_idx",
            "start_path",
            "end_path",
            "start_line_start_byte",
            "end_line_start_byte",
            "start_line_text",
            "end_line_text",
        )


__all__ = ["scip_to_byte_offsets"]
