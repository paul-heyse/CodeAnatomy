"""Bytecode line table join helpers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame

from datafusion_engine.arrow.interop import empty_table_for_schema
from datafusion_engine.arrow.semantic import span_metadata, span_type
from datafusion_engine.schema.introspection import table_names_snapshot
from datafusion_engine.schema.registry import extract_schema_for
from datafusion_engine.udf.expr import udf_expr
from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    from datafusion import SessionContext


LOGGER = logging.getLogger(__name__)


def _py_bc_line_table_with_bytes_schema() -> pa.Schema:
    base_schema = extract_schema_for("py_bc_line_table")
    fields = list(base_schema)
    names = {field.name for field in fields}
    extra_fields = (
        ("line_start_byte", pa.int64()),
        ("line_end_byte", pa.int64()),
        ("line_text", pa.string()),
        ("newline_kind", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
        ("span", span_type()),
        ("col_unit", pa.string()),
    )
    for name, dtype in extra_fields:
        if name in names:
            continue
        if name == "span":
            fields.append(pa.field(name, dtype, metadata=span_metadata(col_unit="byte")))
        else:
            fields.append(pa.field(name, dtype))
        names.add(name)
    return pa.schema(fields)


def _empty_py_bc_line_table_with_bytes(ctx: SessionContext) -> DataFrame:
    table = empty_table_for_schema(_py_bc_line_table_with_bytes_schema())
    return ctx.from_arrow(table)


def py_bc_line_table_with_bytes(
    ctx: SessionContext,
    *,
    line_table: str = "py_bc_line_table",
    line_index_table: str = "file_line_index_v1",
) -> DataFrame:
    """Return bytecode line rows joined with file line index bytes.

    Parameters
    ----------
    ctx
        DataFusion session context with the required tables registered.
    line_table
        Table/view name for bytecode line rows.
    line_index_table
        Table/view name for line index rows (line_start_byte + line_text).

    Returns
    -------
    DataFrame
        Bytecode line rows with line index byte offsets attached.

    Notes
    -----
    Missing input tables return an empty result so downstream builds can
    continue while diagnostics capture the gaps.
    """
    with stage_span(
        "semantics.bytecode_line_table",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
        attributes={
            "codeanatomy.line_table": line_table,
            "codeanatomy.line_index_table": line_index_table,
        },
    ):
        table_names = table_names_snapshot(ctx)
        if line_table not in table_names:
            LOGGER.warning(
                "Missing bytecode line table %r; returning empty bytecode line table.",
                line_table,
            )
            return _empty_py_bc_line_table_with_bytes(ctx)
        if line_index_table not in table_names:
            LOGGER.warning(
                "Missing line index table %r; returning empty bytecode line table.",
                line_index_table,
            )
            return _empty_py_bc_line_table_with_bytes(ctx)

        bc_lines = ctx.table(line_table)
        line_index = ctx.table(line_index_table).select(
            col("file_id"),
            col("line_no"),
            col("line_start_byte"),
            col("line_end_byte"),
            col("line_text"),
            col("newline_kind"),
        )
        joined = bc_lines.join(
            line_index,
            join_keys=(["file_id", "line0"], ["file_id", "line_no"]),
            how="left",
            coalesce_duplicate_keys=True,
        )
        bstart = col("line_start_byte").cast(pa.int64())
        bend = f.coalesce(col("line_end_byte").cast(pa.int64()), bstart)
        joined = joined.with_column("bstart", bstart).with_column("bend", bend)
        joined = joined.with_column("span", udf_expr("span_make", col("bstart"), col("bend")))
        joined = joined.with_column("col_unit", lit("byte"))
        return joined.drop("line_no")


__all__ = ["py_bc_line_table_with_bytes"]
