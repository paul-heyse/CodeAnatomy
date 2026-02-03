"""Bytecode line table join helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame

from datafusion_engine.schema.introspection import table_names_snapshot
from datafusion_engine.udf.shims import span_make
from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    from datafusion import SessionContext


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

    Raises
    ------
    ValueError
        Raised when required tables are missing.
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
            msg = f"Missing bytecode line table {line_table!r}."
            raise ValueError(msg)
        if line_index_table not in table_names:
            msg = f"Missing line index table {line_index_table!r}."
            raise ValueError(msg)

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
        joined = joined.with_column("span", span_make(col("bstart"), col("bend")))
        joined = joined.with_column("col_unit", lit("byte"))
        return joined.drop("line_no")


__all__ = ["py_bc_line_table_with_bytes"]
