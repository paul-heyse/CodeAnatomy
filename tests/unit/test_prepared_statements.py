"""Tests for DataFusion prepared statement execution."""

from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext

from arrowdsl.schema.build import rows_from_table
from datafusion_engine.bridge import (
    PreparedStatementOptions,
    execute_prepared_statement,
    prepare_statement,
)
from obs.diagnostics import DiagnosticsCollector, prepared_statement_hook


def test_prepared_statement_matches_unprepared() -> None:
    """Match prepared execution output to direct SQL."""
    ctx = SessionContext()
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    ctx.from_arrow(table, name="t")

    diagnostics = DiagnosticsCollector()
    prepare_statement(
        ctx,
        name="diag",
        sql="SELECT id, name FROM t WHERE id = $1",
        options=PreparedStatementOptions(
            param_types=("INT",),
            record_hook=prepared_statement_hook(diagnostics),
        ),
    )
    prepared = execute_prepared_statement(ctx, name="diag", params=[2]).to_arrow_table()
    direct = ctx.sql("SELECT id, name FROM t WHERE id = 2").to_arrow_table()

    assert rows_from_table(prepared) == rows_from_table(direct)
    recorded = diagnostics.artifacts_snapshot().get("datafusion_prepared_statements_v1", [])
    assert recorded
