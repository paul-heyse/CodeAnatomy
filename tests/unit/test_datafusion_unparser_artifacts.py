"""Unit tests for DataFusion unparser plan artifacts."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.bridge import collect_plan_artifacts
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.runtime import DataFusionRuntimeProfile
from sqlglot_tools.optimizer import parse_sql_strict

datafusion = pytest.importorskip("datafusion")


def test_datafusion_unparser_payload_is_deterministic() -> None:
    """Capture deterministic SQL unparser payloads for plan artifacts."""
    ctx = DataFusionRuntimeProfile().session_context()
    ctx.register_record_batches(
        "events",
        [pa.table({"id": [1, 2], "label": ["a", "b"]}).to_batches()],
    )
    sql = "SELECT id, label FROM events"
    expr = parse_sql_strict(sql, dialect="datafusion")
    df = ctx.sql(sql)
    options = DataFusionCompileOptions()
    first = collect_plan_artifacts(ctx, expr, options=options, df=df)
    second = collect_plan_artifacts(ctx, expr, options=options, df=df)
    if first.unparsed_sql is None:
        pytest.skip("Unparser unavailable for this DataFusion build.")
    assert first.unparse_error is None
    assert second.unparse_error is None
    assert first.unparsed_sql == second.unparsed_sql
