"""Unit tests for DataFusion unparser plan artifacts."""

from __future__ import annotations

import json

import ibis
import pyarrow as pa
import pytest

from datafusion_engine.bridge import collect_plan_artifacts
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.param_tables import scalar_param_signature
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


def test_plan_artifacts_include_param_signature() -> None:
    """Record parameter signatures for plan artifacts."""
    ctx = DataFusionRuntimeProfile().session_context()
    ctx.register_record_batches(
        "events",
        [pa.table({"id": [1, 2], "label": ["a", "b"]}).to_batches()],
    )
    sql = "SELECT events.id FROM events WHERE events.id = :id"
    expr = parse_sql_strict(sql, dialect="datafusion")
    options = DataFusionCompileOptions(params={"id": 1})
    artifacts = collect_plan_artifacts(ctx, expr, options=options)
    assert artifacts.param_signature == scalar_param_signature({"id": 1})


def test_plan_artifacts_include_projection_map() -> None:
    """Record projection requirements for plan artifacts."""
    ctx = DataFusionRuntimeProfile().session_context()
    ctx.register_record_batches(
        "events",
        [pa.table({"id": [1, 2], "label": ["a", "b"]}).to_batches()],
    )
    sql = "SELECT events.id FROM events WHERE events.id = :id"
    expr = parse_sql_strict(sql, dialect="datafusion")
    options = DataFusionCompileOptions(params={"id": 1}, dynamic_projection=True)
    artifacts = collect_plan_artifacts(ctx, expr, options=options)
    assert artifacts.projection_map is not None
    projection_map = json.loads(artifacts.projection_map)
    assert projection_map == {"events": ["id"]}


def test_plan_artifacts_include_ibis_payload() -> None:
    """Capture Ibis artifacts alongside SQLGlot plan artifacts."""
    ctx = DataFusionRuntimeProfile().session_context()
    ctx.register_record_batches(
        "events",
        [pa.table({"id": [1, 2], "label": ["a", "b"]}).to_batches()],
    )
    sql = "SELECT id FROM events"
    expr = parse_sql_strict(sql, dialect="datafusion")
    ibis_expr = ibis.memtable({"id": [1]})
    options = DataFusionCompileOptions(ibis_expr=ibis_expr)
    artifacts = collect_plan_artifacts(ctx, expr, options=options)
    assert artifacts.ibis_decompile is not None
    assert artifacts.ibis_sql is not None
    assert artifacts.ibis_sql_pretty is not None
