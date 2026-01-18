"""Cross-engine Substrait validation tests."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pytest
from datafusion import SessionContext

from datafusion_engine.bridge import collect_plan_artifacts
from datafusion_engine.compile_options import DataFusionCompileOptions
from sqlglot_tools.optimizer import parse_sql_strict


@pytest.mark.integration
def test_substrait_cross_validation_match() -> None:
    """Compare PyArrow Substrait output to DataFusion results."""
    ctx = SessionContext()
    table = pa.table({"id": [1, 2, 3], "label": ["alpha", "beta", "gamma"]})
    ctx.register_record_batches("input_table", [table.to_batches()])
    sql = "SELECT * FROM input_table"
    options = DataFusionCompileOptions(substrait_validation=True)
    expr = parse_sql_strict(sql, dialect=options.dialect)
    df = ctx.sql(sql)
    artifacts = collect_plan_artifacts(ctx, expr, options=options, df=df)
    validation = artifacts.substrait_validation
    if not isinstance(validation, Mapping):
        pytest.skip("Substrait validation unavailable.")
    if validation.get("status") in {"unavailable", "missing_plan"}:
        pytest.skip("Substrait validation unavailable.")
    assert validation.get("match") is True
