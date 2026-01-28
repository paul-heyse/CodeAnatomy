"""Cross-engine Substrait validation tests."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pytest

from datafusion_engine.execution_helpers import validate_substrait_plan
from datafusion_engine.plan_bundle import build_plan_bundle
from datafusion_engine.runtime import DataFusionRuntimeProfile


@pytest.mark.integration
def test_substrait_cross_validation_match() -> None:
    """Compare PyArrow Substrait output to DataFusion results."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    session_runtime = profile.session_runtime()
    table = pa.table({"id": [1, 2, 3], "label": ["alpha", "beta", "gamma"]})
    ctx.register_record_batches("input_table", [table.to_batches()])
    sql = "SELECT * FROM input_table"
    df = ctx.sql(sql)
    bundle = build_plan_bundle(
        ctx,
        df,
        session_runtime=session_runtime,
    )
    if bundle.substrait_bytes is None:
        pytest.skip("Substrait serialization unavailable.")
    validation = validate_substrait_plan(bundle.substrait_bytes, df=df)
    if not isinstance(validation, Mapping):
        pytest.skip("Substrait validation unavailable.")
    if validation.get("status") in {"unavailable", "missing_plan"}:
        pytest.skip("Substrait validation unavailable.")
    assert validation.get("match") is True
