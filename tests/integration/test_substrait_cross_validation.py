"""Cross-engine Substrait validation tests."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.plan.bundle_artifact import PlanBundleOptions, build_plan_artifact
from datafusion_engine.plan.execution_runtime import validate_substrait_plan
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.datafusion_runtime import df_profile


@pytest.mark.integration
def test_substrait_cross_validation_match() -> None:
    """Compare PyArrow Substrait output to DataFusion results."""
    profile = df_profile()
    ctx = profile.session_context()
    session_runtime = profile.session_runtime()
    table = pa.table({"id": [1, 2, 3], "label": ["alpha", "beta", "gamma"]})

    register_arrow_table(ctx, name="input_table", value=table)
    sql = "SELECT * FROM input_table"
    df = ctx.sql(sql)
    bundle = build_plan_artifact(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    assert bundle.substrait_bytes is not None
    validation = validate_substrait_plan(bundle.substrait_bytes, df=df, ctx=ctx)
    assert validation.get("status") == "ok"
    assert validation.get("match") is True
