"""Unit tests for Substrait validation execution helpers."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.plan.bundle_artifact import PlanBundleOptions, build_plan_artifact
from datafusion_engine.plan.result_types import validate_substrait_plan
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.optional_deps import require_datafusion

require_datafusion()


def test_validate_substrait_plan_round_trip_match() -> None:
    """Validate replayed Substrait output against the source dataframe."""
    profile = df_profile()
    ctx = profile.session_context()
    register_arrow_table(ctx, name="events", value=pa.table({"id": [1, 2, 3]}))
    df = ctx.sql("SELECT id FROM events WHERE id > 1")
    bundle = build_plan_artifact(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=profile.session_runtime()),
    )
    assert bundle.substrait_bytes is not None
    validation = validate_substrait_plan(bundle.substrait_bytes, df=df, ctx=ctx)
    assert validation.get("status") == "ok"
    assert validation.get("match") is True


def test_validate_substrait_plan_reports_replay_errors() -> None:
    """Return deterministic error payloads when replay fails."""
    profile = df_profile()
    ctx = profile.session_context()
    register_arrow_table(ctx, name="events", value=pa.table({"id": [1, 2, 3]}))
    df = ctx.sql("SELECT id FROM events")
    validation = validate_substrait_plan(b"not-a-substrait-plan", df=df, ctx=ctx)
    assert validation.get("status") == "error"
    assert validation.get("match") is False
    diagnostics = validation.get("diagnostics")
    assert isinstance(diagnostics, dict)
    assert diagnostics.get("reason") is not None
