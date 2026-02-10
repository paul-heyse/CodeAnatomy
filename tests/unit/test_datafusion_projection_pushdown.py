"""Tests for dynamic projection pushdown behavior."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.plan.bundle_artifact import PlanBundleOptions, build_plan_artifact
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.optional_deps import require_datafusion

require_datafusion()


def test_dynamic_projection_reduces_columns() -> None:
    """Ensure projection pushdown removes unused columns."""
    profile = df_profile()
    ctx = profile.session_context()
    session_runtime = profile.session_runtime()
    register_arrow_table(
        ctx,
        name="events",
        value=pa.table(
            {
                "id": [1, 2],
                "label": ["a", "b"],
                "extra": [10, 20],
            }
        ),
    )
    df = ctx.sql("SELECT events.id FROM events")
    bundle = build_plan_artifact(
        ctx,
        df,
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    plan_text = bundle.display_optimized_plan() or bundle.display_logical_plan() or ""
    assert "label" not in plan_text
    assert "extra" not in plan_text
