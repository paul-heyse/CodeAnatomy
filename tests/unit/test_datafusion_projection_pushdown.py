"""Tests for dynamic projection pushdown behavior."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.plan_bundle import build_plan_bundle
from datafusion_engine.runtime import DataFusionRuntimeProfile

datafusion = pytest.importorskip("datafusion")


def test_dynamic_projection_reduces_columns() -> None:
    """Ensure projection pushdown removes unused columns."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    session_runtime = profile.session_runtime()
    ctx.register_record_batches(
        "events",
        [
            pa.table(
                {
                    "id": [1, 2],
                    "label": ["a", "b"],
                    "extra": [10, 20],
                }
            ).to_batches()
        ],
    )
    df = ctx.sql("SELECT events.id FROM events")
    bundle = build_plan_bundle(
        ctx,
        df,
        session_runtime=session_runtime,
    )
    plan_text = bundle.display_optimized_plan() or bundle.display_logical_plan() or ""
    assert "label" not in plan_text
    assert "extra" not in plan_text
