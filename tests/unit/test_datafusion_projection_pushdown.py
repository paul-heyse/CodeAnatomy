"""Tests for dynamic projection pushdown behavior."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.execution_helpers import collect_plan_artifacts, df_from_sqlglot_or_sql
from datafusion_engine.runtime import DataFusionRuntimeProfile
from sqlglot_tools.optimizer import parse_sql_strict

datafusion = pytest.importorskip("datafusion")


def test_dynamic_projection_reduces_columns() -> None:
    """Ensure projection pushdown removes unused columns."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
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
    expr = parse_sql_strict("SELECT events.id FROM events", dialect="datafusion")
    options = DataFusionCompileOptions(dynamic_projection=True, runtime_profile=profile)
    df = df_from_sqlglot_or_sql(ctx, expr, options=options)
    artifacts = collect_plan_artifacts(ctx, expr, options=options, df=df)
    plan_text = artifacts.optimized_plan or artifacts.logical_plan or ""
    assert "label" not in plan_text
    assert "extra" not in plan_text
