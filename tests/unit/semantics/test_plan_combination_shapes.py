"""Plan-shape checks for combined semantic DataFrame composition."""

from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext, col, lit


def test_combined_plan_exposes_logical_optimized_and_physical_shapes() -> None:
    """Combined semantic query should expose all core plan surfaces."""
    ctx = SessionContext()

    left = pa.table(
        {
            "dataset_name": ["alpha", "beta"],
            "confidence": [0.95, 0.55],
        }
    )
    right = pa.table(
        {
            "dataset_name": ["alpha", "gamma"],
            "weight": [3, 1],
        }
    )

    ctx.register_record_batches("left_df", [left.to_batches()])
    ctx.register_record_batches("right_df", [right.to_batches()])

    combined = (
        ctx.table("left_df")
        .join(ctx.table("right_df"), on=["dataset_name"], how="inner")
        .filter(col("confidence") > lit(0.8))
    )

    assert combined.logical_plan() is not None
    assert combined.optimized_logical_plan() is not None
    assert combined.execution_plan() is not None
