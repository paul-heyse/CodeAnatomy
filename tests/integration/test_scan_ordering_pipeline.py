"""End-to-end scan ordering coverage for determinism tiers."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from arrowdsl.core.context import (
    DeterminismTier,
    ExecutionContext,
    OrderingLevel,
    runtime_profile_factory,
)
from arrowdsl.plan.scan_io import plan_from_source
from arrowdsl.schema.metadata import ordering_from_schema
from storage.io import write_dataset_parquet


@pytest.mark.integration
@pytest.mark.parametrize(
    ("tier", "expected_order", "expected_level"),
    [
        (DeterminismTier.STABLE_SET, [2, 1], OrderingLevel.IMPLICIT),
        (DeterminismTier.CANONICAL, [1, 2], OrderingLevel.EXPLICIT),
    ],
)
def test_scan_pipeline_ordering(
    tmp_path: Path,
    tier: DeterminismTier,
    expected_order: list[int],
    expected_level: OrderingLevel,
) -> None:
    """Apply determinism tiers to Parquet-backed scan plans."""
    table = pa.table({"entity_id": [2, 1], "name": ["b", "a"]})
    dataset_dir = tmp_path / "scan_dataset"
    write_dataset_parquet(table, dataset_dir)

    dataset = ds.dataset(str(dataset_dir), format="parquet")
    runtime = runtime_profile_factory("default").with_determinism(tier)
    ctx = ExecutionContext(runtime=runtime)
    plan = plan_from_source(dataset, ctx=ctx, label="ordering_scan")
    result = plan.to_table(ctx=ctx)

    ordering = ordering_from_schema(result.schema)
    assert ordering.level == expected_level
    entity_ids = result.to_pydict().get("entity_id", [])
    assert entity_ids == expected_order
