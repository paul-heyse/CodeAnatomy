"""End-to-end scan ordering coverage for determinism tiers."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.core.runtime_profiles import runtime_profile_factory
from arrowdsl.schema.metadata import ordering_from_schema
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan
from ibis_engine.sources import IbisDeltaWriteOptions, plan_from_source
from tests.utils import write_delta_table


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
    """Apply determinism tiers to Delta-backed scan plans."""
    table = pa.table({"entity_id": [2, 1], "name": ["b", "a"]})
    dataset_dir = tmp_path / "scan_dataset"
    write_delta_table(
        table,
        str(dataset_dir),
        options=IbisDeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
    )

    runtime = runtime_profile_factory("default").with_determinism(tier)
    ctx = ExecutionContext(runtime=runtime)
    table = DeltaTable(str(dataset_dir)).to_pyarrow_dataset().to_table()
    backend = build_backend(
        IbisBackendConfig(
            datafusion_profile=runtime.datafusion,
            fuse_selects=runtime.ibis_fuse_selects,
            default_limit=runtime.ibis_default_limit,
            default_dialect=runtime.ibis_default_dialect,
            interactive=runtime.ibis_interactive,
        )
    )
    plan = plan_from_source(table, ctx=ctx, backend=backend, name="ordering_scan")
    execution = IbisExecutionContext(ctx=ctx, ibis_backend=backend)
    result = materialize_ibis_plan(plan, execution=execution)

    ordering = ordering_from_schema(result.schema)
    assert ordering.level == expected_level
    entity_ids = result.to_pydict().get("entity_id", [])
    assert entity_ids == expected_order
