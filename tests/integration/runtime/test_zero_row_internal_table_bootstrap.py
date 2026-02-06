"""Integration tests for zero-row internal table bootstrap behavior."""

from __future__ import annotations

from pathlib import Path

import pytest

from datafusion_engine.cache.inventory import (
    CacheInventoryEntry,
    CACHE_INVENTORY_TABLE_NAME,
    ensure_cache_inventory_table,
    record_cache_inventory_entry,
)
from datafusion_engine.delta.observability import (
    DELTA_SNAPSHOT_TABLE_NAME,
    DeltaSnapshotArtifact,
    record_delta_snapshot,
)
from datafusion_engine.session.runtime import DataFusionRuntimeProfile, PolicyBundleConfig
from tests.test_helpers.optional_deps import (
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)

require_datafusion_udfs()
require_deltalake()
require_delta_extension()


@pytest.mark.integration
def test_internal_tables_bootstrap_and_append_on_missing_delta_log(tmp_path: Path) -> None:
    """Missing internal Delta logs should bootstrap tables and allow appends."""
    artifacts_root = tmp_path / "artifacts"
    profile = DataFusionRuntimeProfile(
        policies=PolicyBundleConfig(plan_artifacts_root=str(artifacts_root)),
    )
    ctx = profile.session_context()

    cache_location = ensure_cache_inventory_table(ctx, profile)
    assert cache_location is not None
    assert cache_location.path is not None
    assert (Path(str(cache_location.path)) / "_delta_log").exists()

    cache_version = record_cache_inventory_entry(
        profile,
        entry=CacheInventoryEntry(
            view_name="zero_row_test_view",
            cache_policy="strict",
            cache_path=str(tmp_path / "cache" / "zero_row_test_view"),
            plan_fingerprint="plan",
            plan_identity_hash="identity",
            schema_identity_hash="schema",
            snapshot_version=0,
            snapshot_timestamp="1970-01-01T00:00:00Z",
            result="ok",
            row_count=0,
            file_count=0,
        ),
        ctx=ctx,
    )
    assert cache_version is not None

    snapshot_version = record_delta_snapshot(
        profile,
        artifact=DeltaSnapshotArtifact(
            table_uri="file:///tmp/zero_row_snapshot",
            snapshot={"version": 0},
            dataset_name="zero_row_dataset",
        ),
        ctx=ctx,
    )
    assert snapshot_version is not None

    snapshot_path = (
        artifacts_root / "delta_observability" / DELTA_SNAPSHOT_TABLE_NAME / "_delta_log"
    )
    assert snapshot_path.exists()
    assert ctx.table_exist(CACHE_INVENTORY_TABLE_NAME)
    assert ctx.table_exist(DELTA_SNAPSHOT_TABLE_NAME)
