"""Integration tests for Delta-backed cache alignment."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.dataset.registration import (
    DataFusionCachePolicy,
    DatasetRegistrationOptions,
    register_dataset_df,
)
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.session.facade import ExecutionResult
from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    FeatureGatesConfig,
    PolicyBundleConfig,
)
from datafusion_engine.tables.spec import table_spec_from_location
from relspec.runtime_artifacts import ExecutionArtifactSpec, RuntimeArtifacts
from tests.test_helpers.delta_commit import latest_commit_metadata
from tests.test_helpers.delta_seed import DeltaSeedOptions, write_delta_table
from tests.test_helpers.optional_deps import (
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)

require_datafusion_udfs()
require_deltalake()
require_delta_extension()


@pytest.mark.integration
def test_dataset_delta_cache_registration(tmp_path: Path) -> None:
    """Cache registered datasets to Delta when policy requests staging."""
    cache_root = tmp_path / "cache"
    runtime_profile = DataFusionRuntimeProfile(
        features=FeatureGatesConfig(cache_enabled=True),
        policies=PolicyBundleConfig(cache_output_root=str(cache_root)),
    )
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    runtime_profile, ctx, delta_path = write_delta_table(
        tmp_path,
        table=table,
        options=DeltaSeedOptions(profile=runtime_profile, table_name="source"),
    )
    location = DatasetLocation(path=str(delta_path), format="delta")
    cache_policy = DataFusionCachePolicy(enabled=True, storage="delta_staging")
    df = register_dataset_df(
        ctx,
        name="source",
        location=location,
        options=DatasetRegistrationOptions(
            cache_policy=cache_policy,
            runtime_profile=runtime_profile,
        ),
    )
    assert df.to_arrow_table().num_rows == 2
    cache_key = table_spec_from_location("source", location).cache_key()
    cache_path = cache_root / "dataset_cache" / f"source__{cache_key}"
    assert (cache_path / "_delta_log").exists()
    commit_metadata = latest_commit_metadata(cache_path)
    assert commit_metadata.get("cache_policy") == "dataset_delta_staging"
    assert commit_metadata.get("cache_scope") == "dataset"


@pytest.mark.integration
def test_runtime_artifact_delta_persistence(tmp_path: Path) -> None:
    """Persist runtime artifacts to Delta when enabled."""
    runtime_profile = DataFusionRuntimeProfile(
        policies=PolicyBundleConfig(
            cache_output_root=str(tmp_path / "cache"),
            runtime_artifact_cache_enabled=True,
        ),
    )
    session_runtime = runtime_profile.session_runtime()
    artifacts = RuntimeArtifacts(execution=session_runtime)
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    artifact = artifacts.register_execution(
        "runtime_output",
        ExecutionResult.from_table(table),
        spec=ExecutionArtifactSpec(
            source_task="task",
            plan_task_signature="sig_v1",
            plan_signature="plan_v1",
        ),
    )
    assert artifact.storage_path is not None
    cache_path = Path(artifact.storage_path)
    assert (cache_path / "_delta_log").exists()
    commit_metadata = latest_commit_metadata(cache_path)
    assert commit_metadata.get("cache_policy") == "runtime_artifact_delta"
    assert commit_metadata.get("cache_scope") == "artifact"
    assert "runtime_output" not in artifacts.materialized_tables
    df = session_runtime.ctx.table("runtime_output")
    assert df.to_arrow_table().num_rows == 2


@pytest.mark.integration
def test_metadata_cache_snapshots(tmp_path: Path) -> None:
    """Snapshot metadata caches into Delta and register outputs."""
    runtime_profile = DataFusionRuntimeProfile(
        features=FeatureGatesConfig(cache_enabled=True),
        policies=PolicyBundleConfig(
            cache_output_root=str(tmp_path / "cache"),
            metadata_cache_snapshot_enabled=True,
        ),
    )
    ctx = runtime_profile.session_context()
    from datafusion_engine.cache.metadata_snapshots import snapshot_datafusion_caches

    snapshots = snapshot_datafusion_caches(ctx, runtime_profile=runtime_profile)
    successful = [row for row in snapshots if row.get("error") is None]
    assert successful
    for row in successful:
        cache_path = row.get("cache_path")
        snapshot_name = row.get("snapshot_name")
        assert isinstance(cache_path, str)
        assert isinstance(snapshot_name, str)
        assert (Path(cache_path) / "_delta_log").exists()
        commit_metadata = latest_commit_metadata(Path(cache_path))
        assert commit_metadata.get("cache_policy") == "metadata_snapshot"
        assert commit_metadata.get("cache_scope") == "metadata"
        df = ctx.table(snapshot_name)
        assert df.to_arrow_table().num_rows >= 0
    from datafusion_engine.cache.ledger import CACHE_SNAPSHOT_REGISTRY_TABLE_NAME

    registry_path = Path(runtime_profile.cache_root()) / "cache_ledgers"
    assert (registry_path / CACHE_SNAPSHOT_REGISTRY_TABLE_NAME / "_delta_log").exists()
    registry_df = ctx.table(CACHE_SNAPSHOT_REGISTRY_TABLE_NAME)
    assert registry_df.to_arrow_table().num_rows >= len(snapshots)


@pytest.mark.integration
def test_cache_run_summary_ledger(tmp_path: Path) -> None:
    """Persist cache run summaries into the Delta ledger."""
    runtime_profile = DataFusionRuntimeProfile(
        features=FeatureGatesConfig(cache_enabled=True),
        policies=PolicyBundleConfig(cache_output_root=str(tmp_path / "cache")),
    )
    ctx = runtime_profile.session_context()
    from datafusion_engine.cache.ledger import (
        CACHE_RUN_SUMMARY_TABLE_NAME,
        CacheRunSummary,
        record_cache_run_summary,
    )

    summary = CacheRunSummary(
        run_id="run-1",
        start_time_unix_ms=1,
        end_time_unix_ms=2,
        cache_root=runtime_profile.cache_root(),
        total_writes=1,
        total_reads=2,
        error_count=0,
    )
    record_cache_run_summary(runtime_profile, summary=summary, ctx=ctx)
    ledger_root = Path(runtime_profile.cache_root()) / "cache_ledgers"
    assert (ledger_root / CACHE_RUN_SUMMARY_TABLE_NAME / "_delta_log").exists()
    df = ctx.table(CACHE_RUN_SUMMARY_TABLE_NAME)
    assert df.to_arrow_table().num_rows >= 1
