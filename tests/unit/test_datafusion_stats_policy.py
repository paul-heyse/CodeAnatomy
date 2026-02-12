"""Unit tests for DataFusion stats policies."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from datafusion_engine.dataset.registration import (
    DatasetRegistrationOptions,
    register_dataset_df,
)
from datafusion_engine.dataset.registry import DatasetLocation, DatasetLocationOverrides
from datafusion_engine.session.runtime import (
    settings_snapshot_for_profile,
)
from schema_spec.contracts import DataFusionScanOptions, table_spec_from_schema
from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.optional_deps import require_datafusion

require_datafusion()


def _write_parquet(path: Path) -> None:
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    pq.write_table(table, path)


def test_scan_policy_applies_stats_settings(tmp_path: Path) -> None:
    """Apply stats settings before registering external datasets."""
    parquet_path = tmp_path / "events.parquet"
    _write_parquet(parquet_path)
    profile = df_profile()
    ctx = profile.session_context()
    scan = DataFusionScanOptions(
        collect_statistics=False,
        meta_fetch_concurrency=8,
        listing_mutable=True,
    )
    register_dataset_df(
        ctx,
        name="events",
        location=DatasetLocation(
            path=str(parquet_path),
            format="parquet",
            datafusion_provider="listing",
            overrides=DatasetLocationOverrides(datafusion_scan=scan),
        ),
        options=DatasetRegistrationOptions(runtime_profile=profile),
    )
    settings = settings_snapshot_for_profile(profile, ctx).to_pydict()
    values = dict(zip(settings.get("name", []), settings.get("value", []), strict=False))
    assert values.get("datafusion.execution.collect_statistics") == "false"
    assert values.get("datafusion.execution.meta_fetch_concurrency") == "8"


def test_scan_policy_applies_listing_cache_and_projection(tmp_path: Path) -> None:
    """Apply listing cache and projection settings on registration."""
    parquet_path = tmp_path / "events.parquet"
    _write_parquet(parquet_path)
    profile = df_profile()
    ctx = profile.session_context()
    scan = DataFusionScanOptions(
        list_files_cache_limit=str(1024),
        list_files_cache_ttl="30s",
        listing_table_factory_infer_partitions=False,
        projection_exprs=("*", "value AS value_tag"),
    )
    register_dataset_df(
        ctx,
        name="events",
        location=DatasetLocation(
            path=str(parquet_path),
            format="parquet",
            datafusion_provider="listing",
            overrides=DatasetLocationOverrides(datafusion_scan=scan),
        ),
        options=DatasetRegistrationOptions(runtime_profile=profile),
    )
    settings = settings_snapshot_for_profile(profile, ctx).to_pydict()
    values = dict(zip(settings.get("name", []), settings.get("value", []), strict=False))
    assert values.get("datafusion.runtime.list_files_cache_limit") == "1024"
    assert values.get("datafusion.runtime.list_files_cache_ttl") == "30s"
    assert values.get("datafusion.execution.listing_table_factory_infer_partitions") == "false"
    schema = ctx.table("events").schema()
    assert "value_tag" in schema.names


def test_listing_schema_evolution_adds_missing_columns(tmp_path: Path) -> None:
    """Fill missing columns when listing schemas evolve."""
    parquet_path = tmp_path / "events.parquet"
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    pq.write_table(table, parquet_path)
    expected_schema = pa.schema(
        [("id", pa.int64()), ("value", pa.string()), ("extra", pa.string())]
    )
    profile = df_profile()
    ctx = profile.session_context()
    scan = DataFusionScanOptions(listing_mutable=True)
    register_dataset_df(
        ctx,
        name="events",
        location=DatasetLocation(
            path=str(parquet_path),
            format="parquet",
            datafusion_provider="listing",
            overrides=DatasetLocationOverrides(
                datafusion_scan=scan,
                table_spec=table_spec_from_schema("events", expected_schema),
            ),
        ),
        options=DatasetRegistrationOptions(runtime_profile=profile),
    )
    result = ctx.table("events").to_arrow_table()
    assert "extra" in result.column_names
    assert all(value is None for value in result["extra"].to_pylist())
