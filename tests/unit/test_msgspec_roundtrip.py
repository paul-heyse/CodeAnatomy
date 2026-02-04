"""Msgspec round-trip tests for config structs."""

from __future__ import annotations

import msgspec
import pytest

from datafusion_engine.dataset.registry import DatasetLocation, DatasetLocationOverrides
from datafusion_engine.session.runtime import (
    CatalogConfig,
    DataFusionConfigPolicy,
    DataFusionFeatureGates,
    DataFusionJoinPolicy,
    DataFusionSettingsContract,
    DataSourceConfig,
    ExecutionConfig,
    FeatureGatesConfig,
    SchemaHardeningProfile,
)
from storage.deltalake.config import DeltaSchemaPolicy


@pytest.mark.parametrize(
    ("value", "type_"),
    [
        (ExecutionConfig(target_partitions=4, memory_pool="greedy"), ExecutionConfig),
        (CatalogConfig(default_catalog="datafusion", registry_catalogs={}), CatalogConfig),
        (
            FeatureGatesConfig(enable_udfs=True, enable_tracing=True),
            FeatureGatesConfig,
        ),
        (DataFusionConfigPolicy(settings={"datafusion.test": "true"}), DataFusionConfigPolicy),
        (DataFusionFeatureGates(enable_dynamic_filter_pushdown=False), DataFusionFeatureGates),
        (DataFusionJoinPolicy(enable_hash_join=False), DataFusionJoinPolicy),
        (
            DataFusionSettingsContract(
                settings={"datafusion.test": "true"},
                feature_gates=DataFusionFeatureGates(),
            ),
            DataFusionSettingsContract,
        ),
        (SchemaHardeningProfile(enable_view_types=True), SchemaHardeningProfile),
        (
            DatasetLocationOverrides(delta_schema_policy=DeltaSchemaPolicy(schema_mode="merge")),
            DatasetLocationOverrides,
        ),
        (DatasetLocation(path="/tmp/table", format="delta"), DatasetLocation),
        (
            DataSourceConfig(
                dataset_templates={"events": DatasetLocation(path="/tmp/events", format="delta")}
            ),
            DataSourceConfig,
        ),
    ],
)
def test_msgspec_roundtrip(value: object, type_: type[object]) -> None:
    """Ensure msgspec structs round-trip via msgpack encoding."""
    encoded = msgspec.msgpack.encode(value)
    decoded = msgspec.msgpack.decode(encoded, type=type_)
    assert decoded == value
