"""Tests for DataFusionRuntimeProfile facade query helpers."""

from __future__ import annotations

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.session.runtime_config_policies import SchemaHardeningProfile
from datafusion_engine.session.runtime_profile_config import (
    DataSourceConfig,
    ExecutionConfig,
    PolicyBundleConfig,
)


def test_dataset_candidates_prefers_direct_match() -> None:
    """dataset_candidates returns direct binding when destination matches key."""
    profile = DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(
            dataset_templates={
                "target": DatasetLocation(path="/tmp/target", format="delta"),
                "other": DatasetLocation(path="/tmp/other", format="delta"),
            }
        )
    )
    candidates = profile.dataset_candidates("target")
    assert candidates == (("target", DatasetLocation(path="/tmp/target", format="delta")),)


def test_join_repartition_enabled_uses_policy_and_keys() -> None:
    """join_repartition_enabled requires keys and partition capacity."""
    profile = DataFusionRuntimeProfile(
        execution=ExecutionConfig(target_partitions=8),
    )
    assert profile.join_repartition_enabled(["k"]) is True
    assert profile.join_repartition_enabled([]) is False


def test_schema_hardening_view_types_reflects_policy() -> None:
    """schema_hardening_view_types mirrors resolved hardening policy."""
    disabled = DataFusionRuntimeProfile()
    assert disabled.schema_hardening_view_types() == frozenset()

    enabled = DataFusionRuntimeProfile(
        policies=PolicyBundleConfig(schema_hardening=SchemaHardeningProfile(enable_view_types=True))
    )
    assert enabled.schema_hardening_view_types() == frozenset({"view"})
