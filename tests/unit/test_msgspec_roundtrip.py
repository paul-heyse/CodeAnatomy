"""Msgspec round-trip tests for config structs."""

from __future__ import annotations

import msgspec
import pytest

from datafusion_engine.compile.options import DataFusionCompileOptionsSpec
from datafusion_engine.dataset.registry import DatasetLocation, DatasetLocationOverrides
from datafusion_engine.expr.query_spec import QuerySpec
from datafusion_engine.plan.cache import PlanCacheKey
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
from datafusion_engine.udf.catalog import DataFusionUdfSpecSnapshot, UdfCatalogSnapshot
from obs.otel.config import OtelConfigSpec
from schema_spec.arrow_types import ArrowPrimitiveSpec
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import DatasetPolicies, DatasetSpec, DeltaPolicyBundle, ValidationPolicySpec
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
            DatasetLocationOverrides(
                delta=DeltaPolicyBundle(schema_policy=DeltaSchemaPolicy(schema_mode="merge"))
            ),
            DatasetLocationOverrides,
        ),
        (DatasetLocation(path="/tmp/table", format="delta"), DatasetLocation),
        (
            DataSourceConfig(
                dataset_templates={"events": DatasetLocation(path="/tmp/events", format="delta")}
            ),
            DataSourceConfig,
        ),
        (
            FieldSpec(name="id", dtype=ArrowPrimitiveSpec(name="int64"), nullable=False),
            FieldSpec,
        ),
        (
            TableSchemaSpec(
                name="example",
                fields=(FieldSpec(name="id", dtype=ArrowPrimitiveSpec(name="int64")),),
                required_non_null=("id",),
                key_fields=("id",),
            ),
            TableSchemaSpec,
        ),
        (
            DatasetSpec(
                table_spec=TableSchemaSpec(
                    name="example",
                    fields=(FieldSpec(name="id", dtype=ArrowPrimitiveSpec(name="int64")),),
                ),
                policies=DatasetPolicies(
                    dataframe_validation=ValidationPolicySpec(enabled=True, sample=10)
                ),
            ),
            DatasetSpec,
        ),
        (QuerySpec.simple("id"), QuerySpec),
        (ValidationPolicySpec(enabled=True, lazy=False, sample=5), ValidationPolicySpec),
        (
            DataFusionCompileOptionsSpec(cache=True, cache_max_columns=32),
            DataFusionCompileOptionsSpec,
        ),
        (
            OtelConfigSpec(enable_traces=True, enable_metrics=False, sampler="always_on"),
            OtelConfigSpec,
        ),
        (
            PlanCacheKey(
                profile_hash="profile",
                substrait_hash="substrait",
                plan_fingerprint="plan",
                udf_snapshot_hash="udf",
                function_registry_hash="registry",
                information_schema_hash="info",
                required_udfs_hash="udfs",
                required_rewrite_tags_hash="tags",
                settings_hash="settings",
                delta_inputs_hash="delta",
            ),
            PlanCacheKey,
        ),
        (
            UdfCatalogSnapshot(
                specs={
                    "udf": DataFusionUdfSpecSnapshot(
                        func_id="udf",
                        engine_name="engine",
                        kind="scalar",
                        input_types=(),
                        return_type=ArrowPrimitiveSpec(name="int64"),
                    )
                },
                function_factory_hash=None,
            ),
            UdfCatalogSnapshot,
        ),
    ],
)
def test_msgspec_roundtrip(value: object, type_: type[object]) -> None:
    """Ensure msgspec structs round-trip via msgpack encoding."""
    encoded = msgspec.msgpack.encode(value)
    decoded = msgspec.msgpack.decode(encoded, type=type_)
    assert decoded == value
