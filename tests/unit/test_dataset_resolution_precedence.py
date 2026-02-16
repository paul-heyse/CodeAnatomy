"""Dataset resolution precedence tests."""

from __future__ import annotations

import msgspec
import pyarrow as pa

from datafusion_engine.dataset.policies import apply_scan_policy_defaults
from datafusion_engine.dataset.registry import (
    DatasetLocation,
    DatasetLocationOverrides,
    resolve_dataset_location,
    resolve_dataset_policies,
)
from schema_spec.arrow_types import arrow_type_from_pyarrow
from schema_spec.dataset_spec import (
    DatasetPolicies,
    DatasetSpec,
    DeltaPolicyBundle,
    DeltaScanPolicyDefaults,
    ScanPolicyConfig,
    ScanPolicyDefaults,
    dataset_spec_schema,
)
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import TableSchemaSpec
from storage.deltalake.config import DeltaSchemaPolicy


def _table_spec(name: str, field_name: str) -> TableSchemaSpec:
    return TableSchemaSpec(
        name=name,
        fields=[
            FieldSpec(
                name=field_name,
                dtype=arrow_type_from_pyarrow(pa.int64()),
                nullable=False,
            )
        ],
    )


def _apply_scan_policy(location: DatasetLocation, *, policy: ScanPolicyConfig) -> DatasetLocation:
    policies = resolve_dataset_policies(location, overrides=location.overrides)
    datafusion_scan, delta_scan = apply_scan_policy_defaults(
        dataset_format=location.format or "delta",
        datafusion_scan=policies.datafusion_scan,
        delta_scan=policies.delta_scan,
        policy=policy,
    )
    overrides = location.overrides or DatasetLocationOverrides()
    if datafusion_scan is not None:
        overrides = msgspec.structs.replace(overrides, datafusion_scan=datafusion_scan)
    if delta_scan is not None:
        delta_bundle = policies.delta_bundle
        if delta_bundle is None:
            delta_bundle = DeltaPolicyBundle(scan=delta_scan)
        else:
            delta_bundle = msgspec.structs.replace(delta_bundle, scan=delta_scan)
        overrides = msgspec.structs.replace(overrides, delta=delta_bundle)
    return msgspec.structs.replace(location, overrides=overrides)


def test_resolved_location_prefers_overrides() -> None:
    """Overrides should take precedence over dataset spec values."""
    base_spec = _table_spec("events", "id")
    override_spec = _table_spec("events_override", "override_id")
    base_policy = DeltaSchemaPolicy(schema_mode="merge")
    override_policy = DeltaSchemaPolicy(schema_mode="overwrite")

    dataset_spec = DatasetSpec(
        table_spec=base_spec,
        policies=DatasetPolicies(delta=DeltaPolicyBundle(schema_policy=base_policy)),
    )
    overrides = DatasetLocationOverrides(
        delta=DeltaPolicyBundle(schema_policy=override_policy),
        table_spec=override_spec,
    )
    location = DatasetLocation(
        path="/tmp/events",
        format="delta",
        dataset_spec=dataset_spec,
        overrides=overrides,
    )

    resolved = resolve_dataset_location(location)

    assert resolved.delta_schema_policy == override_policy
    assert resolved.schema == override_spec.to_arrow_schema()


def test_resolved_location_uses_dataset_spec_when_no_override() -> None:
    """Dataset spec values are used when overrides are absent."""
    base_spec = _table_spec("events", "id")
    base_policy = DeltaSchemaPolicy(schema_mode="merge")
    dataset_spec = DatasetSpec(
        table_spec=base_spec,
        policies=DatasetPolicies(delta=DeltaPolicyBundle(schema_policy=base_policy)),
    )
    location = DatasetLocation(
        path="/tmp/events",
        format="delta",
        dataset_spec=dataset_spec,
    )

    resolved = resolve_dataset_location(location)

    assert resolved.delta_schema_policy == base_policy
    assert resolved.schema == dataset_spec_schema(dataset_spec)


def test_scan_policy_applies_listing_defaults() -> None:
    """Listing scan defaults should populate scan options when absent."""
    policy = ScanPolicyConfig(
        listing=ScanPolicyDefaults(
            collect_statistics=True,
            list_files_cache_ttl="1h",
        )
    )
    location = DatasetLocation(path="/tmp/events", format="parquet")

    resolved = _apply_scan_policy(location, policy=policy)

    overrides = resolved.overrides
    scan = overrides.datafusion_scan if overrides is not None else None
    assert scan is not None
    assert scan.collect_statistics is True
    assert scan.list_files_cache_ttl == "1h"


def test_scan_policy_applies_delta_defaults() -> None:
    """Delta scan defaults should use delta-specific policy overrides."""
    policy = ScanPolicyConfig(
        listing=ScanPolicyDefaults(list_files_cache_ttl="1h"),
        delta_listing=ScanPolicyDefaults(list_files_cache_ttl="5m"),
        delta_scan=DeltaScanPolicyDefaults(schema_force_view_types=True),
    )
    location = DatasetLocation(path="/tmp/events", format="delta")

    resolved = _apply_scan_policy(location, policy=policy)

    overrides = resolved.overrides
    scan = overrides.datafusion_scan if overrides is not None else None
    delta_bundle = overrides.delta if overrides is not None else None
    delta_scan = delta_bundle.scan if delta_bundle is not None else None
    assert scan is not None
    assert scan.list_files_cache_ttl == "5m"
    assert delta_scan is not None
    assert delta_scan.schema_force_view_types is True
