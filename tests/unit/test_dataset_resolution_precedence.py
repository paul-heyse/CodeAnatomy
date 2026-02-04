"""Dataset resolution precedence tests."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.dataset.registry import (
    DatasetLocation,
    DatasetLocationOverrides,
    resolve_dataset_location,
)
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import DatasetSpec
from storage.deltalake.config import DeltaSchemaPolicy


def _table_spec(name: str, field_name: str) -> TableSchemaSpec:
    return TableSchemaSpec(
        name=name,
        fields=[FieldSpec(name=field_name, dtype=pa.int64(), nullable=False)],
    )


def test_resolved_location_prefers_overrides() -> None:
    """Overrides should take precedence over dataset spec values."""
    base_spec = _table_spec("events", "id")
    override_spec = _table_spec("events_override", "override_id")
    base_policy = DeltaSchemaPolicy(schema_mode="merge")
    override_policy = DeltaSchemaPolicy(schema_mode="overwrite")

    dataset_spec = DatasetSpec(table_spec=base_spec, delta_schema_policy=base_policy)
    overrides = DatasetLocationOverrides(
        delta_schema_policy=override_policy,
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
    dataset_spec = DatasetSpec(table_spec=base_spec, delta_schema_policy=base_policy)
    location = DatasetLocation(
        path="/tmp/events",
        format="delta",
        dataset_spec=dataset_spec,
    )

    resolved = resolve_dataset_location(location)

    assert resolved.delta_schema_policy == base_policy
    assert resolved.schema == dataset_spec.schema()
