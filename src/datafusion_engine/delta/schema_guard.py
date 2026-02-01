"""Schema evolution guards for Delta materialization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

from datafusion_engine.identity import schema_identity_hash
from storage.deltalake.delta import DeltaSchemaRequest, delta_table_schema

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation


SchemaEvolutionMode = Literal["strict", "additive"]


@dataclass(frozen=True)
class SchemaEvolutionPolicy:
    """Schema evolution policy for semantic outputs."""

    mode: SchemaEvolutionMode = "strict"


def _field_map(schema: pa.Schema) -> dict[str, pa.Field]:
    return {field.name: field for field in schema}


def _ensure_additive(existing: pa.Schema, updated: pa.Schema) -> None:
    existing_fields = _field_map(existing)
    updated_fields = _field_map(updated)
    missing: list[str] = []
    mismatched: list[str] = []
    for name, field in existing_fields.items():
        updated_field = updated_fields.get(name)
        if updated_field is None:
            missing.append(name)
            continue
        if updated_field.type != field.type:
            mismatched.append(name)
    if missing or mismatched:
        msg = (
            "Delta schema evolution rejected. "
            f"Missing fields: {missing!r}. "
            f"Type mismatches: {mismatched!r}."
        )
        raise ValueError(msg)


def enforce_schema_policy(
    *,
    expected_schema: pa.Schema,
    dataset_location: DatasetLocation,
    policy: SchemaEvolutionPolicy,
) -> str:
    """Enforce schema evolution policy against existing Delta table.

    Parameters
    ----------
    expected_schema
        Schema for the new semantic output.
    dataset_location
        Delta dataset location metadata.
    policy
        Schema evolution policy (strict or additive).

    Returns
    -------
    str
        Schema identity hash for the expected schema.

    Raises
    ------
    ValueError
        Raised when schema evolution policy validation fails.
    """
    request = DeltaSchemaRequest(
        path=str(dataset_location.path),
        storage_options=dict(dataset_location.storage_options),
        log_storage_options=dict(dataset_location.delta_log_storage_options),
        version=dataset_location.delta_version,
        timestamp=dataset_location.delta_timestamp,
        gate=dataset_location.delta_feature_gate,
    )
    existing_schema = delta_table_schema(request)
    expected_hash = schema_identity_hash(expected_schema)
    if existing_schema is None:
        return expected_hash
    if policy.mode == "strict":
        existing_hash = schema_identity_hash(existing_schema)
        if existing_hash != expected_hash:
            msg = "Delta schema evolution rejected (strict policy)."
            raise ValueError(msg)
        return expected_hash
    if policy.mode == "additive":
        _ensure_additive(existing_schema, expected_schema)
        return expected_hash
    msg = f"Unsupported schema evolution mode: {policy.mode!r}."
    raise ValueError(msg)


__all__ = [
    "SchemaEvolutionMode",
    "SchemaEvolutionPolicy",
    "enforce_schema_policy",
]
