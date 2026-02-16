"""Delta-specialized write helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from datafusion_engine.io.write_core import (
    DeltaWriteOutcome,
    DeltaWriteSpec,
    WritePipeline,
    WriteRequest,
    _delta_schema_policy_override,
)
from schema_spec.dataset_spec import DeltaMaintenancePolicy

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation


def _resolve_delta_schema_policy(
    options: Mapping[str, object],
    *,
    dataset_location: DatasetLocation | None,
) -> object | None:
    """Resolve effective Delta schema policy for write execution.

    Returns:
    -------
    object | None
        Effective schema policy override, if any.
    """
    schema_policy = _delta_schema_policy_override(options)
    if schema_policy is not None:
        return schema_policy
    if dataset_location is None:
        return None
    return dataset_location.delta_schema_policy


def _delta_maintenance_policy_override(
    options: Mapping[str, object],
) -> DeltaMaintenancePolicy | None:
    """Resolve maintenance-policy override payload for a Delta write.

    Returns:
    -------
    DeltaMaintenancePolicy | None
        Resolved maintenance policy override payload.

    Raises:
        TypeError: If the mapping payload cannot be parsed as policy.
    """
    raw = options.get("delta_maintenance_policy")
    if raw is None:
        raw = options.get("maintenance_policy")
    if raw is None:
        return None
    if isinstance(raw, DeltaMaintenancePolicy):
        return raw
    if isinstance(raw, Mapping):
        payload = {str(key): value for key, value in raw.items()}
        try:
            return DeltaMaintenancePolicy(**payload)
        except TypeError as exc:
            msg = "delta_maintenance_policy mapping is invalid."
            raise TypeError(msg) from exc
    return None


__all__ = [
    "DeltaWriteOutcome",
    "DeltaWriteSpec",
    "WritePipeline",
    "WriteRequest",
    "_delta_maintenance_policy_override",
    "_resolve_delta_schema_policy",
]
