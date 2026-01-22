"""Helpers for resolving dataset locations across runtime catalogs."""

from __future__ import annotations

from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.registry import DatasetLocation


def resolve_dataset_location(
    name: str,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> DatasetLocation | None:
    """Resolve a dataset location from the runtime profile.

    Returns
    -------
    DatasetLocation | None
        Location metadata for the dataset when configured.
    """
    return runtime_profile.dataset_location(name)


__all__ = ["resolve_dataset_location"]
