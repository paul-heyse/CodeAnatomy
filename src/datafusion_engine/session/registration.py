"""Session registration facade helpers."""

from __future__ import annotations

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.dataset.registration import DataFusionCachePolicy
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.registry_facade import registry_facade_for_context
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from semantics.program_manifest import ManifestDatasetBindings


def register_dataset_df(
    ctx: SessionContext,
    *,
    profile: DataFusionRuntimeProfile,
    name: str,
    location: DatasetLocation,
    cache_policy: DataFusionCachePolicy | None = None,
    overwrite: bool = True,
) -> DataFrame:
    """Register a dataset through the registry facade.

    Returns:
    -------
    DataFrame
        Registered dataset DataFrame handle.
    """
    facade = registry_facade_for_context(
        ctx,
        runtime_profile=profile,
        dataset_resolver=ManifestDatasetBindings(locations={}),
    )
    return facade.register_dataset_df(
        name=name,
        location=location,
        cache_policy=cache_policy,
        overwrite=overwrite,
    )


__all__ = ["register_dataset_df"]
