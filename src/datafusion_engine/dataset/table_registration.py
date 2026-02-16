"""Table-registration helpers for DataFusion datasets."""

from __future__ import annotations

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.dataset.registration import DataFusionCachePolicy, register_dataset_df
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def register_dataset_table(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    runtime_profile: DataFusionRuntimeProfile,
    cache_policy: DataFusionCachePolicy | None = None,
    overwrite: bool = True,
) -> DataFrame:
    """Register a dataset table via the canonical registration path.

    Returns:
    -------
    DataFrame
        Registered table DataFrame handle.
    """
    return register_dataset_df(
        ctx,
        name=name,
        location=location,
        cache_policy=cache_policy,
        runtime_profile=runtime_profile,
        overwrite=overwrite,
    )


__all__ = ["register_dataset_table"]
