"""Delta Change Data Feed (CDF) helpers for DataFusion."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion.dataframe import DataFrame

from datafusion_engine.dataset.registry import (
    resolve_datafusion_provider,
)
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.session.facade import DataFusionExecutionFacade

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import ManifestDatasetResolver


CDF_METADATA_COLUMNS: tuple[str, ...] = (
    "_change_type",
    "_commit_version",
    "_commit_timestamp",
)


@dataclass(frozen=True)
class CdfRegistration:
    """Registration record for a CDF input."""

    table_name: str
    cdf_name: str


def register_cdf_inputs(
    ctx: SessionContext,
    runtime_profile: DataFusionRuntimeProfile,
    *,
    table_names: Sequence[str],
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> dict[str, str]:
    """Register CDF-backed inputs and return base-to-CDF name mapping.

    Returns:
        dict[str, str]: Mapping from base table names to CDF view names.

    Raises:
        ValueError: If ``dataset_resolver`` is None.
    """
    from datafusion_engine.tables.metadata import table_provider_metadata

    if dataset_resolver is None:
        msg = "dataset_resolver is required for CDF input registration."
        raise ValueError(msg)
    adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
    mapping: dict[str, str] = {}
    for name in table_names:
        location = dataset_resolver.location(name)
        cdf_name = f"{name}__cdf"
        metadata = table_provider_metadata(id(ctx), table_name=name)
        supports_cdf = bool(metadata.supports_cdf) if metadata is not None else False
        provider = resolve_datafusion_provider(location) if location is not None else None
        if location is not None and provider == "delta_cdf":
            facade.register_dataset(
                name=cdf_name,
                location=location,
                overwrite=True,
            )
            cdf_df = ctx.table(cdf_name)
        elif supports_cdf:
            cdf_df = ctx.table(name)
        else:
            continue
        cleaned = strip_cdf_metadata(cdf_df)
        adapter.register_view(cdf_name, cleaned, overwrite=True, temporary=False)
        mapping[name] = cdf_name
    return mapping


def strip_cdf_metadata(df: DataFrame) -> DataFrame:
    """Drop Delta CDF metadata columns when present.

    Returns:
    -------
    DataFrame
        DataFrame with CDF metadata columns removed.
    """
    schema = df.schema()
    names = schema.names if hasattr(schema, "names") else tuple(field.name for field in schema)
    to_drop = [name for name in CDF_METADATA_COLUMNS if name in names]
    if not to_drop:
        return df
    return df.drop(*to_drop)


__all__ = ["CDF_METADATA_COLUMNS", "CdfRegistration", "register_cdf_inputs", "strip_cdf_metadata"]
