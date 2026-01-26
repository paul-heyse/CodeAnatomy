"""Delta Lake scan profile helpers."""

from __future__ import annotations

from typing import Protocol

from schema_spec.system import DatasetSpec, DeltaScanOptions


class DeltaScanLocation(Protocol):
    """Protocol for objects carrying Delta scan configuration."""

    @property
    def format(self) -> str:
        """Return the dataset format string."""
        ...

    @property
    def delta_scan(self) -> DeltaScanOptions | None:
        """Return override scan options, if any."""
        ...

    @property
    def dataset_spec(self) -> DatasetSpec | None:
        """Return dataset spec, if any."""
        ...


_DEFAULT_DELTA_SCAN = DeltaScanOptions(
    file_column_name="__delta_rs_path",
    enable_parquet_pushdown=True,
    schema_force_view_types=True,
    wrap_partition_values=True,
    schema=None,
)


def build_delta_scan_config(location: DeltaScanLocation) -> DeltaScanOptions | None:
    """Return the effective Delta scan configuration for a location.

    Returns
    -------
    DeltaScanOptions | None
        Resolved Delta scan options when applicable.
    """
    if location.format != "delta":
        return None
    base = _DEFAULT_DELTA_SCAN
    if location.dataset_spec is not None:
        base = _merge_delta_scan(base, location.dataset_spec.delta_scan)
    return _merge_delta_scan(base, location.delta_scan)


def _merge_delta_scan(
    base: DeltaScanOptions,
    override: DeltaScanOptions | None,
) -> DeltaScanOptions:
    if override is None:
        return base
    return DeltaScanOptions(
        file_column_name=override.file_column_name or base.file_column_name,
        enable_parquet_pushdown=(
            override.enable_parquet_pushdown
            if override.enable_parquet_pushdown is not None
            else base.enable_parquet_pushdown
        ),
        schema_force_view_types=(
            override.schema_force_view_types
            if override.schema_force_view_types is not None
            else base.schema_force_view_types
        ),
        wrap_partition_values=(
            override.wrap_partition_values
            if override.wrap_partition_values is not None
            else base.wrap_partition_values
        ),
        schema=override.schema or base.schema,
    )


__all__ = ["DeltaScanLocation", "build_delta_scan_config"]
