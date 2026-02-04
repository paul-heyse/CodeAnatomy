"""Delta Lake scan profile helpers."""

from __future__ import annotations

from schema_spec.system import DatasetSpec, DeltaScanOptions

_DEFAULT_DELTA_SCAN = DeltaScanOptions(
    file_column_name="__delta_rs_path",
    enable_parquet_pushdown=True,
    schema_force_view_types=True,
    wrap_partition_values=True,
    schema=None,
)


def build_delta_scan_config(
    *,
    dataset_format: str,
    dataset_spec: DatasetSpec | None,
    override: DeltaScanOptions | None,
) -> DeltaScanOptions | None:
    """Return the effective Delta scan configuration for a dataset.

    Returns
    -------
    DeltaScanOptions | None
        Resolved Delta scan options when applicable.
    """
    if dataset_format != "delta":
        return None
    base = _DEFAULT_DELTA_SCAN
    if dataset_spec is not None:
        base = _merge_delta_scan(base, dataset_spec.delta_scan)
    return _merge_delta_scan(base, override)


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


__all__ = ["build_delta_scan_config"]
