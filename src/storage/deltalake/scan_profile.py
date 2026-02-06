"""Delta Lake scan profile helpers."""

from __future__ import annotations

from datafusion_engine.dataset.policies import resolve_delta_scan_options
from schema_spec.system import DatasetSpec, DeltaScanOptions


def build_delta_scan_config(
    *,
    dataset_format: str,
    dataset_spec: DatasetSpec | None,
    override: DeltaScanOptions | None,
) -> DeltaScanOptions | None:
    """Return the effective Delta scan configuration for a dataset.

    Returns:
    -------
    DeltaScanOptions | None
        Resolved Delta scan options when applicable.
    """
    return resolve_delta_scan_options(
        dataset_format=dataset_format,
        dataset_spec=dataset_spec,
        override=override,
    )


__all__ = ["build_delta_scan_config"]
