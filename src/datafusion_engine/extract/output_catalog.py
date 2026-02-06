"""Helpers for extract output dataset catalogs."""

from __future__ import annotations

from pathlib import Path

from datafusion_engine.dataset.registry import DatasetCatalog, DatasetLocation
from datafusion_engine.extract.metadata import extract_metadata_specs
from datafusion_engine.extract.registry import dataset_spec as extract_dataset_spec


def build_extract_output_catalog(*, output_root: str) -> DatasetCatalog:
    """Return a dataset catalog for extract outputs.

    Parameters
    ----------
    output_root
        Root directory for extract outputs.

    Returns:
    -------
    DatasetCatalog
        Catalog of extract output dataset locations.
    """
    root = Path(output_root)
    catalog = DatasetCatalog()
    for row in extract_metadata_specs():
        name = row.name
        catalog.register(
            name,
            DatasetLocation(
                path=str(root / name),
                format="delta",
                dataset_spec=extract_dataset_spec(name),
            ),
        )
    return catalog


__all__ = ["build_extract_output_catalog"]
